// Package workerhost runs one OS process per WhatsApp session and keeps each
// one alive. It is stdlib-only on purpose: it knows nothing about whatsmeow,
// WaCalls call signalling or the ACAF protocol, so the process-management
// logic can be tested without any WhatsApp dependency.
//
// Model: a Supervisor owns N workers. Each worker is a child process that
// serves the normal WaCalls HTTP API for exactly ONE session on a loopback
// port. The supervisor starts it, waits for /healthz, polls /healthz as the
// heartbeat, restarts it with exponential backoff when it exits or stops
// answering, and terminates it cleanly (SIGTERM, then SIGKILL after a grace
// period) on Stop/Shutdown. A crash, OOM kill or leak in one worker cannot
// touch another worker's memory, sockets or call state.
package workerhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Exit codes a worker uses to tell the supervisor what to do next.
const (
	// ExitPermanent: restarting cannot help (logged out, stream replaced,
	// client outdated, session row/device gone). The supervisor parks the
	// worker in StateFailed instead of restart-looping against WhatsApp.
	ExitPermanent = 78
	// ExitRecycle: the worker chose to exit to shed memory. Restart soon,
	// without growing the backoff (it was not a fault).
	ExitRecycle = 75
)

type State string

const (
	StateStarting State = "starting"
	StateRunning  State = "running"
	StateBackoff  State = "backoff"
	StateStopping State = "stopping"
	StateStopped  State = "stopped"
	StateFailed   State = "failed" // permanent; needs a human or a re-Start
)

var (
	ErrCapacity = errors.New("workerhost: worker capacity reached")
	ErrExists   = errors.New("workerhost: worker already exists")
	ErrNotFound = errors.New("workerhost: no such worker")
)

// Health is the JSON a worker returns from GET /healthz. It is the heartbeat
// payload and the contract between cmd/server's worker mode and this package.
type Health struct {
	OK          bool    `json:"ok"`
	SessionID   string  `json:"sessionId,omitempty"`
	WAState     string  `json:"waState,omitempty"` // open, qr, code, connecting, logged_out
	Connected   bool    `json:"connected"`         // WhatsApp websocket currently up
	ActiveCalls int     `json:"activeCalls"`
	RSSMB       float64 `json:"rssMb"`
	HeapMB      float64 `json:"heapMb"`
	Goroutines  int     `json:"goroutines"`
	UptimeSec   int64   `json:"uptimeSec"`
	Draining    bool    `json:"draining,omitempty"`
}

// Spec describes one worker. Any "{port}" in Args or Env values is replaced
// with the loopback port the supervisor picked for this start.
type Spec struct {
	ID   string
	Args []string
	Env  []string
}

// Status is a point-in-time copy of one worker's state.
type Status struct {
	ID           string    `json:"id"`
	State        State     `json:"state"`
	PID          int       `json:"pid,omitempty"`
	Port         int       `json:"port,omitempty"`
	Restarts     int       `json:"restarts"`
	Attempt      int       `json:"attempt"` // consecutive failed starts driving the backoff
	StartedAt    time.Time `json:"startedAt,omitempty"`
	LastHealthAt time.Time `json:"lastHealthAt,omitempty"`
	NextStartAt  time.Time `json:"nextStartAt,omitempty"`
	LastExit     string    `json:"lastExit,omitempty"`
	LastExitCode int       `json:"lastExitCode"`
	LastError    string    `json:"lastError,omitempty"`
	HealthFails  int       `json:"healthFails"`
	Health       Health    `json:"health"`
}

type Config struct {
	Command         string        // worker binary (usually os.Executable())
	MaxWorkers      int           // 0 = unlimited
	BackoffBase     time.Duration // first restart delay
	BackoffMax      time.Duration // cap
	Jitter          float64       // 0..1 fraction of random +/- spread; 0 = deterministic
	StableAfter     time.Duration // healthy this long => backoff resets
	StartTimeout    time.Duration // max wait for first healthy probe
	HealthInterval  time.Duration // heartbeat period
	HealthTimeout   time.Duration // per-probe timeout
	HealthFailLimit int           // consecutive failed probes before kill+restart
	StopGrace       time.Duration // SIGTERM -> SIGKILL
	Probe           func(ctx context.Context, port int) (Health, error)
	OnChange        func(Status) // every state transition and every successful heartbeat
	Log             *slog.Logger
}

func (c *Config) defaults() {
	if c.BackoffBase <= 0 {
		c.BackoffBase = time.Second
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = 60 * time.Second
	}
	if c.StableAfter <= 0 {
		c.StableAfter = 60 * time.Second
	}
	if c.StartTimeout <= 0 {
		c.StartTimeout = 30 * time.Second
	}
	if c.HealthInterval <= 0 {
		c.HealthInterval = 10 * time.Second
	}
	if c.HealthTimeout <= 0 {
		c.HealthTimeout = 3 * time.Second
	}
	if c.HealthFailLimit <= 0 {
		c.HealthFailLimit = 3
	}
	if c.StopGrace <= 0 {
		c.StopGrace = 10 * time.Second
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
}

// NextDelay is the exponential backoff: base * 2^attempt, capped at max, with
// optional +/- jitter. attempt 0 returns base. Also used by the worker's own
// WhatsApp reconnect loop so both layers back off the same way.
func NextDelay(attempt int, base, max time.Duration, jitter float64) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	if attempt < 0 {
		attempt = 0
	}
	d := base
	for i := 0; i < attempt && d < max; i++ {
		d *= 2
	}
	if max > 0 && d > max {
		d = max
	}
	if jitter > 0 {
		spread := (rand.Float64()*2 - 1) * jitter // -jitter..+jitter
		d = time.Duration(float64(d) * (1 + spread))
	}
	return d
}

type Supervisor struct {
	cfg Config

	mu      sync.Mutex
	workers map[string]*worker
}

func New(cfg Config) *Supervisor {
	cfg.defaults()
	if cfg.Probe == nil {
		cfg.Probe = HTTPProbe("")
	}
	return &Supervisor{cfg: cfg, workers: map[string]*worker{}}
}

func isActive(s State) bool { return s != StateStopped && s != StateFailed }

// Start launches a supervised worker. It returns immediately; use Get/Wait to
// observe readiness.
func (s *Supervisor) Start(spec Spec) error {
	if spec.ID == "" {
		return errors.New("workerhost: empty worker id")
	}
	s.mu.Lock()
	if old, ok := s.workers[spec.ID]; ok {
		if isActive(old.snapshot().State) {
			s.mu.Unlock()
			return ErrExists
		}
		delete(s.workers, spec.ID) // re-Start of a failed/stopped worker
	}
	if s.cfg.MaxWorkers > 0 {
		active := 0
		for _, w := range s.workers {
			if isActive(w.snapshot().State) {
				active++
			}
		}
		if active >= s.cfg.MaxWorkers {
			s.mu.Unlock()
			return ErrCapacity
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &worker{sup: s, spec: spec, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	w.st = Status{ID: spec.ID, State: StateStarting}
	s.workers[spec.ID] = w
	s.mu.Unlock()
	go w.loop()
	return nil
}

// Stop terminates one worker gracefully and forgets it.
func (s *Supervisor) Stop(id string) error {
	s.mu.Lock()
	w, ok := s.workers[id]
	s.mu.Unlock()
	if !ok {
		return ErrNotFound
	}
	w.cancel()
	<-w.done
	s.mu.Lock()
	if s.workers[id] == w {
		delete(s.workers, id)
	}
	s.mu.Unlock()
	return nil
}

// Shutdown stops every worker concurrently; returns when all have exited or
// ctx expires.
func (s *Supervisor) Shutdown(ctx context.Context) {
	s.mu.Lock()
	all := make([]*worker, 0, len(s.workers))
	for _, w := range s.workers {
		all = append(all, w)
	}
	s.mu.Unlock()
	for _, w := range all {
		w.cancel()
	}
	for _, w := range all {
		select {
		case <-w.done:
		case <-ctx.Done():
			return
		}
	}
}

func (s *Supervisor) Get(id string) (Status, bool) {
	s.mu.Lock()
	w, ok := s.workers[id]
	s.mu.Unlock()
	if !ok {
		return Status{}, false
	}
	return w.snapshot(), true
}

func (s *Supervisor) List() []Status {
	s.mu.Lock()
	all := make([]*worker, 0, len(s.workers))
	for _, w := range s.workers {
		all = append(all, w)
	}
	s.mu.Unlock()
	out := make([]Status, 0, len(all))
	for _, w := range all {
		out = append(out, w.snapshot())
	}
	return out
}

// Port returns the loopback port of a worker that is currently Running.
func (s *Supervisor) Port(id string) (int, bool) {
	st, ok := s.Get(id)
	if !ok || st.State != StateRunning {
		return 0, false
	}
	return st.Port, true
}

// WaitReady blocks until the worker is Running, reaches a terminal/backoff
// state after failing to start, or ctx ends.
func (s *Supervisor) WaitReady(ctx context.Context, id string) (Status, error) {
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for {
		st, ok := s.Get(id)
		if !ok {
			return Status{}, ErrNotFound
		}
		if st.State == StateRunning {
			return st, nil
		}
		if st.State == StateFailed || st.State == StateStopped || (st.State == StateBackoff && st.LastExit != "") {
			return st, fmt.Errorf("worker %s: %s (%s)", id, st.State, st.LastExit)
		}
		select {
		case <-ctx.Done():
			return st, ctx.Err()
		case <-t.C:
		}
	}
}

type worker struct {
	sup    *Supervisor
	spec   Spec
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu sync.Mutex
	st Status
}

func (w *worker) snapshot() Status {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.st
}

func (w *worker) update(f func(*Status)) {
	w.mu.Lock()
	f(&w.st)
	cp := w.st
	w.mu.Unlock()
	if cb := w.sup.cfg.OnChange; cb != nil {
		cb(cp)
	}
}

func (w *worker) loop() {
	defer close(w.done)
	cfg := w.sup.cfg
	log := cfg.Log.With("worker", w.spec.ID)
	attempt := 0
	for {
		if w.ctx.Err() != nil {
			w.update(func(s *Status) { s.State = StateStopped; s.PID = 0; s.Port = 0 })
			return
		}
		res := w.runOnce(log)
		if w.ctx.Err() != nil {
			w.update(func(s *Status) { s.State = StateStopped; s.PID = 0; s.Port = 0 })
			return
		}
		switch {
		case res.exitCode == ExitPermanent:
			log.Error("worker exited permanently; not restarting", "reason", res.reason)
			w.update(func(s *Status) {
				s.State = StateFailed
				s.PID, s.Port = 0, 0
				s.LastExit, s.LastExitCode = res.reason, res.exitCode
			})
			return
		case res.exitCode == ExitRecycle:
			attempt = 0 // deliberate memory recycle, not a fault
		case res.healthyFor >= cfg.StableAfter:
			attempt = 0 // it ran fine for a while; this is a fresh incident
		}
		delay := NextDelay(attempt, cfg.BackoffBase, cfg.BackoffMax, cfg.Jitter)
		if res.exitCode != ExitRecycle {
			attempt++
		}
		next := time.Now().Add(delay)
		log.Warn("worker down; restarting after backoff", "reason", res.reason, "delay", delay.String(), "attempt", attempt)
		w.update(func(s *Status) {
			s.State = StateBackoff
			s.PID, s.Port = 0, 0
			s.Restarts++
			s.Attempt = attempt
			s.NextStartAt = next
			s.LastExit, s.LastExitCode = res.reason, res.exitCode
		})
		select {
		case <-time.After(delay):
		case <-w.ctx.Done():
		}
	}
}

type runResult struct {
	exitCode   int
	reason     string
	healthyFor time.Duration
}

func (w *worker) runOnce(log *slog.Logger) runResult {
	cfg := w.sup.cfg
	port, err := freePort()
	if err != nil {
		return runResult{exitCode: -1, reason: "no free port: " + err.Error()}
	}
	args := make([]string, len(w.spec.Args))
	for i, a := range w.spec.Args {
		args[i] = strings.ReplaceAll(a, "{port}", strconv.Itoa(port))
	}
	env := append([]string{}, os.Environ()...)
	for _, e := range w.spec.Env {
		env = append(env, strings.ReplaceAll(e, "{port}", strconv.Itoa(port)))
	}
	cmd := exec.Command(cfg.Command, args...)
	cmd.Env = env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = sysProcAttr()
	if err := cmd.Start(); err != nil {
		return runResult{exitCode: -1, reason: "spawn failed: " + err.Error()}
	}
	pid := cmd.Process.Pid
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	started := time.Now()
	w.update(func(s *Status) {
		s.State = StateStarting
		s.PID, s.Port = pid, port
		s.StartedAt = started
		s.HealthFails = 0
		s.NextStartAt = time.Time{}
	})
	log.Info("worker process started", "pid", pid, "port", port)

	exitResult := func(err error, healthySince time.Time) runResult {
		code := exitCodeOf(err)
		r := runResult{exitCode: code, reason: fmt.Sprintf("exited code=%d", code)}
		if !healthySince.IsZero() {
			r.healthyFor = time.Since(healthySince)
		}
		return r
	}
	terminate := func(why string) {
		w.update(func(s *Status) { s.State = StateStopping })
		terminateProcess(cmd, exited, cfg.StopGrace, log, why)
	}

	// 1) Wait for the first healthy probe.
	var healthySince time.Time
	startDeadline := time.NewTimer(cfg.StartTimeout)
	defer startDeadline.Stop()
	tick := time.NewTicker(300 * time.Millisecond)
	defer tick.Stop()
ready:
	for {
		select {
		case err := <-exited:
			return exitResult(err, time.Time{})
		case <-w.ctx.Done():
			terminate("shutdown")
			return runResult{reason: "stopped"}
		case <-startDeadline.C:
			terminate("start timeout")
			return runResult{exitCode: -1, reason: "start timeout: no healthy probe"}
		case <-tick.C:
			if h, ok := w.probe(port); ok {
				healthySince = time.Now()
				w.markHealthy(h)
				break ready
			}
		}
	}

	// 2) Heartbeat loop.
	hb := time.NewTicker(cfg.HealthInterval)
	defer hb.Stop()
	for {
		select {
		case err := <-exited:
			return exitResult(err, healthySince)
		case <-w.ctx.Done():
			terminate("shutdown")
			return runResult{reason: "stopped"}
		case <-hb.C:
			if h, ok := w.probe(port); ok {
				w.markHealthy(h)
				continue
			}
			fails := 0
			w.update(func(s *Status) { s.HealthFails++; fails = s.HealthFails })
			log.Warn("worker health probe failed", "consecutive", fails)
			if fails >= cfg.HealthFailLimit {
				terminate("unresponsive")
				return runResult{exitCode: -1, reason: "unresponsive: killed after failed heartbeats", healthyFor: time.Since(healthySince)}
			}
		}
	}
}

func (w *worker) probe(port int) (Health, bool) {
	ctx, cancel := context.WithTimeout(w.ctx, w.sup.cfg.HealthTimeout)
	defer cancel()
	h, err := w.sup.cfg.Probe(ctx, port)
	if err != nil || !h.OK {
		return h, false
	}
	return h, true
}

func (w *worker) markHealthy(h Health) {
	w.update(func(s *Status) {
		s.State = StateRunning
		s.Health = h
		s.HealthFails = 0
		s.LastHealthAt = time.Now()
	})
}

func exitCodeOf(err error) int {
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			return 128 + int(ws.Signal())
		}
		return ee.ExitCode()
	}
	return -1
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// HTTPProbe returns a Probe that GETs http://127.0.0.1:<port>/healthz with the
// internal secret header the worker API requires.
func HTTPProbe(secret string) func(ctx context.Context, port int) (Health, error) {
	client := &http.Client{}
	return func(ctx context.Context, port int) (Health, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:"+strconv.Itoa(port)+"/healthz", nil)
		if err != nil {
			return Health{}, err
		}
		if secret != "" {
			req.Header.Set("X-Internal-Secret", secret)
		}
		resp, err := client.Do(req)
		if err != nil {
			return Health{}, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return Health{}, fmt.Errorf("healthz status %d", resp.StatusCode)
		}
		var h Health
		if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
			return Health{}, err
		}
		return h, nil
	}
}

func secondsUntil(t time.Time) float64 { return time.Until(t).Seconds() + 1 }
