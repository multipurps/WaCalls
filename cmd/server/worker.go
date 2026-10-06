package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"wacalls/internal/voip/core"
	"wacalls/internal/workerhost"

	"go.mau.fi/whatsmeow/types/events"
)

// Worker mode: this process owns exactly ONE WhatsApp session. It is the
// existing WaCalls server (same HTTP API, same call handling, same ACAF
// bridge) restricted to one session and wrapped with the lifecycle pieces a
// supervisor needs: /healthz, a WhatsApp reconnect loop with exponential
// backoff, memory guards, a stale-call reaper and clean shutdown.
// Nothing about call signalling or the assistant protocol changes here.

type workerOpts struct {
	addr      string
	dbPath    string
	sessionID string
	maxCalls  int
	memSoftMB int // GC works harder above this (Go soft limit)
	memHardMB int // recycle the process above this RSS
}

type workerState struct {
	log      *slog.Logger
	srv      *server
	id       string
	opts     workerOpts
	started  time.Time
	draining atomic.Bool
	exit     chan int // watchdogs request process exit with a code
}

func (w *workerState) requestExit(code int) {
	select {
	case w.exit <- code:
	default:
	}
}

func runWorker(ctx context.Context, log *slog.Logger, o workerOpts) int {
	log = log.With("worker", o.sessionID)
	slog.SetDefault(log)

	if o.memSoftMB > 0 {
		debug.SetMemoryLimit(int64(o.memSoftMB) << 20)
	}
	debug.SetGCPercent(50) // smaller heap headroom; fine for a signalling-heavy, low-alloc process

	srv, err := newServer(ctx, o.dbPath, "", o.maxCalls, log)
	if err != nil {
		log.Error("worker startup failed", "err", err)
		return 1
	}
	w := &workerState{log: log, srv: srv, id: o.sessionID, opts: o, started: time.Now(), exit: make(chan int, 1)}
	srv.sessions.onEvent = w.onEvent
	srv.health = w.health

	if err := srv.sessions.RestoreOne(ctx, o.sessionID, os.Getenv("WACALLS_PAIR_PHONE")); err != nil {
		log.Error("session restore failed", "err", err)
		var pe permanentError
		if errors.As(err, &pe) {
			return workerhost.ExitPermanent
		}
		return 1
	}

	// Listen only after the session is registered, so the first successful
	// /healthz means "GET /api/sessions/{sid} will not 404".
	httpSrv := &http.Server{Addr: o.addr, Handler: srv.routes()}
	listenErr := make(chan error, 1)
	go func() {
		log.Info("worker listening", "addr", o.addr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			listenErr <- err
		}
	}()

	go w.memoryLoop(ctx)
	go w.reconnectLoop(ctx)
	go w.reaperLoop(ctx)
	go w.heartbeatLog(ctx)

	code := 0
	select {
	case <-ctx.Done():
		log.Info("worker received shutdown signal")
	case code = <-w.exit:
		log.Warn("worker exiting on watchdog request", "code", code)
	case err := <-listenErr:
		log.Error("worker http server failed", "err", err)
		return 1
	}

	// Clean shutdown: refuse new work, end calls (closes AI bridges and tells
	// WhatsApp the calls ended), drop the WhatsApp socket, stop HTTP.
	w.draining.Store(true)
	srv.sessions.refuseNew.Store(true)
	done := make(chan struct{})
	go func() { srv.sessions.disconnectAll(); close(done) }()
	select {
	case <-done:
	case <-time.After(7 * time.Second):
		log.Warn("call teardown timed out; exiting anyway")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	return code
}

func (w *workerState) health() workerhost.Health {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	h := workerhost.Health{
		OK:         true,
		SessionID:  w.id,
		HeapMB:     float64(ms.HeapAlloc) / (1 << 20),
		RSSMB:      rssMB(),
		Goroutines: runtime.NumGoroutine(),
		UptimeSec:  int64(time.Since(w.started).Seconds()),
		Draining:   w.draining.Load(),
	}
	if s := w.srv.sessions.only(); s != nil {
		h.WAState = s.info().State
		h.Connected = s.client.IsConnected()
		h.ActiveCalls = s.reg.count()
	}
	return h
}

// onEvent turns WhatsApp-side terminal conditions into a permanent exit so the
// supervisor stops instead of hammering WhatsApp with a dead login.
func (w *workerState) onEvent(raw any) {
	switch raw.(type) {
	case *events.LoggedOut:
		w.log.Error("whatsapp logged this device out; worker will not restart")
		w.requestExit(workerhost.ExitPermanent)
	case *events.StreamReplaced:
		w.log.Error("another client replaced this session's stream; worker will not restart")
		w.requestExit(workerhost.ExitPermanent)
	case *events.ClientOutdated:
		w.log.Error("whatsapp rejected this client version as outdated; needs a whatsmeow update")
		w.requestExit(workerhost.ExitPermanent)
	case *events.TemporaryBan:
		w.log.Error("whatsapp temporarily banned this account; worker will not restart")
		w.requestExit(workerhost.ExitPermanent)
	}
}

// memoryLoop is the per-worker OOM guard. The container (512 MB on Render
// Free) is shared by every worker, so one runaway worker would get ALL of them
// OOM-killed. Instead each worker polices itself: above the hard RSS limit it
// stops accepting calls and recycles as soon as no call is live; if memory
// keeps climbing (hard*1.25) it exits even mid-call, because losing one call
// beats losing every user's session.
func (w *workerState) memoryLoop(ctx context.Context) {
	hard := float64(w.opts.memHardMB)
	if hard <= 0 {
		return
	}
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		rss := rssMB()
		if rss <= hard {
			continue
		}
		w.srv.sessions.refuseNew.Store(true)
		calls := 0
		if s := w.srv.sessions.only(); s != nil {
			calls = s.reg.count()
		}
		if calls == 0 || rss > hard*1.25 {
			w.log.Error("memory limit exceeded; recycling worker", "rss_mb", rss, "limit_mb", hard, "active_calls", calls)
			w.requestExit(workerhost.ExitRecycle)
			return
		}
		w.log.Warn("memory over limit; refusing new calls, recycling when idle", "rss_mb", rss, "active_calls", calls)
	}
}

// reconnectLoop backs up whatsmeow's own auto-reconnect. whatsmeow gets the
// first 30s. If the socket is still down it forces Disconnect+Connect with
// exponential backoff (2s..60s, jittered). After 8 failed forced attempts it
// exits so the supervisor starts a fresh process (its own backoff applies).
func (w *workerState) reconnectLoop(ctx context.Context) {
	const (
		grace       = 30 * time.Second
		maxAttempts = 8
	)
	var downSince, nextTry time.Time
	attempt := 0
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		s := w.srv.sessions.only()
		if s == nil || s.client.Store.ID == nil || w.draining.Load() {
			downSince, attempt = time.Time{}, 0 // not paired yet: nothing to reconnect
			continue
		}
		if s.client.IsConnected() {
			downSince, attempt = time.Time{}, 0
			continue
		}
		now := time.Now()
		if downSince.IsZero() {
			downSince, nextTry = now, now.Add(grace)
			w.log.Warn("whatsapp connection is down; waiting for auto-reconnect")
			continue
		}
		if now.Before(nextTry) {
			continue
		}
		if attempt >= maxAttempts {
			w.log.Error("whatsapp reconnect kept failing; exiting for a fresh process", "attempts", attempt)
			w.requestExit(1)
			return
		}
		attempt++
		w.log.Warn("forcing whatsapp reconnect", "attempt", attempt)
		s.client.Disconnect()
		if err := s.client.Connect(); err != nil {
			w.log.Warn("reconnect attempt failed", "attempt", attempt, "err", err)
		}
		nextTry = now.Add(workerhost.NextDelay(attempt, 2*time.Second, 60*time.Second, 0.2))
	}
}

// reaperLoop is the call-state cleanup for the live process: a call that rings
// forever, or runs past WACALLS_MAX_CALL_MINUTES (default 60), is ended and, if
// it still lingers, force-removed so registry/broker entries and the AI bridge
// websocket cannot leak. (On a crash the whole process's call state vanishes
// with it; that is the point of one process per user.)
func (w *workerState) reaperLoop(ctx context.Context) {
	maxCall := time.Duration(envInt("WACALLS_MAX_CALL_MINUTES", 60)) * time.Minute
	maxRing := 2 * time.Minute
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		for _, c := range w.srv.broker.staleCalls(maxRing, maxCall) {
			c := c
			sess, ok := w.srv.sessions.Get(c.SessionID)
			if !ok {
				continue
			}
			w.log.Warn("reaping stale call", "call_id", c.CallID, "status", c.Status)
			go func() {
				sess.terminateCall(c.CallID, core.EndCallReasonUserEnded)
				time.Sleep(5 * time.Second)
				if _, still := w.srv.broker.getCall(c.CallID); still {
					sess.removeCall(c.CallID)
					w.srv.broker.endCall(c.CallID, "reaped")
				}
			}()
		}
	}
}

func (w *workerState) heartbeatLog(ctx context.Context) {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h := w.health()
			w.log.Info("worker heartbeat", "wa_state", h.WAState, "connected", h.Connected,
				"calls", h.ActiveCalls, "rss_mb", int(h.RSSMB), "heap_mb", int(h.HeapMB), "goroutines", h.Goroutines)
		}
	}
}

// rssMB is the process's resident set size (what the container limit counts).
// Falls back to Go's own accounting where /proc is unavailable.
func rssMB() float64 {
	if b, err := os.ReadFile("/proc/self/statm"); err == nil {
		if f := strings.Fields(string(b)); len(f) >= 2 {
			if pages, err := strconv.ParseUint(f[1], 10, 64); err == nil {
				return float64(pages*uint64(os.Getpagesize())) / (1 << 20)
			}
		}
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return float64(ms.Sys-ms.HeapReleased) / (1 << 20)
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return def
}
