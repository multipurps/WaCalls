//go:build !windows

package workerhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// ---- helper process -------------------------------------------------------
// The tests re-exec the test binary as a fake "worker". Behaviour is chosen by
// HELPER_MODE; HELPER_ID plays the part of the session id so tests can prove
// requests reach the right worker and that workers never share state.

func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	addr := os.Args[len(os.Args)-1]
	id := os.Getenv("HELPER_ID")
	mode := os.Getenv("HELPER_MODE")
	marker := os.Getenv("HELPER_MARKER")

	switch mode {
	case "crash":
		os.Exit(1)
	case "permanent":
		os.Exit(ExitPermanent)
	case "recycle":
		time.Sleep(150 * time.Millisecond)
		os.Exit(ExitRecycle)
	}

	mux := http.NewServeMux()
	var probes int
	var mu sync.Mutex
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		probes++
		n := probes
		mu.Unlock()
		if mode == "hang_after_3" && n > 3 {
			time.Sleep(10 * time.Second) // never answers within the probe timeout
		}
		_ = json.NewEncoder(w).Encode(Health{OK: true, SessionID: id, Connected: true, WAState: "open", RSSMB: 20})
	})
	mux.HandleFunc("/api/sessions/", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"servedBy": id, "path": r.URL.Path})
	})
	srv := &http.Server{Addr: addr, Handler: mux}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
	go func() {
		<-sigs
		if mode == "ignore_term" {
			select {} // never exits on its own: supervisor must SIGKILL
		}
		if marker != "" {
			_ = os.WriteFile(marker, []byte("clean "+id), 0o600)
		}
		os.Exit(0)
	}()
	_ = srv.ListenAndServe()
	os.Exit(3)
}

func testSup(t *testing.T, mut func(*Config)) *Supervisor {
	t.Helper()
	cfg := Config{
		Command:         os.Args[0],
		BackoffBase:     100 * time.Millisecond,
		BackoffMax:      800 * time.Millisecond,
		StableAfter:     time.Hour,
		StartTimeout:    5 * time.Second,
		HealthInterval:  100 * time.Millisecond,
		HealthTimeout:   200 * time.Millisecond,
		HealthFailLimit: 3,
		StopGrace:       500 * time.Millisecond,
	}
	if mut != nil {
		mut(&cfg)
	}
	s := New(cfg)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.Shutdown(ctx)
	})
	return s
}

func helperSpec(id string, env ...string) Spec {
	return Spec{
		ID:   id,
		Args: []string{"-test.run=TestHelperProcess", "--", "127.0.0.1:{port}"},
		Env:  append([]string{"GO_WANT_HELPER_PROCESS=1", "HELPER_ID=" + id}, env...),
	}
}

func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func ready(t *testing.T, s *Supervisor, id string) Status {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := s.WaitReady(ctx, id)
	if err != nil {
		t.Fatalf("worker %s not ready: %v", id, err)
	}
	return st
}

// ---- tests ----------------------------------------------------------------

func TestNextDelay(t *testing.T) {
	base, max := time.Second, 30*time.Second
	want := []time.Duration{1, 2, 4, 8, 16, 30, 30}
	for i, w := range want {
		if got := NextDelay(i, base, max, 0); got != w*time.Second {
			t.Errorf("attempt %d: got %v want %v", i, got, w*time.Second)
		}
	}
	for i := 0; i < 200; i++ { // jitter stays within +/-20%
		d := NextDelay(3, base, max, 0.2)
		if d < 6400*time.Millisecond || d > 9600*time.Millisecond {
			t.Fatalf("jittered delay out of range: %v", d)
		}
	}
}

// Two users linked at once: separate processes, ports and state; killing one
// must not disturb the other; each request reaches only its own worker.
func TestTwoSimultaneousUsersAreIsolated(t *testing.T) {
	s := testSup(t, nil)
	if err := s.Start(helperSpec("userA")); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(helperSpec("userB")); err != nil {
		t.Fatal(err)
	}
	a, b := ready(t, s, "userA"), ready(t, s, "userB")
	if a.PID == b.PID || a.Port == b.Port || a.PID == 0 || b.PID == 0 {
		t.Fatalf("workers must be separate processes/ports: A=%+v B=%+v", a, b)
	}
	if a.Health.SessionID != "userA" || b.Health.SessionID != "userB" {
		t.Fatalf("heartbeat cross-wired: A=%q B=%q", a.Health.SessionID, b.Health.SessionID)
	}

	// Routing: a request for A's session lands on A's process, B's on B's.
	for _, id := range []string{"userA", "userB", "userA"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/sessions/"+id+"/x", nil)
		if !s.Forward(rec, req, id) {
			t.Fatalf("Forward(%s) not handled", id)
		}
		var body map[string]string
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		if body["servedBy"] != id {
			t.Fatalf("request for %s was served by %q", id, body["servedBy"])
		}
	}

	// Crash A hard (SIGKILL, as an OOM kill would). B must not notice.
	if err := syscall.Kill(a.PID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "A restarted with a new pid", 5*time.Second, func() bool {
		st, _ := s.Get("userA")
		return st.State == StateRunning && st.PID != a.PID && st.Restarts >= 1
	})
	b2, _ := s.Get("userB")
	if b2.PID != b.PID || b2.State != StateRunning || b2.Restarts != 0 {
		t.Fatalf("user B was disturbed by user A's crash: before=%+v after=%+v", b, b2)
	}
}

func TestCrashLoopBacksOffExponentially(t *testing.T) {
	var mu sync.Mutex
	var starts []time.Time
	s := testSup(t, func(c *Config) {
		c.OnChange = func(st Status) {
			if st.State == StateStarting && st.PID != 0 {
				mu.Lock()
				if len(starts) == 0 || !st.StartedAt.Equal(starts[len(starts)-1]) {
					starts = append(starts, st.StartedAt)
				}
				mu.Unlock()
			}
		}
	})
	if err := s.Start(helperSpec("crashy", "HELPER_MODE=crash")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "5 start attempts", 10*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(starts) >= 5 })
	mu.Lock()
	defer mu.Unlock()
	// delays should be ~100,200,400,800 (capped at 800) between attempts
	min := []time.Duration{100, 200, 400, 800}
	for i := 0; i < 4; i++ {
		gap := starts[i+1].Sub(starts[i])
		if gap < min[i]*time.Millisecond {
			t.Errorf("gap %d = %v, want >= %v (backoff not growing)", i, gap, min[i]*time.Millisecond)
		}
	}
	st, _ := s.Get("crashy")
	if st.Attempt < 4 {
		t.Errorf("attempt counter = %d, want >= 4", st.Attempt)
	}
}

func TestPermanentExitIsNotRestarted(t *testing.T) {
	s := testSup(t, nil)
	_ = s.Start(helperSpec("loggedout", "HELPER_MODE=permanent"))
	waitFor(t, "failed state", 5*time.Second, func() bool {
		st, _ := s.Get("loggedout")
		return st.State == StateFailed
	})
	time.Sleep(500 * time.Millisecond) // > 4 backoff periods
	st, _ := s.Get("loggedout")
	if st.State != StateFailed || st.Restarts != 0 || st.LastExitCode != ExitPermanent {
		t.Fatalf("permanent exit must not restart: %+v", st)
	}
	// A failed worker does not hold a capacity slot and can be started again.
	if err := s.Start(helperSpec("loggedout")); err != nil {
		t.Fatalf("re-Start after permanent failure: %v", err)
	}
	ready(t, s, "loggedout")
}

func TestMemoryRecycleRestartsWithoutGrowingBackoff(t *testing.T) {
	s := testSup(t, nil)
	_ = s.Start(helperSpec("fat", "HELPER_MODE=recycle"))
	waitFor(t, "3 restarts", 8*time.Second, func() bool {
		st, _ := s.Get("fat")
		return st.Restarts >= 3
	})
	st, _ := s.Get("fat")
	if st.Attempt != 0 {
		t.Fatalf("recycle exits must not escalate backoff, attempt=%d", st.Attempt)
	}
}

func TestUnresponsiveWorkerIsKilledAndRestarted(t *testing.T) {
	s := testSup(t, nil)
	_ = s.Start(helperSpec("hung", "HELPER_MODE=hang_after_3"))
	first := ready(t, s, "hung")
	waitFor(t, "hung worker replaced", 8*time.Second, func() bool {
		st, _ := s.Get("hung")
		return st.Restarts >= 1 && st.PID != first.PID
	})
	if err := syscall.Kill(first.PID, 0); err == nil {
		// signal 0 succeeds only if the pid still exists (zombie reaped by Wait => ESRCH)
		t.Fatalf("hung worker pid %d still alive", first.PID)
	}
}

func TestCapacityLimit(t *testing.T) {
	s := testSup(t, func(c *Config) { c.MaxWorkers = 2 })
	for _, id := range []string{"a", "b"} {
		if err := s.Start(helperSpec(id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Start(helperSpec("c")); !errors.Is(err, ErrCapacity) {
		t.Fatalf("third worker: err=%v want ErrCapacity", err)
	}
	if err := s.Start(helperSpec("a")); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate: err=%v want ErrExists", err)
	}
	if err := s.Stop("a"); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(helperSpec("c")); err != nil {
		t.Fatalf("slot should be free after Stop: %v", err)
	}
}

func TestCleanShutdownTermsThenKills(t *testing.T) {
	dir := t.TempDir()
	mA, mB := filepath.Join(dir, "A"), filepath.Join(dir, "B")
	s := testSup(t, nil)
	_ = s.Start(helperSpec("A", "HELPER_MARKER="+mA))
	_ = s.Start(helperSpec("B", "HELPER_MARKER="+mB))
	_ = s.Start(helperSpec("stubborn", "HELPER_MODE=ignore_term"))
	for _, id := range []string{"A", "B", "stubborn"} {
		ready(t, s, id)
	}
	stubborn, _ := s.Get("stubborn")

	t0 := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.Shutdown(ctx)
	if el := time.Since(t0); el > 3*time.Second {
		t.Fatalf("shutdown took %v", el)
	}
	for _, m := range []string{mA, mB} {
		b, err := os.ReadFile(m)
		if err != nil || !strings.HasPrefix(string(b), "clean") {
			t.Fatalf("worker did not get SIGTERM and exit cleanly: %v %q", err, b)
		}
	}
	if err := syscall.Kill(stubborn.PID, 0); err == nil {
		t.Fatalf("worker ignoring SIGTERM was not SIGKILLed after grace")
	}
	for _, st := range s.List() {
		if st.State != StateStopped {
			t.Errorf("%s left in state %s", st.ID, st.State)
		}
	}
}

func TestForwardReturns503NotFoundForDownWorker(t *testing.T) {
	s := testSup(t, nil)
	_ = s.Start(helperSpec("down", "HELPER_MODE=crash"))
	waitFor(t, "backoff", 3*time.Second, func() bool { st, _ := s.Get("down"); return st.State == StateBackoff })
	rec := httptest.NewRecorder()
	if !s.Forward(rec, httptest.NewRequest("GET", "/api/sessions/down", nil), "down") {
		t.Fatal("known worker must be handled")
	}
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("down worker: code=%d retry-after=%q", rec.Code, rec.Header().Get("Retry-After"))
	}
	if s.Forward(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil), "unknown") {
		t.Fatal("unknown worker must report unhandled so the caller can 404")
	}
}

func TestHTTPProbeSendsSecret(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Internal-Secret") != "s3" {
			w.WriteHeader(401)
			return
		}
		_ = json.NewEncoder(w).Encode(Health{OK: true, SessionID: "z"})
	})}
	go srv.Serve(ln)
	defer srv.Close()
	port := ln.Addr().(*net.TCPAddr).Port
	if _, err := HTTPProbe("bad")(context.Background(), port); err == nil {
		t.Fatal("wrong secret must fail")
	}
	h, err := HTTPProbe("s3")(context.Background(), port)
	if err != nil || h.SessionID != "z" {
		t.Fatalf("probe: %+v %v", h, err)
	}
	_ = fmt.Sprint()
}
