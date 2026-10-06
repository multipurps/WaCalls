package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"wacalls/internal/workerhost"

	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// Manager mode: this process holds NO WhatsApp connection. It owns the
// sessions table, supervises one worker process per paired session
// (internal/workerhost), and reverse-proxies the unchanged WaCalls HTTP API to
// the right worker by session id. The Emysa app keeps calling the same URLs
// with the same X-Internal-Secret; only what sits behind them changed.

type managerOpts struct {
	addr         string
	dbPath       string
	maxWorkers   int
	workerCalls  int
	workerSoftMB int
	workerHardMB int
}

type manager struct {
	log     *slog.Logger
	opts    managerOpts
	secret  string
	db      *sql.DB
	dialect string
	store   *sessionStore
	sup     *workerhost.Supervisor
	started time.Time

	mu     sync.Mutex
	parked map[string]sessionRow // paired sessions that did not fit under maxWorkers

	mirrorCh   chan workerhost.Status
	lastMirror map[string]mirrorMark
}

type mirrorMark struct {
	state workerhost.State
	at    time.Time
}

func runManager(ctx context.Context, log *slog.Logger, o managerOpts) int {
	secret := os.Getenv("WACALLS_INTERNAL_SECRET")
	if secret == "" {
		log.Error("WACALLS_INTERNAL_SECRET is required in manager mode")
		return 1
	}
	exe, err := os.Executable()
	if err != nil {
		log.Error("cannot locate own binary to spawn workers", "err", err)
		return 1
	}
	debug.SetMemoryLimit(64 << 20) // the manager holds no WhatsApp state; keep it small

	db, dialect, err := openDB(o.dbPath)
	if err != nil {
		log.Error("database open failed", "err", err)
		return 1
	}
	waDialect := map[string]string{"postgres": "postgres", "sqlite": "sqlite3"}[dialect]
	if err := sqlstore.NewWithDB(db, waDialect, waLog.Noop).Upgrade(ctx); err != nil { // once, here, never in workers
		log.Error("schema upgrade failed", "err", err)
		return 1
	}
	store, err := newSessionStore(ctx, db, dialect)
	if err != nil {
		log.Error("session store init failed", "err", err)
		return 1
	}

	m := &manager{
		log: log, opts: o, secret: secret, db: db, dialect: dialect, store: store, started: time.Now(),
		parked:   map[string]sessionRow{},
		mirrorCh: make(chan workerhost.Status, 64), lastMirror: map[string]mirrorMark{},
	}
	if err := m.ensureStatusTable(ctx); err != nil {
		log.Warn("worker status table unavailable; continuing without DB mirror", "err", err)
	}
	m.sup = workerhost.New(workerhost.Config{
		Command:         exe,
		MaxWorkers:      o.maxWorkers,
		BackoffBase:     2 * time.Second,
		BackoffMax:      2 * time.Minute,
		Jitter:          0.2,
		StableAfter:     90 * time.Second,
		StartTimeout:    45 * time.Second,
		HealthInterval:  10 * time.Second,
		HealthTimeout:   3 * time.Second,
		HealthFailLimit: 3,
		StopGrace:       10 * time.Second,
		Probe:           workerhost.HTTPProbe(secret),
		OnChange:        m.enqueueMirror,
		Log:             log,
	})
	go m.mirrorLoop(ctx)

	if err := m.startAll(ctx); err != nil {
		log.Error("could not list sessions", "err", err)
		return 1
	}

	httpSrv := &http.Server{Addr: o.addr, Handler: m.routes()}
	listenErr := make(chan error, 1)
	go func() {
		log.Info("manager listening", "addr", o.addr, "max_workers", o.maxWorkers)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			listenErr <- err
		}
	}()

	code := 0
	select {
	case <-ctx.Done():
		log.Info("manager shutting down")
	case err := <-listenErr:
		log.Error("manager http server failed", "err", err)
		code = 1
	}
	sdCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_ = httpSrv.Shutdown(sdCtx)
	cancel()
	wCtx, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	m.sup.Shutdown(wCtx) // SIGTERM every worker, SIGKILL after StopGrace
	cancel2()
	return code
}

// startAll boots one worker per paired session, up to maxWorkers. Rows with no
// jid are abandoned pairings (same cleanup the single-process Restore does).
func (m *manager) startAll(ctx context.Context) error {
	rows, err := m.store.list(ctx)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.JID == "" {
			_ = m.store.delete(ctx, row.ID)
			continue
		}
		if err := m.sup.Start(m.spec(row.ID, "")); err != nil {
			if errors.Is(err, workerhost.ErrCapacity) {
				m.mu.Lock()
				m.parked[row.ID] = row
				m.mu.Unlock()
				m.log.Warn("session parked: worker capacity reached", "session", row.ID, "max_workers", m.opts.maxWorkers)
				continue
			}
			m.log.Error("could not start worker", "session", row.ID, "err", err)
		}
	}
	m.log.Info("workers started", "paired_sessions", len(rows), "parked", len(m.parked))
	return nil
}

func (m *manager) spec(id, phone string) workerhost.Spec {
	args := []string{
		"-mode", "worker", "-session", id, "-addr", "127.0.0.1:{port}", "-db", m.opts.dbPath,
		"-max-calls-per-session", strconv.Itoa(m.opts.workerCalls),
		"-mem-soft-mb", strconv.Itoa(m.opts.workerSoftMB),
		"-mem-hard-mb", strconv.Itoa(m.opts.workerHardMB),
	}
	env := []string{"WACALLS_DB_MAX_CONNS=2", "WACALLS_SKIP_MIGRATE=1"}
	if phone != "" {
		env = append(env, "WACALLS_PAIR_PHONE="+phone) // env, not argv: keeps the number out of `ps`
	}
	return workerhost.Spec{ID: id, Args: args, Env: env}
}

func (m *manager) routes() http.Handler {
	api := http.NewServeMux()
	api.HandleFunc("GET /api/workers", m.handleWorkers)
	api.HandleFunc("GET /api/sessions", m.handleList)
	api.HandleFunc("POST /api/sessions", m.handleCreate)
	api.HandleFunc("DELETE /api/sessions/{sid}", m.handleDelete)
	api.HandleFunc("GET /api/sessions/{sid}", m.handleProxy)
	api.HandleFunc("/api/sessions/{sid}/", m.handleProxy)
	api.HandleFunc("GET /api/events", m.handleEvents)

	top := http.NewServeMux()
	// Unauthenticated liveness for Render's health check: counts only, no ids.
	top.HandleFunc("GET /healthz", m.handleHealthz)
	// No withCORS here: proxied responses already carry the worker's own CORS
	// headers, and adding a second set makes browsers reject the response.
	top.Handle("/", withAuth(api))
	return top
}

func (m *manager) handleHealthz(w http.ResponseWriter, r *http.Request) {
	running, total := 0, 0
	for _, st := range m.sup.List() {
		total++
		if st.State == workerhost.StateRunning {
			running++
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "role": "manager", "workersRunning": running, "workersTotal": total})
}

func (m *manager) handleWorkers(w http.ResponseWriter, r *http.Request) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	m.mu.Lock()
	parked := make([]string, 0, len(m.parked))
	for id := range m.parked {
		parked = append(parked, id)
	}
	m.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"manager": map[string]any{
			"rssMb": rssMB(), "heapMb": float64(ms.HeapAlloc) / (1 << 20), "goroutines": runtime.NumGoroutine(),
			"uptimeSec": int64(time.Since(m.started).Seconds()),
		},
		"maxWorkers": m.opts.maxWorkers,
		"workers":    m.sup.List(),
		"parked":     parked,
	})
}

// handleList builds the session list from the DB rows plus supervisor state:
// no fan-out HTTP calls to workers, so it stays cheap and answers even while
// workers are restarting.
func (m *manager) handleList(w http.ResponseWriter, r *http.Request) {
	rows, err := m.store.list(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	out := make([]SessionInfo, 0, len(rows))
	for _, row := range rows {
		out = append(out, m.info(row))
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessions": out})
}

func (m *manager) info(row sessionRow) SessionInfo {
	info := SessionInfo{ID: row.ID, Name: row.Name, JID: row.JID, State: "connecting", Paired: row.JID != ""}
	st, ok := m.sup.Get(row.ID)
	switch {
	case !ok:
		info.State = "parked"
	case st.State == workerhost.StateRunning && st.Health.WAState != "":
		info.State = st.Health.WAState
		info.Paired = info.Paired || st.Health.WAState == "open"
	case st.State == workerhost.StateFailed:
		info.State, info.Paired = "logged_out", false
	}
	return info
}

func (m *manager) handleCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name  string `json:"name"`
		Phone string `json:"phone"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	name := strings.TrimSpace(body.Name)
	if name == "" {
		name = "Session"
	}
	id := newSessionID()
	if err := m.store.insert(r.Context(), id, name); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if err := m.sup.Start(m.spec(id, strings.TrimSpace(body.Phone))); err != nil {
		_ = m.store.delete(r.Context(), id)
		code := http.StatusInternalServerError
		if errors.Is(err, workerhost.ErrCapacity) {
			code = http.StatusServiceUnavailable // at capacity: move to a bigger host
		}
		writeJSON(w, code, map[string]string{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()
	if _, err := m.sup.WaitReady(ctx, id); err != nil {
		_ = m.sup.Stop(id)
		_ = m.store.delete(context.Background(), id)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "start pairing: " + err.Error()})
		return
	}
	m.log.Info("session created", "session", id, "name", name)
	writeJSON(w, http.StatusOK, map[string]string{"id": id})
}

func (m *manager) handleDelete(w http.ResponseWriter, r *http.Request) {
	sid := r.PathValue("sid")
	st, known := m.sup.Get(sid)
	if known && st.State == workerhost.StateRunning {
		// Let the worker do the real work (WhatsApp logout, device + row delete).
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		m.sup.Forward(rec, r, sid)
		if rec.code >= 200 && rec.code < 300 {
			_ = m.sup.Stop(sid)
		}
		return
	}
	m.mu.Lock()
	_, wasParked := m.parked[sid]
	delete(m.parked, sid)
	m.mu.Unlock()
	if !known && !wasParked {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no session " + sid})
		return
	}
	// Worker is down/parked: we cannot log out of WhatsApp from here. Remove our
	// record and stop supervising; the whatsmeow device rows stay (orphaned).
	m.log.Warn("deleting session while its worker is not running; whatsmeow device left in place", "session", sid)
	_ = m.sup.Stop(sid)
	_ = m.store.delete(r.Context(), sid)
	w.WriteHeader(http.StatusNoContent)
}

func (m *manager) handleProxy(w http.ResponseWriter, r *http.Request) {
	sid := r.PathValue("sid")
	if m.sup.Forward(w, r, sid) {
		return
	}
	m.mu.Lock()
	_, isParked := m.parked[sid]
	m.mu.Unlock()
	if isParked {
		w.Header().Set("Retry-After", "60")
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "session parked: worker capacity reached"})
		return
	}
	writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such session"})
}

// handleEvents: the single-process server had one global SSE stream. With one
// process per session there is no global stream, so SSE is per session.
func (m *manager) handleEvents(w http.ResponseWriter, r *http.Request) {
	sid := r.URL.Query().Get("session")
	if sid == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "manager mode: /api/events needs ?session=<id>"})
		return
	}
	if !m.sup.Forward(w, r, sid) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such session"})
	}
}

// statusRecorder captures the status code a proxied response used.
type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(c int) { s.code = c; s.ResponseWriter.WriteHeader(c) }
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// ---- Supabase mirror: worker health visible to the dashboard ---------------

func (m *manager) ensureStatusTable(ctx context.Context) error {
	if m.dialect != "postgres" {
		return errors.New("status mirror is postgres-only")
	}
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS wa_worker_status (
			session_id     TEXT PRIMARY KEY,
			state          TEXT NOT NULL,
			pid            INT,
			restarts       INT NOT NULL DEFAULT 0,
			last_heartbeat TIMESTAMPTZ,
			rss_mb         REAL,
			active_calls   INT NOT NULL DEFAULT 0,
			wa_state       TEXT,
			connected      BOOLEAN NOT NULL DEFAULT false,
			last_exit      TEXT,
			updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		// Same default-deny posture as the session tables: only the DB role this
		// service connects with (which bypasses RLS) can read it.
		`ALTER TABLE wa_worker_status ENABLE ROW LEVEL SECURITY`,
	} {
		if _, err := m.db.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// enqueueMirror is the supervisor's OnChange hook. It never blocks the
// supervisor: state transitions are always queued, heartbeats at most every
// 30s per worker, and a full queue drops the update.
func (m *manager) enqueueMirror(st workerhost.Status) {
	m.mu.Lock()
	prev := m.lastMirror[st.ID]
	if st.State == prev.state && time.Since(prev.at) < 30*time.Second {
		m.mu.Unlock()
		return
	}
	m.lastMirror[st.ID] = mirrorMark{state: st.State, at: time.Now()}
	m.mu.Unlock()
	select {
	case m.mirrorCh <- st:
	default:
	}
}

func (m *manager) mirrorLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case st := <-m.mirrorCh:
			if m.dialect != "postgres" {
				continue
			}
			var hb any
			if !st.LastHealthAt.IsZero() {
				hb = st.LastHealthAt
			}
			wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, err := m.db.ExecContext(wctx, `
				INSERT INTO wa_worker_status (session_id, state, pid, restarts, last_heartbeat, rss_mb, active_calls, wa_state, connected, last_exit, updated_at)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, now())
				ON CONFLICT (session_id) DO UPDATE SET
					state=$2, pid=$3, restarts=$4, last_heartbeat=COALESCE($5, wa_worker_status.last_heartbeat),
					rss_mb=$6, active_calls=$7, wa_state=$8, connected=$9, last_exit=$10, updated_at=now()`,
				st.ID, string(st.State), st.PID, st.Restarts, hb, st.Health.RSSMB, st.Health.ActiveCalls,
				st.Health.WAState, st.Health.Connected, st.LastExit)
			cancel()
			if err != nil {
				m.log.Debug("worker status mirror write failed", "err", err)
			}
		}
	}
}
