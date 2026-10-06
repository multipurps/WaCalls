package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	dbPath := flag.String("db", "wacalls.db", "SQLite session database path (used only if DATABASE_URL is unset)")
	staticDir := flag.String("static", "client/dist", "static client directory (optional)")
	debug := flag.Bool("debug", false, "verbose logging")
	maxCalls := flag.Int("max-calls-per-session", 8, "max concurrent calls per session (0 = unlimited)")

	// Phase 4: process-per-session. "single" is the original behaviour and the
	// default, so nothing changes until WACALLS_MODE=manager is set on purpose.
	mode := flag.String("mode", envOr("WACALLS_MODE", "single"), "single | manager | worker")
	sessionID := flag.String("session", "", "worker mode: the one session id this process owns")
	memSoftMB := flag.Int("mem-soft-mb", 80, "worker mode: Go soft memory limit in MB")
	memHardMB := flag.Int("mem-hard-mb", 110, "worker mode: recycle the process above this RSS in MB (0 = off)")
	maxWorkers := flag.Int("max-workers", envInt("WACALLS_MAX_WORKERS", 4), "manager mode: max concurrent worker processes")
	workerCalls := flag.Int("worker-max-calls", envInt("WACALLS_WORKER_MAX_CALLS", 4), "manager mode: max concurrent calls per worker")
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	switch *mode {
	case "worker":
		if *sessionID == "" {
			log.Error("worker mode needs -session")
			os.Exit(2)
		}
		code := runWorker(ctx, log, workerOpts{
			addr: *addr, dbPath: *dbPath, sessionID: *sessionID, maxCalls: *maxCalls,
			memSoftMB: *memSoftMB, memHardMB: *memHardMB,
		})
		stop()
		os.Exit(code) // exit code is the contract with the supervisor (see workerhost)
	case "manager":
		code := runManager(ctx, log, managerOpts{
			addr: *addr, dbPath: *dbPath, maxWorkers: *maxWorkers, workerCalls: *workerCalls,
			workerSoftMB: *memSoftMB, workerHardMB: *memHardMB,
		})
		stop()
		os.Exit(code)
	case "single":
		runSingle(ctx, stop, log, *addr, *dbPath, *staticDir, *maxCalls)
	default:
		log.Error("unknown -mode", "mode", *mode)
		os.Exit(2)
	}
}

// runSingle is the original main(), unchanged in behaviour: every session in
// this one process.
func runSingle(ctx context.Context, stop context.CancelFunc, log *slog.Logger, addr, dbPath, staticDir string, maxCalls int) {
	defer stop()

	srv, err := newServer(ctx, dbPath, staticDir, maxCalls, log)
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1)
	}
	defer srv.sessions.disconnectAll()

	if err := srv.sessions.Restore(ctx); err != nil {
		log.Error("session restore failed", "err", err)
		os.Exit(1)
	}

	httpSrv := &http.Server{Addr: addr, Handler: srv.routes()}
	go func() {
		log.Info("HTTP server listening", "addr", addr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server error", "err", err)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
