package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"

	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
	_ "github.com/lib/pq"
	_ "modernc.org/sqlite"
)

type server struct {
	broker    *Broker
	sessions  *SessionManager
	log       *slog.Logger
	staticDir string
}

// openDB picks Postgres (Supabase) whenever DATABASE_URL is set - sessions
// then survive a Render restart/redeploy, unlike the old local SQLite file
// which lived in the container's ephemeral filesystem and reset on every
// restart (this was the root cause of WhatsApp sessions vanishing with
// "no such session" after any relay restart). DATABASE_URL should be
// Supabase's Postgres connection string, e.g. from the Session Pooler
// (port 6543, ?sslmode=require) so it works from a Render free-tier
// instance without IPv6.
//
// SQLite is kept as the local-dev-only fallback when DATABASE_URL is unset.
func openDB(dbPath string) (db *sql.DB, dialect string, err error) {
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		db, err = sql.Open("postgres", dsn)
		if err != nil {
			return nil, "", err
		}
		db.SetMaxOpenConns(5)
		return db, "postgres", nil
	}
	dsn := "file:" + dbPath + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)"
	db, err = sql.Open("sqlite", dsn)
	if err != nil {
		return nil, "", err
	}
	db.SetMaxOpenConns(1)
	return db, "sqlite", nil
}

func newServer(ctx context.Context, dbPath, staticDir string, maxCalls int, log *slog.Logger) (*server, error) {
	db, dialect, err := openDB(dbPath)
	if err != nil {
		return nil, err
	}
	waDialect := map[string]string{"postgres": "postgres", "sqlite": "sqlite3"}[dialect]
	container := sqlstore.NewWithDB(db, waDialect, waLog.Noop)
	if err := container.Upgrade(ctx); err != nil {
		return nil, err
	}
	store, err := newSessionStore(ctx, db, dialect)
	if err != nil {
		return nil, err
	}

	waLogger := waLog.Noop
	if log.Enabled(ctx, slog.LevelDebug) {
		waLogger = waLog.Stdout("WA", "INFO", true)
	}

	broker := NewBroker()
	mgr := newSessionManager(ctx, container, broker, store, waLogger, log, maxCalls)
	broker.SnapshotFn = mgr.snapshotEvents

	return &server{broker: broker, sessions: mgr, log: log, staticDir: staticDir}, nil
}
