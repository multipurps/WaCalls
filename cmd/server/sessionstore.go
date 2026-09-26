package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
)

type sessionRow struct {
	ID   string
	Name string
	JID  string
}

// sessionStore's own table (distinct from whatsmeow's sqlstore tables,
// which the Container manages itself). driver picks placeholder style and
// the rowid-ordering column, since database/sql doesn't do that rewriting
// and plain SQLite rowid doesn't exist on Postgres.
type sessionStore struct {
	db     *sql.DB
	driver string // "postgres" or "sqlite"
}

func newSessionStore(ctx context.Context, db *sql.DB, driver string) (*sessionStore, error) {
	ddl := `CREATE TABLE IF NOT EXISTS sessions (
		id       TEXT PRIMARY KEY,
		name     TEXT NOT NULL,
		jid      TEXT,
		created  BIGINT
	)`
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return nil, err
	}
	return &sessionStore{db: db, driver: driver}, nil
}

// ph returns this driver's placeholder for the nth (1-based) bind param.
func (s *sessionStore) ph(n int) string {
	if s.driver == "postgres" {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}

func newSessionID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *sessionStore) list(ctx context.Context) ([]sessionRow, error) {
	// "created" (set at insert time, see below) replaces SQLite's implicit
	// rowid as the ordering column, since Postgres has no rowid.
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, COALESCE(jid, '') FROM sessions ORDER BY created`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []sessionRow
	for rows.Next() {
		var r sessionRow
		if err := rows.Scan(&r.ID, &r.Name, &r.JID); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *sessionStore) insert(ctx context.Context, id, name string) error {
	q := fmt.Sprintf(
		`INSERT INTO sessions (id, name, jid, created) VALUES (%s, %s, NULL, extract(epoch from now())::bigint)`,
		s.ph(1), s.ph(2),
	)
	if s.driver != "postgres" {
		q = fmt.Sprintf(`INSERT INTO sessions (id, name, jid, created) VALUES (%s, %s, NULL, strftime('%%s','now'))`, s.ph(1), s.ph(2))
	}
	_, err := s.db.ExecContext(ctx, q, id, name)
	return err
}

func (s *sessionStore) setJID(ctx context.Context, id, jid string) error {
	q := fmt.Sprintf(`UPDATE sessions SET jid = %s WHERE id = %s`, s.ph(1), s.ph(2))
	_, err := s.db.ExecContext(ctx, q, jid, id)
	return err
}

func (s *sessionStore) delete(ctx context.Context, id string) error {
	q := fmt.Sprintf(`DELETE FROM sessions WHERE id = %s`, s.ph(1))
	_, err := s.db.ExecContext(ctx, q, id)
	return err
}
