package main

import (
	"errors"
	"testing"

	"wacalls/internal/workerhost"

	"go.mau.fi/whatsmeow/store"
)

func TestOutdatedExitCode(t *testing.T) {
	old := store.WAVersionContainer{2, 3000, 1041871181}
	newer := store.WAVersionContainer{2, 3000, 1050728964}

	// WhatsApp rejected us and a newer version is published: restart, the new process adopts it.
	if got := outdatedExitCode(old, &newer, nil); got != workerhost.ExitRecycle {
		t.Fatalf("newer version published: got exit %d, want recycle (%d)", got, workerhost.ExitRecycle)
	}
	// Already on the newest: restarting cannot help, so park. This is what stops a restart loop.
	if got := outdatedExitCode(newer, &newer, nil); got != workerhost.ExitPermanent {
		t.Fatalf("already newest: got exit %d, want permanent (%d)", got, workerhost.ExitPermanent)
	}
	// A published version older than ours is not an upgrade.
	if got := outdatedExitCode(newer, &old, nil); got != workerhost.ExitPermanent {
		t.Fatalf("older published: got exit %d, want permanent", got)
	}
	// The lookup failed: do not restart blindly.
	if got := outdatedExitCode(old, nil, errors.New("blocked")); got != workerhost.ExitPermanent {
		t.Fatalf("lookup failed: got exit %d, want permanent", got)
	}
	if got := outdatedExitCode(old, &newer, errors.New("partial")); got != workerhost.ExitPermanent {
		t.Fatalf("lookup error with a value: got exit %d, want permanent", got)
	}
}
