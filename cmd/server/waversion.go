package main

import (
	"context"
	"log/slog"
	"time"

	"wacalls/internal/workerhost"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
)

// refreshWAVersion makes this process present itself to WhatsApp as the web client version that is current
// now, instead of the one whatsmeow was compiled with.
//
// WhatsApp turns away versions that are a few months old (events.ClientOutdated), and the version baked into
// the library is whatever was current when it was built. Without this, every deploy of an older build stops
// connecting, and pairing fails with "websocket disconnected before info query returned response".
//
// Call it once at start, before any connection exists: store.SetWAVersion is not safe to call while a client
// is connecting. If the lookup fails the built-in version stays, so a flaky lookup can never make things worse.
func refreshWAVersion(ctx context.Context, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	builtin := store.GetWAVersion()
	latest, err := whatsmeow.GetLatestVersion(ctx, nil)
	if err != nil {
		log.Warn("could not look up the current WhatsApp version; using the built-in one", "err", err, "builtin", builtin.String())
		return
	}
	if !builtin.LessThan(*latest) {
		log.Info("WhatsApp version is current", "version", builtin.String())
		return
	}
	store.SetWAVersion(*latest)
	log.Info("using the current WhatsApp version", "was", builtin.String(), "now", latest.String())
}

// outdatedExitCode decides what a worker does when WhatsApp rejects its client version.
//
//	ExitRecycle:   a newer web version is published, so a fresh process (which adopts it at start) can connect.
//	ExitPermanent: nothing newer exists, or it could not be looked up. Restarting would only hammer WhatsApp
//	               with the same rejected version, so park: only a whatsmeow update helps.
//
// It can never loop: after one restart the process already runs the newest version, so a second rejection
// finds nothing newer and parks.
func outdatedExitCode(current store.WAVersionContainer, latest *store.WAVersionContainer, lookupErr error) int {
	if lookupErr == nil && latest != nil && current.LessThan(*latest) {
		return workerhost.ExitRecycle
	}
	return workerhost.ExitPermanent
}
