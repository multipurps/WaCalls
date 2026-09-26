package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"time"

	"wacalls/internal/voip/call"
	"wacalls/internal/voip/core"
)

// aiReportInfo is set on an activeCall only when an AIBridge is attached to
// it (see doAttachAI in httpapi.go) - an ordinary browser-operated call has
// nil here and nothing gets reported, since there's no Audio-call- chat
// thread to report back into.
type aiReportInfo struct {
	AppUserID      string
	AppSessionID   string
	ContactName    string
	PeerIdentifier string
}

// reportRelayOutcome POSTs to Audio-call-'s /api/relay-call-status, the same
// endpoint mp-relay calls for Telegram, so a WhatsApp AI call gets the same
// "Finished the call with X" follow-up in its originating chat thread.
// Configured via APP_API_URL and RELAY_CALLBACK_SECRET; if either is unset,
// this is a silent no-op - exactly like before this feature existed.
//
// UNVERIFIED: never run against the real endpoint from here.
func reportRelayOutcome(log *slog.Logger, info *aiReportInfo, sd call.CallStateData) {
	appAPIURL := os.Getenv("APP_API_URL")
	callbackSecret := os.Getenv("RELAY_CALLBACK_SECRET")
	if appAPIURL == "" || callbackSecret == "" || info == nil {
		return
	}

	var status string
	var durationSeconds *int
	if sd.ConnectedAt != nil {
		status = "completed"
		d := sd.DurationSecs
		durationSeconds = &d
	} else {
		switch sd.EndReason {
		case core.EndCallReasonDeclined, core.EndCallReasonTimeout, core.EndCallReasonBusy, core.EndCallReasonDoNotDisturb:
			status = "no_answer"
		default:
			status = "failed"
		}
	}

	body, err := json.Marshal(map[string]any{
		"userId":          info.AppUserID,
		"sessionId":       info.AppSessionID,
		"platform":        "whatsapp",
		"status":          status,
		"durationSeconds": durationSeconds,
		"peerIdentifier":  info.PeerIdentifier,
		"contactName":     info.ContactName,
	})
	if err != nil {
		log.Error("reportRelayOutcome: marshal failed", "err", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, appAPIURL+"/api/relay-call-status", bytes.NewReader(body))
	if err != nil {
		log.Error("reportRelayOutcome: request build failed", "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Relay-Secret", callbackSecret)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Error("reportRelayOutcome: request failed", "err", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Error("reportRelayOutcome: rejected", "status", resp.StatusCode)
	}
}
