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

// reportRelayOutcome POSTs to Audio-call-'s existing api/social-calling.js
// (?action=relay-call-status) - not a standalone endpoint, because that
// app's Vercel plan is already at its serverless-function cap. Same
// endpoint mp-relay calls for Telegram (see that repo's bin/server.php),
// so a WhatsApp AI call gets the same "Finished the call with X" follow-up
// in its originating chat thread. Configured via APP_API_URL and
// RELAY_CALLBACK_SECRET; if either is unset, this is a silent no-op -
// exactly like before this feature existed.
//
// UNVERIFIED: never run against the real endpoint from here.
func reportRelayOutcome(log *slog.Logger, info *aiReportInfo, sd call.CallStateData) {
	appAPIURL := os.Getenv("APP_API_URL")
	callbackSecret := os.Getenv("RELAY_CALLBACK_SECRET")
	if appAPIURL == "" || callbackSecret == "" || info == nil {
		return
	}

	status, durationSeconds := outcomeStatus(sd)
	postRelayStatus(log, info, status, durationSeconds, string(sd.EndReason))
}

// outcomeStatus maps how a call ended to the status the app understands.
// Pure so it can be tested: a rejection is reported as "rejected", never folded
// into no_answer or failed.
func outcomeStatus(sd call.CallStateData) (string, *int) {
	if sd.ConnectedAt != nil {
		d := sd.DurationSecs
		return "completed", &d
	}
	switch sd.EndReason {
	case core.EndCallReasonDeclined:
		return "rejected", nil
	case core.EndCallReasonTimeout, core.EndCallReasonBusy, core.EndCallReasonDoNotDisturb:
		return "no_answer", nil
	default:
		return "failed", nil
	}
}

// reportRelayStatus tells the app a non-terminal provider state (currently
// only "ringing"). Same endpoint and secret as the outcome report; the app
// ignores a ringing report that arrives after the call was answered/ended.
func reportRelayStatus(log *slog.Logger, info *aiReportInfo, status string) {
	postRelayStatus(log, info, status, nil, "")
}

func postRelayStatus(log *slog.Logger, info *aiReportInfo, status string, durationSeconds *int, reason string) {
	appAPIURL := os.Getenv("APP_API_URL")
	callbackSecret := os.Getenv("RELAY_CALLBACK_SECRET")
	if appAPIURL == "" || callbackSecret == "" || info == nil {
		return
	}
	body, err := json.Marshal(map[string]any{
		"userId":          info.AppUserID,
		"sessionId":       info.AppSessionID,
		"platform":        "whatsapp",
		"status":          status,
		"reason":          reason,
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, appAPIURL+"/api/social-calling?action=relay-call-status", bytes.NewReader(body))
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
