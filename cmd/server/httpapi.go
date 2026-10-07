package main

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"wacalls/internal/voip/call"
	"wacalls/internal/voip/core"

	"go.mau.fi/whatsmeow/types"
)

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/sessions", s.handleSessionList)
	mux.HandleFunc("GET /api/sessions/{sid}", s.handleSessionDetail)
	mux.HandleFunc("POST /api/sessions", s.handleSessionCreate)
	mux.HandleFunc("DELETE /api/sessions/{sid}", s.handleSessionDelete)
	mux.HandleFunc("POST /api/sessions/{sid}/logout", s.handleSessionLogout)
	mux.HandleFunc("POST /api/sessions/{sid}/pair", s.handleSessionPair)
	mux.HandleFunc("POST /api/sessions/{sid}/pair/code", s.handleSessionPairCode)
	mux.HandleFunc("POST /api/sessions/{sid}/calls", s.handleStartCall)
	mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/webrtc", s.handleWebRTC)
	mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/ai", s.handleAttachAI)
	mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/accept", s.handleAccept)
	mux.HandleFunc("POST /api/sessions/{sid}/calls/{id}/reject", s.handleReject)
	mux.HandleFunc("DELETE /api/sessions/{sid}/calls/{id}", s.handleEndCall)
	mux.HandleFunc("GET /api/sessions/{sid}/history", s.handleHistory)

	mux.HandleFunc("GET /api/events", s.handleEvents)
	mux.HandleFunc("GET /healthz", s.handleHealthz)

	if s.staticDir != "" {
		if _, err := os.Stat(s.staticDir); err == nil {
			mux.Handle("/", http.FileServer(http.Dir(s.staticDir)))
		}
	}
	return withAuth(withCORS(mux))
}

// This API had no authentication at all before this - anyone who found the
// URL could create sessions, pair a WhatsApp account, or place calls
// through it. Matches the pattern already used by this app's other relay
// (server-social/social-relay.js): a shared secret header, checked before
// anything else runs. Set WACALLS_INTERNAL_SECRET to enable; if it's unset,
// this refuses to serve anything rather than silently running open.
func withAuth(h http.Handler) http.Handler {
	secret := os.Getenv("WACALLS_INTERNAL_SECRET")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			h.ServeHTTP(w, r)
			return
		}
		if secret == "" || r.Header.Get("X-Internal-Secret") != secret {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		h.ServeHTTP(w, r)
	})
}

func withCORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Client-Id")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func clientID(r *http.Request) string {
	if id := r.Header.Get("X-Client-Id"); id != "" {
		return id
	}
	return r.URL.Query().Get("clientId")
}

func (s *server) sessionByID(w http.ResponseWriter, sid string) *Session {
	sess, ok := s.sessions.Get(sid)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such session"})
		return nil
	}
	return sess
}

func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	s.broker.serveSSE(w, r, clientID(r))
}

func (s *server) handleSessionList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"sessions": s.sessions.infos()})
}

func (s *server) handleSessionDetail(w http.ResponseWriter, r *http.Request) {
	d, ok := s.sessions.Detail(r.PathValue("sid"))
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such session"})
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (s *server) handleSessionCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name  string `json:"name"`
		Phone string `json:"phone"` // optional - if set, pairs via numeric code instead of QR
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	name := strings.TrimSpace(body.Name)
	if name == "" {
		name = "Session"
	}
	id, err := s.sessions.Create(name, strings.TrimSpace(body.Phone))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"id": id})
}

func (s *server) handleSessionDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.sessions.Delete(r.Context(), r.PathValue("sid")); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleSessionLogout(w http.ResponseWriter, r *http.Request) {
	if err := s.sessions.Logout(r.Context(), r.PathValue("sid")); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleSessionPair(w http.ResponseWriter, r *http.Request) {
	if err := s.sessions.Pair(r.PathValue("sid")); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleSessionPairCode(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Phone string `json:"phone"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	phone := strings.TrimSpace(body.Phone)
	if phone == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "phone required"})
		return
	}
	if err := s.sessions.PairWithCode(r.PathValue("sid"), phone); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleStartCall(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doStartCall(sess, w, r)
	}
}

func (s *server) handleWebRTC(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doWebRTC(sess, w, r)
	}
}

func (s *server) handleAttachAI(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doAttachAI(sess, w, r)
	}
}

func (s *server) handleAccept(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doAccept(sess, w, r)
	}
}

func (s *server) handleReject(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doReject(sess, w, r)
	}
}

func (s *server) handleEndCall(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		s.doEndCall(sess, w, r)
	}
}

func (s *server) handleHistory(w http.ResponseWriter, r *http.Request) {
	if sess := s.sessionByID(w, r.PathValue("sid")); sess != nil {
		writeJSON(w, http.StatusOK, map[string]any{"rows": s.broker.historyRows(sess.id, 50)})
	}
}

func (s *server) doStartCall(sess *Session, w http.ResponseWriter, r *http.Request) {
	if sess.client.Store.ID == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "not paired"})
		return
	}
	var body struct {
		Phone      string `json:"phone"`
		DurationMs int    `json:"duration_ms"`
		Record     bool   `json:"record"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.Phone) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "phone required"})
		return
	}
	owner := clientID(r)
	if other := s.broker.ownerActiveCall(owner); other != "" {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "operator already on a call"})
		return
	}
	if max := s.sessions.maxCalls; s.sessions.refuseNew.Load() || (max > 0 && sess.reg.count() >= max) {
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "max concurrent calls"})
		return
	}
	peer := types.NewJID(normalizePhone(body.Phone), types.DefaultUserServer)

	callID, err := sess.startOutgoing(r.Context(), peer, false)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.broker.upsertCall(CallRecord{
		SessionID: sess.id, CallID: callID, Owner: &owner, Direction: "outbound", Peer: peer.String(),
		StartedAt: time.Now().UnixMilli(), Status: StatusRinging,
	})
	writeJSON(w, http.StatusOK, map[string]any{"call": map[string]string{"callId": callID}})
}

func (s *server) doWebRTC(sess *Session, w http.ResponseWriter, r *http.Request) {
	callID := r.PathValue("id")
	ac, ok := sess.reg.get(callID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such call"})
		return
	}
	var body struct {
		SDPOffer string `json:"sdp_offer"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.SDPOffer == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "sdp_offer required"})
		return
	}
	bridge, answer, err := NewBridge(body.SDPOffer, s.log)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	bridge.OnBrowserPCM = func(pcm []float32) {
		ac.cm.FeedCapturedPCM(pcm)
	}
	bridge.OnTerminalICE = func() {
		go sess.terminateCall(callID, core.EndCallReasonUserEnded)
	}
	sess.setBridge(callID, bridge)
	writeJSON(w, http.StatusOK, map[string]string{"sdp_answer": answer})
}

// doAttachAI is the AI equivalent of doWebRTC: instead of a browser
// operator negotiating a WebRTC data channel, it connects this call
// straight to the Pipecat assistant so Emysa can talk on it. Called by
// Audio-call- right after POST /calls, carrying the app's own chat
// sessionId/contactName purely so the call's outcome can be reported back
// into that chat when it ends (see aioutcome.go) - has no bearing on the
// call itself.
func (s *server) doAttachAI(sess *Session, w http.ResponseWriter, r *http.Request) {
	callID := r.PathValue("id")
	s.log.Info("aibridge: attach requested", "call_id", callID)
	ac, ok := sess.reg.get(callID)
	if !ok {
		s.log.Warn("aibridge: attach failed, no such call in registry", "call_id", callID)
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such call"})
		return
	}
	bridgeURL := os.Getenv("ASSISTANT_BRIDGE_URL")
	bridgeSecret := os.Getenv("ASSISTANT_BRIDGE_SECRET")
	if bridgeURL == "" || bridgeSecret == "" {
		s.log.Error("aibridge: ASSISTANT_BRIDGE_URL / ASSISTANT_BRIDGE_SECRET not configured", "call_id", callID)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "ASSISTANT_BRIDGE_URL / ASSISTANT_BRIDGE_SECRET not configured"})
		return
	}
	sampleRate := core.DefaultAudioConfig.SampleRate

	var body struct {
		UserID       string `json:"userId"`
		AppSessionID string `json:"sessionId"`
		ContactName  string `json:"contactName"`
		PeerNumber   string `json:"peerNumber"`
		VoiceID      string `json:"voiceId"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	bridgeSessionID := "call-" + callID // ACAF bridge session id - unrelated to the app's own chat sessionId above
	bridge, err := NewAIBridge(bridgeURL, bridgeSecret, bridgeSessionID, sampleRate, body.VoiceID, s.log)
	if err != nil {
		s.log.Error("aibridge: could not reach assistant", "call_id", callID, "err", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "could not reach assistant: " + err.Error()})
		return
	}
	s.log.Info("aibridge: connected to assistant, waiting for call to be answered before releasing audio", "call_id", callID)

	// The handshake happens now (model/TTS warm and ready the instant the
	// call connects, no boot delay for whoever's calling) but the assistant
	// must not be heard - and pipecat's configurable ASSISTANT_GREETING can
	// fire the moment the handshake completes - until the callee has
	// actually picked up, not just while it's still ringing. released
	// gates both directions of audio until then.
	var released atomic.Bool
	var droppedEarly atomic.Uint64
	release := func(why string) {
		if !released.CompareAndSwap(false, true) {
			return
		}
		s.log.Info("aibridge: call answered, releasing assistant audio",
			"call_id", callID, "trigger", why, "assistant_frames_dropped_while_ringing", droppedEarly.Load())
		// Tell the assistant *after* the gate is open, so the first
		// syllables of its greeting are not lost to the gate.
		go bridge.SignalCallActive()
	}
	bridge.OnAssistantPCM = func(pcm []float32) {
		if !released.Load() {
			droppedEarly.Add(1)
			return
		}
		ac.cm.FeedCapturedPCM(pcm)
	}
	bridge.OnEnded = func() {
		s.log.Info("aibridge: assistant ended the call", "call_id", callID)
		go sess.terminateCall(callID, core.EndCallReasonUserEnded)
	}
	sess.setBridge(callID, bridge)
	reportInfo := &aiReportInfo{
		AppUserID:      body.UserID,
		AppSessionID:   body.AppSessionID,
		ContactName:    body.ContactName,
		PeerIdentifier: body.PeerNumber,
	}
	sess.reg.setAIReport(callID, reportInfo)

	// Tell the app when WhatsApp reports the call as ringing (outbound offer
	// sent, awaiting the callee), once. Nothing is inferred from timers: if the
	// provider never reports it, the app never shows "Ringing".
	var ringingReported atomic.Bool
	reportRinging := func() {
		if ringingReported.CompareAndSwap(false, true) {
			go reportRelayStatus(s.log, reportInfo, "ringing")
		}
	}

	// Chained, not replaced: cm.OnStateChange is already set in
	// wireCall() (session.go) for call-record bookkeeping - overwriting it
	// outright would silently break that, the exact class of bug that's
	// bitten this codebase before.
	prevOnStateChange := ac.cm.OnStateChange
	ac.cm.OnStateChange = func(c *call.CallInfo) {
		if prevOnStateChange != nil {
			prevOnStateChange(c)
		}
		if c.StateData.State == core.CallStateRinging {
			reportRinging()
		}
		if c.IsActive() {
			release("state-change")
		}
	}

	// The hook above only fires on FUTURE state changes. NewAIBridge can
	// block for tens of seconds (it retries the dial to ride out a cold
	// assistant), long enough for the callee to answer before this point.
	// In that case no further "active" transition ever arrives and the gate
	// would stay shut for the whole call: the callee answers and hears
	// nothing. Check the state we are already in, after the hook is
	// installed so an answer landing right now cannot fall between the two.
	if cur := ac.cm.CurrentCall(); cur != nil && cur.IsActive() {
		release("already-active-at-attach")
	} else if cur != nil && cur.StateData.State == core.CallStateRinging {
		reportRinging() // it began ringing while the assistant was still connecting
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "attached"})
}

func (s *server) doAccept(sess *Session, w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ac, ok := sess.reg.get(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such call"})
		return
	}
	owner := clientID(r)
	if other := s.broker.ownerActiveCall(owner); other != "" && other != id {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "operator already on a call"})
		return
	}
	if !s.broker.setOwner(id, owner) {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "claimed by another client"})
		return
	}
	s.broker.emitIncomingClaimed(sess.id, id, owner)
	if err := ac.cm.AcceptCall(r.Context(), id); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"call": map[string]string{"callId": id}})
}

func (s *server) doReject(sess *Session, w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if ac, ok := sess.reg.get(id); ok {
		_ = ac.cm.RejectCall(r.Context(), id, core.EndCallReasonDeclined)
	}
	sess.removeCall(id)
	s.broker.endCall(id, string(core.EndCallReasonDeclined))
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) doEndCall(sess *Session, w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if ac, ok := sess.reg.get(id); ok {
		_ = ac.cm.EndCall(r.Context(), core.EndCallReasonUserEnded)
	}
	sess.removeCall(id)
	s.broker.endCall(id, string(core.EndCallReasonUserEnded))
	w.WriteHeader(http.StatusNoContent)
}

func normalizePhone(p string) string {
	p = strings.TrimSpace(p)
	p = strings.TrimPrefix(p, "+")
	var b strings.Builder
	for _, c := range p {
		if c >= '0' && c <= '9' {
			b.WriteRune(c)
		}
	}
	return b.String()
}

// handleHealthz is the worker heartbeat endpoint. It sits behind withAuth like
// every other route; the manager supplies the shared secret when it probes.
func (s *server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if s.health == nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "mode": "single"})
		return
	}
	writeJSON(w, http.StatusOK, s.health())
}
