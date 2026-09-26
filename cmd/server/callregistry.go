package main

import (
	"sync"

	"wacalls/internal/voip/call"
)

// callAudioSink is whatever the far leg of a call's audio is - a human
// operator via the browser (Bridge) or the AI assistant (AIBridge).
// CallManager never sees this type; only cmd/server routes audio through
// it, via the two methods both concrete types already had before this
// existed as an interface.
type callAudioSink interface {
	WritePCM(pcm []float32) error
	Close()
}

type activeCall struct {
	cm     *call.CallManager
	bridge callAudioSink
	// Set only when an AIBridge is attached - lets wireCall's OnStateChange
	// report the call's outcome back to the Audio-call- app when it ends
	// (see reportRelayOutcome in aioutcome.go). nil for ordinary
	// browser-operated calls, which have nothing to report back to.
	aiReport *aiReportInfo
}

type callRegistry struct {
	mu    sync.Mutex
	calls map[string]*activeCall
}

func newCallRegistry() *callRegistry {
	return &callRegistry{calls: map[string]*activeCall{}}
}

func (r *callRegistry) add(callID string, ac *activeCall) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls[callID] = ac
}

func (r *callRegistry) get(callID string) (*activeCall, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ac, ok := r.calls[callID]
	return ac, ok
}

func (r *callRegistry) remove(callID string) (*activeCall, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ac, ok := r.calls[callID]
	if !ok {
		return nil, false
	}
	delete(r.calls, callID)
	return ac, true
}

func (r *callRegistry) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *callRegistry) setBridge(callID string, b callAudioSink) (callAudioSink, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ac, ok := r.calls[callID]
	if !ok {
		return nil, false
	}
	oldB := ac.bridge
	ac.bridge = b
	return oldB, true
}

func (r *callRegistry) setAIReport(callID string, info *aiReportInfo) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	ac, ok := r.calls[callID]
	if !ok {
		return false
	}
	ac.aiReport = info
	return true
}

func (r *callRegistry) drain() []*activeCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*activeCall, 0, len(r.calls))
	for _, ac := range r.calls {
		out = append(out, ac)
	}
	r.calls = map[string]*activeCall{}
	return out
}
