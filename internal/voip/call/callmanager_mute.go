package call

// HandleCallMute records the callee's microphone state. Muted callee audio is
// dropped in onRelayData before it reaches the assistant, and the state is
// reported through OnPeerMute so the assistant can also stop listening.
func (m *CallManager) HandleCallMute(muted bool) {
	m.mu.Lock()
	m.peerMuted = muted
	cb := m.OnPeerMute
	call := m.currentCall
	m.mu.Unlock()

	if call != nil {
		_ = call.ApplyTransition(Transition{Type: TransitionAudioMuteChanged, Muted: muted})
	}
	if cb != nil {
		cb(muted)
	}
}

// PeerMuted reports whether the callee is currently muted.
func (m *CallManager) PeerMuted() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.peerMuted
}
