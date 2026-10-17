package main

import (
	"go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types/events"
)

// peerMuteFromNode reads the callee's own microphone state from an incoming
// call stanza. WhatsApp sends it as a single "mute_v2" child carrying a
// "mute-state" attribute ("1" muted, "0" unmuted). whatsmeow does not decode
// it, so it arrives as an UnknownCallEvent holding the raw node. This is the
// inbound mirror of signaling.BuildMuteV2Stanza.
func peerMuteFromNode(node *binary.Node) (callID string, muted bool, ok bool) {
	if node == nil {
		return "", false, false
	}
	children := node.GetChildren()
	if len(children) != 1 || children[0].Tag != "mute_v2" {
		return "", false, false
	}
	ag := children[0].AttrGetter()
	callID = ag.String("call-id")
	state := ag.String("mute-state")
	if callID == "" || (state != "0" && state != "1") {
		return "", false, false
	}
	return callID, state == "1", true
}

// onPeerMute applies a callee mute change to the matching live call. Mute
// state for a call that is not live is ignored; it cannot apply to anything.
func (s *Session) onPeerMute(evt *events.UnknownCallEvent) {
	callID, muted, ok := peerMuteFromNode(evt.Node)
	if !ok {
		return
	}
	ac, found := s.reg.get(callID)
	if !found {
		return
	}
	ac.cm.HandleCallMute(muted)
	s.log.Info("callee microphone state changed", "call_id", callID, "muted", muted)
}
