package main

import (
	"testing"

	"go.mau.fi/whatsmeow/binary"
)

func muteNode(callID, state string) *binary.Node {
	return &binary.Node{
		Tag: "call",
		Content: []binary.Node{{
			Tag:   "mute_v2",
			Attrs: binary.Attrs{"call-id": callID, "mute-state": state},
		}},
	}
}

func TestPeerMuteFromNode(t *testing.T) {
	id, muted, ok := peerMuteFromNode(muteNode("abc", "1"))
	if !ok || id != "abc" || !muted {
		t.Fatalf("muted=1: got id=%q muted=%v ok=%v", id, muted, ok)
	}
	_, muted, ok = peerMuteFromNode(muteNode("abc", "0"))
	if !ok || muted {
		t.Fatalf("muted=0: got muted=%v ok=%v", muted, ok)
	}
}

func TestPeerMuteRejectsOtherStanzas(t *testing.T) {
	cases := map[string]*binary.Node{
		"nil":          nil,
		"no mute node": {Tag: "call", Content: []binary.Node{{Tag: "relaylatency"}}},
		"two children": {Tag: "call", Content: []binary.Node{{Tag: "mute_v2"}, {Tag: "mute_v2"}}},
		"no call id":   muteNode("", "1"),
		"bad state":    muteNode("abc", "maybe"),
	}
	for name, node := range cases {
		if _, _, ok := peerMuteFromNode(node); ok {
			t.Errorf("%s: expected rejection", name)
		}
	}
}
