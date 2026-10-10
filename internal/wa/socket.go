package wa

import (
	"context"
	"time"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/signaling"

	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

type Socket struct {
	cli *whatsmeow.Client
}

func NewSocket(cli *whatsmeow.Client) *Socket { return &Socket{cli: cli} }

var _ core.VoipSocket = (*Socket)(nil)

func (s *Socket) di() *whatsmeow.DangerousInternalClient { return s.cli.DangerousInternals() }

func (s *Socket) OwnPN() types.JID { return s.di().GetOwnID() }

func (s *Socket) OwnLID() types.JID { return s.di().GetOwnLID() }

func (s *Socket) AccountDeviceIdentityNode() (waBinary.Node, bool) {
	if s.cli.Store == nil || s.cli.Store.Account == nil {
		return waBinary.Node{}, false
	}
	return s.di().MakeDeviceIdentityNode(), true
}

func (s *Socket) SendNode(ctx context.Context, node waBinary.Node) error {
	return s.di().SendNode(ctx, node)
}

func (s *Socket) Query(ctx context.Context, node waBinary.Node) (*waBinary.Node, error) {
	id, _ := node.Attrs["id"].(string)
	if id == "" {
		return nil, s.di().SendNode(ctx, node)
	}
	di := s.di()
	ch := di.WaitResponse(id)
	if err := di.SendNode(ctx, node); err != nil {
		di.CancelResponse(id, ch)
		return nil, err
	}
	select {
	case resp := <-ch:
		return resp, nil
	case <-time.After(15 * time.Second):
		di.CancelResponse(id, ch)
		return nil, nil
	case <-ctx.Done():
		di.CancelResponse(id, ch)
		return nil, ctx.Err()
	}
}

func (s *Socket) GetUSyncDevices(ctx context.Context, jids []types.JID) ([]types.JID, error) {
	return s.cli.GetUserDevices(ctx, jids)
}

func (s *Socket) AssertSessions(ctx context.Context, jids []types.JID, force bool) error {
	return nil
}

func (s *Socket) CreateParticipantNodes(ctx context.Context, devices []types.JID, callKey []byte, encAttrs waBinary.Attrs) ([]waBinary.Node, bool, error) {
	plaintext, err := signaling.EncodeCallKeyMessage(callKey)
	if err != nil {
		return nil, false, err
	}
	id := s.cli.GenerateMessageID()
	return s.di().EncryptMessageForDevices(ctx, devices, id, plaintext, plaintext, encAttrs)
}

func (s *Socket) DecryptCallKey(ctx context.Context, from types.JID, encChild *waBinary.Node) ([]byte, error) {
	typ, _ := encChild.Attrs["type"].(string)
	isPreKey := typ == "pkmsg"
	plaintext, _, err := s.di().DecryptDM(ctx, encChild, from, isPreKey, time.Now())
	if err != nil {
		return nil, err
	}
	return signaling.DecodeCallKeyPlaintext(plaintext)
}

func (s *Socket) GetTCToken(ctx context.Context, jid types.JID) ([]byte, error) {
	if s.cli.Store == nil || s.cli.Store.PrivacyTokens == nil {
		return nil, nil
	}
	for _, cand := range []types.JID{s.ResolveLIDForPN(ctx, jid).ToNonAD(), jid.ToNonAD()} {
		if cand.IsEmpty() {
			continue
		}
		tok, err := s.cli.Store.PrivacyTokens.GetPrivacyToken(ctx, cand)
		if err != nil {
			return nil, err
		}
		if tok != nil && len(tok.Token) > 0 {
			return tok.Token, nil
		}
	}
	return nil, nil
}

func (s *Socket) ResolveLIDForPN(ctx context.Context, pn types.JID) types.JID {
	if pn.Server == types.HiddenUserServer {
		return pn
	}
	lookup := func() types.JID {
		if s.cli.Store != nil && s.cli.Store.LIDs != nil {
			if lid, err := s.cli.Store.LIDs.GetLIDForPN(ctx, pn); err == nil && !lid.IsEmpty() {
				return lid
			}
		}
		return types.EmptyJID
	}
	if lid := lookup(); !lid.IsEmpty() {
		return lid
	}
	// Offers addressed to a bare phone-number JID get an empty ack from WhatsApp (the call
	// never rings); only LID-addressed offers are accepted. A number we have no stored LID
	// for must be resolved against the server first. Both queries below record the mapping in
	// the store as a side effect, so the lookup is retried after each.
	if info, err := s.cli.GetUserInfo(ctx, []types.JID{pn}); err != nil {
		s.cli.Log.Warnf("LID resolve: GetUserInfo(%s) failed: %v", pn, err)
	} else if lid := info[pn].LID; !lid.IsEmpty() {
		return lid
	}
	if lid := lookup(); !lid.IsEmpty() {
		return lid
	}
	if resp, err := s.cli.IsOnWhatsApp(ctx, []string{"+" + pn.User}); err != nil {
		s.cli.Log.Warnf("LID resolve: IsOnWhatsApp(%s) failed: %v", pn, err)
	} else {
		for _, r := range resp {
			if !r.IsIn {
				s.cli.Log.Warnf("LID resolve: %s is not on WhatsApp", pn)
			}
		}
	}
	if lid := lookup(); !lid.IsEmpty() {
		return lid
	}
	s.cli.Log.Warnf("LID resolve: no LID found for %s; the offer will use the phone-number address and is likely to be ignored", pn)
	return pn
}
