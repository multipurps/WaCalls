package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync/atomic"
	"time"

	"wacalls/internal/voip/media"

	"github.com/coder/websocket"
)

const websocketDialTimeout = 10 * time.Second

// AIBridge is the AI-leg adapter: it carries raw 16 kHz mono PCM between the
// CallManager and the Pipecat assistant service over a plain WebSocket
// (ACAF v1 - see multipurps/Audio-call-'s docs/ACAF-PROTOCOL.md), the same
// protocol mp-relay speaks for Telegram. It satisfies the same callAudioSink
// interface as the browser's Bridge (WritePCM/Close), so cmd/server can
// attach either one to a call without CallManager ever knowing which.
//
// This side has it easier than mp-relay's: WaCalls already decodes/encodes
// Opus internally and hands CallManager raw []float32 PCM at 16 kHz mono,
// which is exactly what ACAF carries - no OGG Opus container, no ffmpeg, no
// realtime-conversion assumptions to get wrong.
//
// UNVERIFIED: built and syntax-checked (go vet/build), but never run against
// a live WhatsApp call or the assistant service - I have neither here.
type AIBridge struct {
	conn       *websocket.Conn
	log        *slog.Logger
	cancel     context.CancelFunc
	closed     atomic.Bool
	outSeq     atomic.Uint32
	sampleRate int

	// OnAssistantPCM is invoked with 16 kHz mono PCM the assistant wants
	// played into the call (its speech) - wire this to
	// ac.cm.FeedCapturedPCM, exactly like Bridge.OnBrowserPCM.
	OnAssistantPCM func(pcm []float32)
	// OnEnded fires if the assistant side closes the connection, or sends a
	// hangup/stopped control message. The caller should end the WhatsApp
	// call itself when this fires - this bridge only carries audio, it
	// never touches call state.
	OnEnded func()
}

// NewAIBridge dials the assistant's ACAF endpoint and starts reading frames.
// sessionID here is the ACAF bridge session id (an internal identifier for
// the audio stream), not the Audio-call- app's own chat session id.
func NewAIBridge(bridgeURL, bridgeSecret, sessionID string, sampleRate int, log *slog.Logger) (*AIBridge, error) {
	dialCtx, dialCancel := context.WithTimeout(context.Background(), websocketDialTimeout)
	defer dialCancel()
	conn, _, err := websocket.Dial(dialCtx, bridgeURL, &websocket.DialOptions{
		HTTPHeader: map[string][]string{"X-Assistant-Session": {sessionID}},
	})
	if err != nil {
		return nil, err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	b := &AIBridge{conn: conn, log: log, cancel: cancel, sampleRate: sampleRate}

	hello, err := json.Marshal(map[string]any{
		"type": "hello", "sessionId": sessionID, "platform": "whatsapp",
		"sampleRate": sampleRate, "channels": 1, "encoding": "pcm_s16le",
		"secret": bridgeSecret,
	})
	if err != nil {
		cancel()
		_ = conn.Close(websocket.StatusInternalError, "hello marshal failed")
		return nil, err
	}
	if err := conn.Write(runCtx, websocket.MessageText, hello); err != nil {
		cancel()
		_ = conn.Close(websocket.StatusInternalError, "hello failed")
		return nil, err
	}

	go b.readLoop(runCtx)
	return b, nil
}

func (b *AIBridge) readLoop(ctx context.Context) {
	for {
		msgType, data, err := b.conn.Read(ctx)
		if err != nil {
			if !b.closed.Load() {
				b.log.Debug("aibridge: read loop ended", "err", err)
				if b.OnEnded != nil {
					b.OnEnded()
				}
			}
			return
		}
		if msgType == websocket.MessageText {
			var msg struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(data, &msg) == nil && (msg.Type == "hangup" || msg.Type == "stopped") {
				if b.OnEnded != nil {
					b.OnEnded()
				}
				return
			}
			continue // 'ready' and other control messages need no action here
		}
		frame, err := acafUnpack(data)
		if err != nil {
			b.log.Warn("aibridge: dropping bad ACAF frame", "err", err)
			continue
		}
		if frame.Type == acafAudioOut {
			if cb := b.OnAssistantPCM; cb != nil {
				cb(media.PCMInt16LEToFloat32(frame.Payload))
			}
		}
		// PARTIAL_TRANSCRIPT and HEARTBEAT need no action beyond being read.
	}
}

// WritePCM sends the WhatsApp peer's captured audio to the assistant as an
// ACAF AUDIO_IN frame. Satisfies callAudioSink, same as Bridge.WritePCM.
func (b *AIBridge) WritePCM(pcm []float32) error {
	if b.closed.Load() || len(pcm) == 0 {
		return nil
	}
	frame := acafPack(acafAudioIn, b.sampleRate, b.outSeq.Add(1), media.PCMFloat32ToInt16LE(pcm))
	return b.conn.Write(context.Background(), websocket.MessageBinary, frame)
}

func (b *AIBridge) Close() {
	if b.closed.CompareAndSwap(false, true) {
		b.cancel()
		_ = b.conn.Close(websocket.StatusNormalClosure, "call ended")
	}
}
