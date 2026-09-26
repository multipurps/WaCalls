package main

import (
	"encoding/binary"
	"errors"
	"time"
)

// Frame types and encoding, matching multipurps/Audio-call-'s
// pipecat-service/app/protocol.py and mp-relay's bin/server.php.
const (
	acafAudioIn           byte = 1
	acafAudioOut          byte = 2
	acafPartialTranscript byte = 3
	acafInterrupt         byte = 4
	acafHeartbeat         byte = 5

	acafEncodingPCMS16LE byte = 1

	acafHeaderLen = 28 // magic(4) version(1) type(1) encoding(1) channels(1) rate(4) seq(4) ts(8) len(4)
)

type acafFrame struct {
	Type        byte
	Encoding    byte
	Channels    byte
	SampleRate  uint32
	Sequence    uint32
	TimestampMs uint64
	Payload     []byte
}

// acafPack builds a mono, PCM16LE ACAF frame - the only shape this bridge
// ever sends (WaCalls' CallManager already only speaks 16 kHz mono).
func acafPack(frameType byte, sampleRate int, sequence uint32, payload []byte) []byte {
	buf := make([]byte, acafHeaderLen+len(payload))
	copy(buf[0:4], "ACAF")
	buf[4] = 1 // version
	buf[5] = frameType
	buf[6] = acafEncodingPCMS16LE
	buf[7] = 1 // channels
	binary.LittleEndian.PutUint32(buf[8:12], uint32(sampleRate))
	binary.LittleEndian.PutUint32(buf[12:16], sequence)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(time.Now().UnixMilli()))
	binary.LittleEndian.PutUint32(buf[24:28], uint32(len(payload)))
	copy(buf[acafHeaderLen:], payload)
	return buf
}

func acafUnpack(data []byte) (*acafFrame, error) {
	if len(data) < acafHeaderLen || string(data[0:4]) != "ACAF" {
		return nil, errors.New("acaf: short or bad-magic frame")
	}
	f := &acafFrame{
		Type:        data[5],
		Encoding:    data[6],
		Channels:    data[7],
		SampleRate:  binary.LittleEndian.Uint32(data[8:12]),
		Sequence:    binary.LittleEndian.Uint32(data[12:16]),
		TimestampMs: binary.LittleEndian.Uint64(data[16:24]),
	}
	plen := binary.LittleEndian.Uint32(data[24:28])
	if len(data) < acafHeaderLen+int(plen) {
		return nil, errors.New("acaf: truncated payload")
	}
	f.Payload = data[acafHeaderLen : acafHeaderLen+int(plen)]
	return f, nil
}
