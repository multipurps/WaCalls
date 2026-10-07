package main

import (
	"testing"
	"time"

	"wacalls/internal/voip/call"
	"wacalls/internal/voip/core"
)

func TestOutcomeStatus(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name string
		sd   call.CallStateData
		want string
	}{
		{"answered call completes", call.CallStateData{ConnectedAt: &now, DurationSecs: 42}, "completed"},
		{"callee rejected", call.CallStateData{EndReason: core.EndCallReasonDeclined}, "rejected"},
		{"rang out", call.CallStateData{EndReason: core.EndCallReasonTimeout}, "no_answer"},
		{"busy", call.CallStateData{EndReason: core.EndCallReasonBusy}, "no_answer"},
		{"do not disturb is not a rejection", call.CallStateData{EndReason: core.EndCallReasonDoNotDisturb}, "no_answer"},
		{"anything else is a failure", call.CallStateData{EndReason: core.EndCallReasonUserEnded}, "failed"},
	}
	for _, c := range cases {
		got, _ := outcomeStatus(c.sd)
		if got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
	if _, d := outcomeStatus(call.CallStateData{ConnectedAt: &now, DurationSecs: 42}); d == nil || *d != 42 {
		t.Errorf("completed call must carry its duration")
	}
}
