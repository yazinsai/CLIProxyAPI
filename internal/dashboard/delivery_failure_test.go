package dashboard

import (
	"context"
	"testing"
)

func TestDeliveryFailuresKeepTheirReason(t *testing.T) {
	for _, tc := range []struct {
		name        string
		status      int
		cancelled   bool
		writeFailed bool
		want        string
		reason      string
	}{
		{name: "write failure", status: 200, writeFailed: true, want: "interrupted", reason: "stream_interrupted"},
		{name: "HTTP rejection followed by disconnect", status: 429, cancelled: true, want: "failed", reason: "rate_limited"},
		{name: "HTTP error write failure", status: 502, writeFailed: true, want: "failed", reason: "upstream_error"},
		{name: "early disconnect", status: 200, cancelled: true, want: "interrupted", reason: "cancelled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracker := NewTracker(nil)
			ObserveModel(WithTracker(context.Background(), tracker), "model", true)
			row := tracker.Complete(Request{StatusCode: tc.status}, tc.cancelled, tc.writeFailed)
			if row.Outcome != tc.want || row.Reason != tc.reason {
				t.Fatalf("outcome=%s/%s, want %s/%s", row.Outcome, row.Reason, tc.want, tc.reason)
			}
		})
	}
}
