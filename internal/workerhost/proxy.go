package workerhost

import (
	"encoding/json"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
)

// Forward proxies r to the worker that owns session sid. It reports whether
// the supervisor knows that worker at all; when it does not, nothing has been
// written and the caller decides (404 vs "parked").
//
// A worker that exists but is not Running (starting, backing off, failed) is
// answered with 503 + Retry-After. 503 is deliberate: the Emysa app treats a
// 404 from the relay as "session gone" and clears the user's saved session id,
// while any 5xx is treated as "relay unreachable, keep what we saved".
func (s *Supervisor) Forward(w http.ResponseWriter, r *http.Request, sid string) bool {
	st, ok := s.Get(sid)
	if !ok {
		return false
	}
	if st.State != StateRunning {
		retry := 5
		if !st.NextStartAt.IsZero() {
			if d := int(secondsUntil(st.NextStartAt)); d > retry {
				retry = d
			}
		}
		w.Header().Set("Retry-After", strconv.Itoa(retry))
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "worker not available", "state": st.State, "retryAfterSec": retry,
		})
		return true
	}
	target := &url.URL{Scheme: "http", Host: "127.0.0.1:" + strconv.Itoa(st.Port)}
	rp := httputil.NewSingleHostReverseProxy(target)
	rp.FlushInterval = -1 // SSE (/api/events) must not be buffered
	rp.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": "worker unreachable", "state": st.State})
	}
	rp.ServeHTTP(w, r)
	return true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
