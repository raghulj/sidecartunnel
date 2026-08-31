package admin

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// bearerScheme is the authorization scheme, compared case-insensitively as RFC 6750 §2.1
// requires.
const bearerScheme = "Bearer"

// authorize wraps h with the bearer check docs/04-integration.md §4 mandates.
//
// The token is compared with crypto/subtle.ConstantTimeCompare, never with ==. The
// difference is invisible in behaviour and visible in timing: == returns on the first
// differing byte, which lets an attacker who can measure the response recover the token
// one byte at a time. The admin listener is on an internal network, which lowers the odds
// and does not change the answer — this is one function call.
//
// A rejection is logged with the method and path and never with the presented credential
// (NFR-7).
func (s *Server) authorize(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		presented, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok || subtle.ConstantTimeCompare([]byte(presented), []byte(s.token)) != 1 {
			s.log.Warn("admin.unauthorized", "method", r.Method, "path", r.URL.Path)
			w.Header().Set("WWW-Authenticate", `Bearer realm="sidecartunnel admin"`)
			s.writeJSON(w, http.StatusUnauthorized, errorBody{Error: "unauthorized"})
			return
		}
		h.ServeHTTP(w, r)
	})
}

// bearerToken extracts the credential from an Authorization header value, reporting
// whether the header carried a Bearer scheme at all.
//
// It does no trimming beyond the single separating space. A header the client got wrong
// is a header that does not authenticate, and being lenient here would mean the set of
// accepted credentials is larger than the set anyone wrote down.
func bearerToken(header string) (string, bool) {
	scheme, credential, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, bearerScheme) {
		return "", false
	}
	return credential, true
}
