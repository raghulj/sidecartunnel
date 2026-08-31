package webhook

import (
	"strings"
	"testing"
)

// The hand-computed vector. Produced independently of this package, with the Python in
// docs/04-integration.md §1.4 — the reference implementation an application actually
// runs — so the test fails if this package and that document ever disagree:
//
//	secret = b"0123456789abcdef0123456789abcdef"
//	signed = f"{ts}.{nonce}.{sha256(cookie)}.{sha256(body)}"
//	hmac.new(secret, signed.encode(), hashlib.sha256).hexdigest()
const (
	vecSecret = "0123456789abcdef0123456789abcdef"
	vecTS     = "1756612800"
	vecNonce  = "01J8XYZABCDEFGHJKMNPQRSTVW"
	vecCookie = "sessionid=abc123; csrftoken=zzz"
	vecBody   = `{"client":"8f2c1e04a7b3d915"}`

	vecCookieDigest = "66f860cb7df9aa365bf7951cb3c6fc2c0b67c606724dba3e450c40e1a69e9e06"
	vecBodyDigest   = "17abf04e7dfda97c3c03ddb803d48a7d214017c14c5a4464b08302d77b64a9c8"
	vecSignature    = "bfcc5a631a54e172a2f41b851ab38930fe7edc8eb55f0cc052b16fe20a88aaf3"

	// One byte of the cookie changed: sessionid=abc123 becomes abc124. Everything else
	// — secret, timestamp, nonce, body — is identical to the vector above.
	vecOtherCookie    = "sessionid=abc124; csrftoken=zzz"
	vecOtherSignature = "612a92d2624dd8bfa65b303450876f65e4cd0f22259c79a7c9bb3234a149faa9"
)

// TestSignedString_ShapeMatchesTheDocument pins the four dot-separated fields, in order.
// docs/04-integration.md §1.1.
func TestSignedString_ShapeMatchesTheDocument(t *testing.T) {
	got := signedString(vecTS, vecNonce, vecCookie, []byte(vecBody))
	want := vecTS + "." + vecNonce + "." + vecCookieDigest + "." + vecBodyDigest
	if got != want {
		t.Errorf("signedString =\n %q\nwant\n %q", got, want)
	}
}

// TestSign_HandComputedVector checks the whole signature against a value computed outside
// Go. A signature this package agrees with only itself is a signature no application can
// verify (docs/04-integration.md §1.1).
func TestSign_HandComputedVector(t *testing.T) {
	got := sign([]byte(vecSecret), vecTS, vecNonce, vecCookie, []byte(vecBody))
	if got != vecSignature {
		t.Errorf("sign = %q, want %q", got, vecSignature)
	}
	if strings.ToLower(got) != got {
		t.Errorf("sign = %q, want lowercase hex: the reference verifier compares against hexdigest()", got)
	}
}

// TestSign_CookieIsLoadBearing_C5 is the regression for
// docs/13-review-findings.md C5. Changing ONLY the cookie must change the signature.
//
// The earlier design signed timestamp + "." + body, and since the body holds nothing but
// a random client id, anyone who observed one signed request could replay those exact
// headers with their own stolen cookie and receive the victim's user id and grant list.
// If this test ever passes with the two signatures equal, the signature is defending
// nothing it claims to.
func TestSign_CookieIsLoadBearing_C5(t *testing.T) {
	withCookie := sign([]byte(vecSecret), vecTS, vecNonce, vecCookie, []byte(vecBody))
	withOther := sign([]byte(vecSecret), vecTS, vecNonce, vecOtherCookie, []byte(vecBody))

	if withCookie == withOther {
		t.Fatal("the signature is identical for two different cookies: the cookie digest is not covered (C5)")
	}
	if withOther != vecOtherSignature {
		t.Errorf("sign with the swapped cookie = %q, want %q", withOther, vecOtherSignature)
	}
}

// TestSign_EachInputChangesTheSignature covers the remaining three fields, so that a
// future refactor cannot quietly drop one from the signed string.
func TestSign_EachInputChangesTheSignature(t *testing.T) {
	base := sign([]byte(vecSecret), vecTS, vecNonce, vecCookie, []byte(vecBody))

	tests := []struct {
		name   string
		secret string
		ts     string
		nonce  string
		cookie string
		body   string
	}{
		{"secret", "fedcba9876543210fedcba9876543210", vecTS, vecNonce, vecCookie, vecBody},
		{"timestamp", vecSecret, "1756612801", vecNonce, vecCookie, vecBody},
		{"nonce", vecSecret, vecTS, "01J8XYZABCDEFGHJKMNPQRSTVX", vecCookie, vecBody},
		{"cookie", vecSecret, vecTS, vecNonce, vecOtherCookie, vecBody},
		{"body", vecSecret, vecTS, vecNonce, vecCookie, `{"client":"0000000000000000"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sign([]byte(tt.secret), tt.ts, tt.nonce, tt.cookie, []byte(tt.body))
			if got == base {
				t.Errorf("changing %s left the signature unchanged: it is not covered", tt.name)
			}
		})
	}
}

// TestSign_EmptyCookieStillSigns: a non-browser client may send no Cookie header at all.
// The digest of the empty string is still a digest, and the request must still be signed
// — an unsigned request is rejected by a correct application, which would turn "no
// cookie" into a transient failure instead of the 401 it is.
func TestSign_EmptyCookieStillSigns(t *testing.T) {
	got := sign([]byte(vecSecret), vecTS, vecNonce, "", []byte(vecBody))
	if len(got) != 64 {
		t.Errorf("sign with an empty cookie = %q, want 64 hex characters", got)
	}
	if got == sign([]byte(vecSecret), vecTS, vecNonce, vecCookie, []byte(vecBody)) {
		t.Error("an empty cookie signs the same as a present one")
	}
}
