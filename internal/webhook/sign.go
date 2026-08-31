package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// signedString builds the exact byte string the signature covers:
//
//	timestamp + "." + nonce + "." + sha256(Cookie header) + "." + sha256(body)
//
// Both digests are lowercase hex, because that is what the reference verifier in
// docs/04-integration.md §1.4 compares — it builds the same string from
// hashlib.sha256(...).hexdigest(). Any other encoding produces a signature no application
// can verify, which every gateway would report as a 403 and treat as a permanent refusal
// of every user.
//
// The cookie digest is the part that matters. An earlier draft signed only
// timestamp + "." + body, and since the body holds nothing but a random client id, an
// attacker who observed one signed request could replay those exact signature headers
// with their own stolen cookie and receive the victim's user id and grant list
// (docs/13-review-findings.md C5).
func signedString(timestamp, nonce, cookie string, body []byte) string {
	cookieDigest := sha256.Sum256([]byte(cookie))
	bodyDigest := sha256.Sum256(body)
	return timestamp + "." + nonce + "." +
		hex.EncodeToString(cookieDigest[:]) + "." +
		hex.EncodeToString(bodyDigest[:])
}

// sign returns the X-St-Signature value: HMAC-SHA256 of signedString under secret,
// lowercase hex. docs/04-integration.md §1.1.
//
// secret is app.webhook_secrets[0]. The gateway always signs with the first; the list
// exists so an application can accept any of several during a rotation, which is what
// lets the two sides restart at different times (docs/08-config.md §3).
//
// It never returns, logs or embeds the secret or the cookie — only the digest of the
// latter leaves this function, inside the MAC (NFR-7).
func sign(secret []byte, timestamp, nonce, cookie string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	// hash.Hash.Write is documented never to return an error, so there is nothing to
	// check here; errcheck is satisfied by the blank assignment rather than by a branch
	// no test could ever reach.
	_, _ = mac.Write([]byte(signedString(timestamp, nonce, cookie, body)))
	return hex.EncodeToString(mac.Sum(nil))
}
