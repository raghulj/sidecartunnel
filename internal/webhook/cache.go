package webhook

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"sync"
	"time"
)

// cache is the optional answer cache behind app.cache_ttl. docs/13-review-findings.md C4.
//
// It is off unless cache_ttl is positive, and off is the right default. It is worth less
// than it sounds — N reconnecting users are N distinct cookies and therefore N cache
// misses — and it costs revocation latency, because a cached entry otherwise survives a
// revocation and a suspended user reconnecting within the TTL gets their pre-revocation
// grants back. That cost is bounded by two rules kept here: entries live at most
// cache_ttl, and any control-channel disconnect flushes the whole cache (Client.Flush).
//
// It stores no cookie. The key is a digest, the value is an Authorized, and neither can
// be turned back into a session (FR-22, docs/13-review-findings.md C3).
//
// All methods are safe for concurrent use.
type cache struct {
	// ttl is app.cache_ttl. Always positive: a non-positive TTL means no cache at all,
	// and newCache is not called.
	ttl time.Duration

	// names is app.cookie_names, in configured order. Empty means the whole Cookie
	// header forms the key (docs/08-config.md §3).
	names []string

	mu sync.Mutex
	// entries is guarded by mu. It is swept whenever a put arrives more than ttl after
	// the last sweep, which bounds it by the connection rate over one TTL rather than by
	// the process's uptime; without that, an entry nobody looks up again is never
	// removed and the cache is a slow leak in a process holding 25,000 connections.
	entries   map[string]cacheEntry
	lastSweep time.Time
}

// cacheEntry is one stored answer and the instant it stops being usable.
type cacheEntry struct {
	value   Authorized
	expires time.Time
}

// newCache builds a cache for a positive ttl. The caller does not construct one at all
// when app.cache_ttl is zero, so there is no "disabled cache" state to get wrong.
func newCache(ttl time.Duration, names []string) *cache {
	return &cache{ttl: ttl, names: names, entries: make(map[string]cacheEntry)}
}

// key returns the cache key for a Cookie header, or "" when the request must not be
// cached at all.
//
// With app.cookie_names configured, only those cookies form the key. Hashing the whole
// header is why the cache did not deduplicate tabs: _ga, _fbp and CSRF tokens differ per
// tab, so two tabs of one user missed each other and the claimed benefit largely did not
// exist (docs/13-review-findings.md C4).
//
// It returns "" — uncacheable — when the header is empty, when it does not parse, or when
// none of the named cookies is present. That last case is the important one: if every
// request lacking the named cookie shared one key, one user's answer would be served to
// every other such user. Missing the cache costs a webhook call; getting this wrong hands
// out someone else's grants.
//
// Parsing here does not contradict FR-3. The header is still forwarded byte for byte; it
// is only the *key* that is derived, and no session is parsed, validated or decrypted.
func (c *cache) key(cookie string) string {
	if cookie == "" {
		return ""
	}

	if len(c.names) == 0 {
		return digest(cookie)
	}

	parsed, err := http.ParseCookie(cookie)
	if err != nil {
		// Uncacheable rather than falling back to the whole header: a header this
		// gateway cannot parse is one whose session cookie it cannot identify, and a key
		// built from the rest would not deduplicate anything anyway.
		return ""
	}

	// Built in configured order, so header order cannot change the key. Cookie names
	// contain neither "=" nor "\n" (net/http rejects both), so the separators make the
	// encoding unambiguous.
	var b strings.Builder
	found := false
	for _, name := range c.names {
		for _, ck := range parsed {
			if ck.Name == name {
				b.WriteString(name)
				b.WriteString("=")
				b.WriteString(ck.Value)
				b.WriteString("\n")
				found = true
				break
			}
		}
	}
	if !found {
		return ""
	}
	return digest(b.String())
}

// digest is the one-way function standing between the cache and the session cookies it is
// keyed on. A key is never reversible into a cookie, which is what keeps a memory dump of
// this process from yielding live sessions (docs/05-authorization.md §8).
func digest(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// get returns a stored answer that has not expired. An entry stored at t is valid until
// t+ttl exclusive.
func (c *cache) get(key string, now time.Time) (Authorized, bool) {
	if key == "" {
		return Authorized{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.entries[key]
	if !ok {
		return Authorized{}, false
	}
	if !now.Before(e.expires) {
		delete(c.entries, key)
		return Authorized{}, false
	}
	return e.value, true
}

// put stores an answer. An empty key is uncacheable and stores nothing.
func (c *cache) put(key string, value Authorized, now time.Time) {
	if key == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if now.Sub(c.lastSweep) >= c.ttl {
		for k, e := range c.entries {
			if !now.Before(e.expires) {
				delete(c.entries, k)
			}
		}
		c.lastSweep = now
	}
	c.entries[key] = cacheEntry{value: value, expires: now.Add(c.ttl)}
}

// flush discards every entry. A control-channel disconnect calls it, because a cached
// entry otherwise survives a revocation: coarse and correct, and revocations are rare
// (docs/13-review-findings.md C4).
func (c *cache) flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]cacheEntry)
}

// size is the number of stored entries, expired or not. It exists for tests and for a
// future gauge; nothing in the request path reads it.
func (c *cache) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}
