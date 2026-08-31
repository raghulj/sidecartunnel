package webhook

import (
	"net"
	"net/netip"
	"strings"
)

// forwardedFor derives the X-St-Forwarded-For value from the socket peer address and the
// client's X-Forwarded-For header. FR-24, docs/13-review-findings.md M18.
//
// peer is the connection's RemoteAddr, with or without a port. xff is the raw inbound
// X-Forwarded-For, which may be empty. trusted is server.trusted_proxies, already
// compiled; it is empty by default, which trusts nothing.
//
// The rule:
//
//   - The peer is not inside trusted: the result is the socket peer address and the
//     inbound header is DISCARDED. Never merged, never appended to, never passed through.
//   - The peer is inside trusted: walk the inbound header from its RIGHTMOST entry
//     leftwards while each hop is itself inside trusted, and take the first untrusted
//     address. When every entry is trusted, fall back to the peer.
//
// Discarding is the whole point of FR-24. Forwarding a client-supplied
// X-Forwarded-For: 127.0.0.1 from the public internet would let an attacker reach an
// application's localhost trust path — an auth bypass in the application, delivered by
// the gateway, under a header prefix implying the gateway vouched for it.
//
// Walking from the right rather than the left is the second half of the same defence. A
// proxy appends what it actually observed, so the rightmost entries are the ones the
// infrastructure wrote and the leftmost are the ones a client could have invented. Taking
// the leftmost untrusted entry lets a client behind a trusted proxy prepend a fake hop
// and have the gateway forward it — the spoofing this requirement exists to prevent,
// surviving one layer in.
//
// The walk stops at the first hop that does not parse as an IP address. Such a hop cannot
// be shown to be a trusted proxy, and everything to its left is less trustworthy rather
// than more, so the peer is used instead. Nothing but an address is ever placed into a
// header the application believes.
//
// One consequence is worth stating because it is a configuration trap: trusting a CIDR
// means trusting everything inside it. If server.trusted_proxies is a whole internal
// network that also contains the clients, a client's own hop is walked off like any
// proxy's and its prepend is what remains. Configure it as the proxy addresses, not the
// network they sit in.
//
// It returns "" only when the peer address itself is unusable — a Unix socket, say — in
// which case the caller sends no address at all rather than an invented one.
//
// It performs no I/O and is safe to call concurrently: trusted is never mutated after
// New.
func forwardedFor(peer, xff string, trusted []netip.Prefix) string {
	peerAddr, ok := parseHop(peer)
	if !ok {
		return ""
	}

	// FR-24: an untrusted peer's header is discarded whole. This is the default path,
	// because server.trusted_proxies defaults to empty.
	if !within(peerAddr, trusted) {
		return peerAddr.String()
	}

	hops := strings.Split(xff, ",")
	for i := len(hops) - 1; i >= 0; i-- {
		addr, valid := parseHop(strings.TrimSpace(hops[i]))
		if !valid {
			break
		}
		if !within(addr, trusted) {
			return addr.String()
		}
	}
	return peerAddr.String()
}

// parseHop parses one address, with or without a port, in either family. It reports
// whether the text was an address at all.
//
// The zone is dropped from a link-local address: it is meaningful only on the host that
// produced it, and forwarding "fe80::1%eth0" to an application would hand it a string it
// cannot use and may not parse.
func parseHop(s string) (netip.Addr, bool) {
	if addr, err := netip.ParseAddr(s); err == nil {
		return addr.Unmap().WithZone(""), true
	}
	host, _, err := net.SplitHostPort(s)
	if err != nil {
		return netip.Addr{}, false
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}, false
	}
	return addr.Unmap().WithZone(""), true
}

// within reports whether addr falls inside any of the prefixes.
//
// The address is already unmapped by parseHop, so an IPv4-mapped IPv6 peer
// (::ffff:10.0.0.7, which is how a dual-stack listener reports an IPv4 client) is
// compared against an IPv4 CIDR as the IPv4 address it is. Without that, a correctly
// configured 10.0.0.0/8 would silently stop matching the moment the listener bound to
// "::" instead of "0.0.0.0".
func within(addr netip.Addr, prefixes []netip.Prefix) bool {
	for _, p := range prefixes {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}
