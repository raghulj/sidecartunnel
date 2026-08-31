package webhook

import (
	"net/netip"
	"testing"
)

// mustPrefixes compiles CIDRs for a test table. A bad CIDR here is a bug in the test.
func mustPrefixes(t *testing.T, cidrs ...string) []netip.Prefix {
	t.Helper()
	out := make([]netip.Prefix, 0, len(cidrs))
	for _, c := range cidrs {
		p, err := netip.ParsePrefix(c)
		if err != nil {
			t.Fatalf("ParsePrefix(%q): %v", c, err)
		}
		out = append(out, p)
	}
	return out
}

// TestForwardedFor_FR24 is the FR-24 / docs/13-review-findings.md M18 table.
//
// Two rows matter. "untrusted peer spoofs a loopback hop": passing that header through
// would let an attacker send X-Forwarded-For: 127.0.0.1 and reach an application's
// localhost trust path from the public internet — an auth bypass in the application,
// delivered by the gateway, under a header prefix implying the gateway vouched for it.
// And "trusted peer, spoofed prepend": the walk starts at the rightmost entry, so a
// client that prepends a fake hop has it ignored rather than forwarded, which is the
// same spoofing surviving one layer in.
func TestForwardedFor_FR24(t *testing.T) {
	trusted := mustPrefixes(t, "10.0.0.0/8", "192.168.1.1/32", "2001:db8::/32")

	tests := []struct {
		name    string
		peer    string
		xff     string
		trusted []netip.Prefix
		want    string
	}{
		{
			name: "untrusted peer, no header: the socket peer",
			peer: "203.0.113.9:51234",
			want: "203.0.113.9",
		},
		{
			name: "untrusted peer spoofs a loopback hop: discarded (FR-24)",
			peer: "203.0.113.9:51234",
			xff:  "127.0.0.1",
			want: "203.0.113.9",
		},
		{
			name:    "untrusted peer spoofs a chain: discarded whole",
			peer:    "203.0.113.9:51234",
			xff:     "127.0.0.1, 10.0.0.1, 8.8.8.8",
			trusted: trusted,
			want:    "203.0.113.9",
		},
		{
			name:    "trusted peer, single hop: the rightmost untrusted hop",
			peer:    "10.0.0.7:4000",
			xff:     "203.0.113.9",
			trusted: trusted,
			want:    "203.0.113.9",
		},
		{
			name:    "trusted peer, chain: trusted hops are walked off the right",
			peer:    "10.0.0.7:4000",
			xff:     "203.0.113.9, 10.0.0.3, 10.0.0.7",
			trusted: trusted,
			want:    "203.0.113.9",
		},
		{
			// The walk stops at the first untrusted hop from the right, so the trusted
			// hop to its LEFT is never reached. Taking the leftmost untrusted entry
			// instead would answer 10.0.0.2 here.
			name:    "trusted peer, the walk stops at the first untrusted hop from the right",
			peer:    "10.0.0.7:4000",
			xff:     "10.0.0.2, 203.0.113.9, 10.0.0.7",
			trusted: trusted,
			want:    "203.0.113.9",
		},
		{
			// FR-24's spoofing case. The client at 10.0.0.5 prepends a fake hop; the
			// proxy at 10.0.0.7 appends what it actually saw. Walking from the right
			// answers with the observed address and the prepend is never reached.
			// Compare the row below: it is the same request against a trusted set wide
			// enough to contain the client, and it answers differently.
			name:    "trusted peer, spoofed prepend is ignored",
			peer:    "10.0.0.7:4000",
			xff:     "1.2.3.4, 10.0.0.5",
			trusted: mustPrefixes(t, "10.0.0.7/32"),
			want:    "10.0.0.5",
		},
		{
			// The documented consequence of the FR-24 walk, pinned so nobody has to
			// rediscover it: trusting a CIDR means trusting everything inside it, so a
			// client that is itself inside trusted_proxies has its hop walked off like
			// any other proxy and its prepend is what remains. Configure
			// server.trusted_proxies as the proxy addresses, not the whole internal
			// network — a /8 that contains the clients does not authenticate anything.
			name:    "trusted peer, a client inside trusted_proxies is walked off too",
			peer:    "10.0.0.7:4000",
			xff:     "1.2.3.4, 10.0.0.5",
			trusted: trusted,
			want:    "1.2.3.4",
		},
		{
			name:    "trusted peer, every hop trusted: fall back to the peer",
			peer:    "10.0.0.7:4000",
			xff:     "10.0.0.2, 10.0.0.3",
			trusted: trusted,
			want:    "10.0.0.7",
		},
		{
			name:    "trusted peer, empty header: fall back to the peer",
			peer:    "10.0.0.7:4000",
			xff:     "   ",
			trusted: trusted,
			want:    "10.0.0.7",
		},
		{
			name:    "trusted peer, unparseable hop to the left is never reached",
			peer:    "10.0.0.7:4000",
			xff:     "not-an-address, 203.0.113.9",
			trusted: trusted,
			want:    "203.0.113.9",
		},
		{
			// A hop that does not parse cannot be shown to be a trusted proxy, so the
			// walk stops there rather than attributing the connection to whatever text
			// sits to its left. Everything left of an unverifiable hop is less
			// trustworthy, not more.
			name:    "trusted peer, unparseable rightmost hop stops the walk",
			peer:    "10.0.0.7:4000",
			xff:     "203.0.113.9, not-an-address",
			trusted: trusted,
			want:    "10.0.0.7",
		},
		{
			name:    "trusted peer, only unparseable hops: fall back to the peer",
			peer:    "10.0.0.7:4000",
			xff:     "not-an-address, <script>",
			trusted: trusted,
			want:    "10.0.0.7",
		},
		{
			// A hostname with a port splits cleanly but is still not an address, and a
			// hostname in X-St-Forwarded-For is exactly the kind of text FR-24 exists to
			// keep out of a header the application believes. As the rightmost hop it
			// stops the walk; the peer is used instead.
			name:    "trusted peer, rightmost host:port that is not an address stops the walk",
			peer:    "10.0.0.7:4000",
			xff:     "203.0.113.9, evil.example:80",
			trusted: trusted,
			want:    "10.0.0.7",
		},
		{
			name:    "trusted peer, host:port to the left is never reached",
			peer:    "10.0.0.7:4000",
			xff:     "evil.example:80, 203.0.113.9",
			trusted: trusted,
			want:    "203.0.113.9",
		},
		{
			name:    "trusted peer, hop carrying a port",
			peer:    "10.0.0.7:4000",
			xff:     "203.0.113.9:44321",
			trusted: trusted,
			want:    "203.0.113.9",
		},
		{
			name:    "trusted peer, bracketed IPv6 hop with a port",
			peer:    "10.0.0.7:4000",
			xff:     "[2001:db9::1]:44321",
			trusted: trusted,
			want:    "2001:db9::1",
		},
		{
			name:    "trusted IPv6 peer, IPv6 chain",
			peer:    "[2001:db8::5]:4000",
			xff:     "2001:db9::1, 2001:db8::6",
			trusted: trusted,
			want:    "2001:db9::1",
		},
		{
			name:    "trusted peer, hop carrying a port to the right",
			peer:    "10.0.0.7:4000",
			xff:     "198.51.100.4, 203.0.113.9:44321",
			trusted: trusted,
			want:    "203.0.113.9",
		},
		{
			name:    "exact /32 trusted peer",
			peer:    "192.168.1.1:9000",
			xff:     "203.0.113.9",
			trusted: trusted,
			want:    "203.0.113.9",
		},
		{
			name:    "peer just outside the trusted /32",
			peer:    "192.168.1.2:9000",
			xff:     "203.0.113.9",
			trusted: trusted,
			want:    "192.168.1.2",
		},
		{
			name: "peer with no port at all",
			peer: "203.0.113.9",
			want: "203.0.113.9",
		},
		{
			name:    "peer that is not an address: nothing is forwarded",
			peer:    "@/tmp/socket",
			xff:     "127.0.0.1",
			trusted: trusted,
			want:    "",
		},
		{
			name: "no peer at all: nothing is forwarded",
			peer: "",
			xff:  "127.0.0.1",
			want: "",
		},
		{
			name:    "IPv4-mapped IPv6 peer is compared as IPv4",
			peer:    "[::ffff:10.0.0.7]:4000",
			xff:     "203.0.113.9",
			trusted: trusted,
			want:    "203.0.113.9",
		},
		{
			name:    "zoned peer address loses its zone",
			peer:    "[fe80::1%eth0]:4000",
			xff:     "203.0.113.9",
			trusted: trusted,
			want:    "fe80::1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := forwardedFor(tt.peer, tt.xff, tt.trusted)
			if got != tt.want {
				t.Errorf("forwardedFor(%q, %q) = %q, want %q", tt.peer, tt.xff, got, tt.want)
			}
		})
	}
}
