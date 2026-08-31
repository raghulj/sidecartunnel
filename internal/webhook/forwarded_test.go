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
// The row that matters is "untrusted peer spoofs a loopback hop": passing that header
// through would let an attacker send X-Forwarded-For: 127.0.0.1 and reach an
// application's localhost trust path from the public internet — an auth bypass in the
// application, delivered by the gateway, under a header prefix implying the gateway
// vouched for it.
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
			name:    "trusted peer, single hop: the leftmost untrusted hop",
			peer:    "10.0.0.7:4000",
			xff:     "203.0.113.9",
			trusted: trusted,
			want:    "203.0.113.9",
		},
		{
			name:    "trusted peer, chain: the leftmost untrusted hop",
			peer:    "10.0.0.7:4000",
			xff:     "203.0.113.9, 10.0.0.3, 10.0.0.7",
			trusted: trusted,
			want:    "203.0.113.9",
		},
		{
			name:    "trusted peer, leading trusted hops are skipped",
			peer:    "10.0.0.7:4000",
			xff:     "10.0.0.2, 203.0.113.9, 10.0.0.7",
			trusted: trusted,
			want:    "203.0.113.9",
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
			name:    "trusted peer, unparseable hop is skipped, never forwarded",
			peer:    "10.0.0.7:4000",
			xff:     "not-an-address, 203.0.113.9",
			trusted: trusted,
			want:    "203.0.113.9",
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
			// keep out of a header the application believes.
			name:    "trusted peer, host:port that is not an address is skipped",
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
