package proto

import (
	"encoding/json"
	"testing"
)

// FuzzDecode drives Decode with arbitrary bytes.
//
// Decode is the gateway's entire attack surface for frame content: it runs on every text
// frame from every connected browser, before any authorization, on a goroutine whose
// panic would take the process down and with it every other connection on the replica
// (docs/14-coding-standards.md §6). "It returns an error" is not the property under test
// — "it never panics, on anything" is.
//
// The corpus in testdata/fuzz/FuzzDecode holds the inputs that have historically been
// the shape of a codec bug: nesting, truncation, unicode, and numbers at the edges of
// int64. Seeds live here too so that a plain `go test` exercises them without -fuzz.
func FuzzDecode(f *testing.F) {
	seeds := []string{
		// Valid frames from docs/03-client-protocol.md §3.
		`{"id":1,"connect":{"subs":["room-4410"]}}`,
		`{"id":2,"subscribe":{"channel":"room-4410"}}`,
		`{"id":3,"unsubscribe":{"channel":"room-4410"}}`,
		`{"id":4,"publish":{"channel":"desk-42","event":"typing","data":{"typing":true}}}`,
		`{"id":5,"sync":{}}`,
		`{"ping":{}}`,

		// Framing violations: zero keys, several keys, unknown command.
		`{}`,
		`{"id":1}`,
		`{"subscribe":{"channel":"a"},"ping":{}}`,
		`{"presence":{}}`,

		// id edges.
		`{"id":0,"ping":{}}`,
		`{"id":-1,"ping":{}}`,
		`{"id":9223372036854775807,"ping":{}}`,
		`{"id":9223372036854775808,"ping":{}}`,
		`{"id":1.5,"ping":{}}`,
		`{"id":null,"ping":{}}`,

		// Not an object, or not JSON at all.
		``,
		` `,
		`null`,
		`[]`,
		`"ping"`,
		`{`,
		`{"subscribe":{"channel":`,
		"\x00\x01\x02",
		`{"subscribe":{"channel":"\ud800"}}`,
		`{"subscribe":{"channel":"日本語"}}`,

		// Nesting and pathological values.
		`{"connect":{"subs":[[[[[[[[[[]]]]]]]]]]}}`,
		`{"publish":{"channel":"a","event":"e","data":1e1000}}`,
		`{"publish":{"channel":"a","event":"e","data":{"a":{"b":{"c":[1,2,3]}}}}}`,
	}
	for _, seed := range seeds {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		cmd, err := Decode(data)
		if err != nil {
			// The failure path must leave nothing half-built: a caller that logs the
			// command alongside the error must not see a partially populated frame.
			if cmd != (Command{}) {
				t.Fatalf("Decode(%q) returned %#v alongside error %v", data, cmd, err)
			}
			return
		}

		// docs/03-client-protocol.md §3: a frame that decodes carries exactly one
		// command and a non-negative id. Everything downstream — internal/conn's
		// dispatch switch above all — is written assuming both.
		if n := countCommands(cmd); n != 1 {
			t.Fatalf("Decode(%q) succeeded with %d command keys, want exactly 1", data, n)
		}
		if cmd.ID < 0 {
			t.Fatalf("Decode(%q) succeeded with id %d, want a positive integer or none", data, cmd.ID)
		}

		// A decoded command must survive re-serialization: json.RawMessage carried out
		// of Decode is the one field that can hold bytes the encoder later rejects, and
		// it reaches the fan-out path.
		if _, err := json.Marshal(cmd); err != nil {
			t.Fatalf("Decode(%q) produced a command that will not marshal: %v", data, err)
		}
	})
}

// countCommands returns how many command pointers on cmd are non-nil.
func countCommands(cmd Command) int {
	n := 0
	for _, present := range []bool{
		cmd.Connect != nil, cmd.Subscribe != nil, cmd.Unsubscribe != nil,
		cmd.Publish != nil, cmd.Sync != nil, cmd.Ping != nil,
	} {
		if present {
			n++
		}
	}
	return n
}
