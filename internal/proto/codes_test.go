package proto

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"
)

// TestErrCodeValues asserts the numeric value of every error code in
// docs/03-client-protocol.md §6.
//
// These are wire values. A client switches on the number, not on the name, so
// renumbering one is a breaking change for every deployed client — this table is what
// turns that from a silent break into a failing test.
func TestErrCodeValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  ErrCode
		want int
	}{
		{"internal", ErrInternal, 100},
		{"bad request", ErrBadRequest, 101},
		{"unknown namespace", ErrUnknownNamespace, 102},
		{"permission denied", ErrPermissionDenied, 103},
		{"already subscribed", ErrAlreadySubscribed, 104},
		{"not subscribed", ErrNotSubscribed, 105},
		{"rate limited", ErrRateLimited, 106},
		{"subscription limit", ErrSubscriptionLimit, 108},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if int(tt.got) != tt.want {
				t.Errorf("= %d, want %d", tt.got, tt.want)
			}
		})
	}
}

// TestCloseCodeValues asserts the numeric value of every close code in
// docs/03-client-protocol.md §7. Same reasoning as TestErrCodeValues: the close code
// reaches the browser as a number and nothing else.
func TestCloseCodeValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  CloseCode
		want int
	}{
		{"draining", CloseDraining, 3000},
		{"handshake timeout", CloseHandshakeTimeout, 3001},
		{"unauthorized", CloseUnauthorized, 3003},
		{"ping timeout", ClosePingTimeout, 3004},
		{"slow consumer", CloseSlowConsumer, 3005},
		{"protocol error", CloseProtocolError, 3006},
		{"rate limited", CloseRateLimited, 3007},
		{"auth unavailable", CloseAuthUnavailable, 3008},
		{"revoked", CloseRevoked, 3501},
		{"expired", CloseExpired, 3503},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if int(tt.got) != tt.want {
				t.Errorf("= %d, want %d", tt.got, tt.want)
			}
		})
	}

	// docs/03-client-protocol.md §7: the bands exist so a new code is additive rather
	// than a break. Assert them, because a code outside its band is a code a client's
	// "is this transport or authorization" branch classifies wrongly.
	for _, tt := range tests {
		switch {
		case tt.want >= 3000 && tt.want <= 3099:
		case tt.want >= 3500:
		default:
			t.Errorf("close code %s = %d is outside the 3000-3099 and 3500+ bands", tt.name, tt.want)
		}
	}
}

// TestCodeRegistriesAreClosed reads codes.go and asserts the declared constant sets are
// exactly what docs/03-client-protocol.md §6 and §7 list — no more, no less.
//
// The value assertions above catch a renumbering. This catches an *addition*, which the
// value assertions cannot see and which is the more likely mistake: someone re-adds one
// of the codes that were deliberately removed as unreachable (error 107, close 3002,
// close 3500), a client learns to expect it, and the gateway can never send it.
// docs/03-client-protocol.md is normative and the code registry is not allowed to drift
// from it (docs/AGENTS.md §4).
func TestCodeRegistriesAreClosed(t *testing.T) {
	t.Parallel()

	wantErr := map[string]int64{
		"ErrInternal": 100, "ErrBadRequest": 101, "ErrUnknownNamespace": 102,
		"ErrPermissionDenied": 103, "ErrAlreadySubscribed": 104, "ErrNotSubscribed": 105,
		"ErrRateLimited": 106, "ErrSubscriptionLimit": 108,
	}
	wantClose := map[string]int64{
		"CloseDraining": 3000, "CloseHandshakeTimeout": 3001, "CloseUnauthorized": 3003,
		"ClosePingTimeout": 3004, "CloseSlowConsumer": 3005, "CloseProtocolError": 3006,
		"CloseRateLimited": 3007, "CloseAuthUnavailable": 3008, "CloseRevoked": 3501,
		"CloseExpired": 3503,
	}

	gotErr, gotClose := declaredCodes(t, "codes.go")

	assertCodeSet(t, "ErrCode", gotErr, wantErr)
	assertCodeSet(t, "CloseCode", gotClose, wantClose)

	// The removals, named individually so a re-addition fails with the reason attached.
	//
	//   107  "frame too large": an oversize frame closes with CloseProtocolError, so
	//        there is no open connection left to answer on (docs/13-review-findings.md m2).
	//   3002 "origin rejected": the Origin check completes before the upgrade and
	//        answers HTTP 403, so no websocket exists to close (§7, M14).
	//   3500 "killed": an ungraceful kill sends nothing, by definition (M14).
	for name, value := range gotErr {
		if value == 107 {
			t.Errorf("%s = 107: error 107 was removed as unreachable and must not return", name)
		}
	}
	for name, value := range gotClose {
		if value == 3002 || value == 3500 {
			t.Errorf("%s = %d: that close code was removed as unreachable and must not return", name, value)
		}
	}
}

// assertCodeSet compares a declared constant set against the registry the specification
// lists.
func assertCodeSet(t *testing.T, typeName string, got, want map[string]int64) {
	t.Helper()

	for name, wantValue := range want {
		gotValue, ok := got[name]
		if !ok {
			t.Errorf("%s constant %s is missing; docs/03-client-protocol.md lists it", typeName, name)
			continue
		}
		if gotValue != wantValue {
			t.Errorf("%s = %d, want %d", name, gotValue, wantValue)
		}
	}
	for name, gotValue := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("%s constant %s = %d is not in docs/03-client-protocol.md; "+
				"a protocol change is a documentation change first (docs/AGENTS.md §4)",
				typeName, name, gotValue)
		}
	}
}

// declaredCodes parses one source file and returns the ErrCode and CloseCode constants
// it declares, by name and value.
func declaredCodes(t *testing.T, filename string) (errCodes, closeCodes map[string]int64) {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), filename, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", filename, err)
	}

	errCodes = map[string]int64{}
	closeCodes = map[string]int64{}

	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			typeIdent, ok := value.Type.(*ast.Ident)
			if !ok {
				t.Fatalf("%v: constant with no explicit type; the registries must stay explicit", value.Names)
			}
			var into map[string]int64
			switch typeIdent.Name {
			case "ErrCode":
				into = errCodes
			case "CloseCode":
				into = closeCodes
			default:
				t.Fatalf("unexpected constant type %s in %s", typeIdent.Name, filename)
			}
			for i, name := range value.Names {
				lit, ok := value.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.INT {
					t.Fatalf("%s: value is not an integer literal; wire codes must be written out", name.Name)
				}
				n, err := strconv.ParseInt(lit.Value, 10, 64)
				if err != nil {
					t.Fatalf("%s: %v", name.Name, err)
				}
				into[name.Name] = n
			}
		}
	}
	return errCodes, closeCodes
}
