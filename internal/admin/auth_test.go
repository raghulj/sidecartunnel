package admin

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"os"
	"strings"
	"testing"
)

// authenticatedRoutes are the three routes docs/04-integration.md §4 puts behind the
// bearer token.
var authenticatedRoutes = []struct{ method, target, body string }{
	{http.MethodGet, "/channels", ""},
	{http.MethodGet, "/channels/room-4410", ""},
	{http.MethodPost, "/disconnect", `{"user":"u-7"}`},
}

// TestAuth_UnconfiguredTokenIs404_FR20 is the rule docs/04-integration.md §4 states in
// capitals: when admin.token is unset the authenticated routes MUST return 404 rather
// than being open.
//
// 404 rather than 401 is deliberate. An accidentally unconfigured admin API should look
// absent, not merely closed: a 401 advertises an operator surface that is one
// misconfiguration away from being reachable, and it tells a scanner exactly which port
// to come back to.
func TestAuth_UnconfiguredTokenIs404_FR20(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.Token = "" })

	for _, rt := range authenticatedRoutes {
		t.Run(rt.method+" "+rt.target, func(t *testing.T) {
			// Unauthenticated, with a token, and with a plausible guess: all absent.
			for _, header := range []string{"", "Bearer ", "Bearer " + testToken} {
				w := h.do(t, rt.method, rt.target, rt.body, "Authorization", header)
				if w.Code != http.StatusNotFound {
					t.Errorf("%s %s with Authorization %q = %d, want 404", rt.method, rt.target, header, w.Code)
				}
			}
		})
	}

	// The unauthenticated routes still work: an unconfigured token disables the operator
	// surface, not liveness and readiness.
	for _, path := range []string{"/health", "/ready", "/metrics"} {
		if w := h.do(t, http.MethodGet, path, ""); w.Code != http.StatusOK {
			t.Errorf("%s with no admin.token = %d, want 200", path, w.Code)
		}
	}
	if got := h.registry.seen(); len(got) != 0 {
		t.Errorf("a request reached the registry with no token configured: %+v", got)
	}
}

// TestAuth_Rejections covers every way a bearer token can fail to match. FR-20's
// acceptance criterion is that an unauthenticated /channels returns 401 while /metrics
// does not.
func TestAuth_Rejections(t *testing.T) {
	h := newHarness(t, nil)

	tests := []struct {
		name   string
		header string
	}{
		{"no header", ""},
		{"wrong token", "Bearer wrong-token-entirely"},
		{"same length, one byte different", "Bearer " + testToken[:len(testToken)-1] + "X"},
		{"a prefix of the token", "Bearer " + testToken[:4]},
		{"the token with something appended", "Bearer " + testToken + "x"},
		{"no scheme", testToken},
		{"wrong scheme", "Basic " + testToken},
		{"scheme only", "Bearer"},
		{"empty token", "Bearer "},
		{"leading space", " Bearer " + testToken},
	}
	for _, tt := range tests {
		for _, rt := range authenticatedRoutes {
			t.Run(tt.name+" "+rt.target, func(t *testing.T) {
				w := h.do(t, rt.method, rt.target, rt.body, "Authorization", tt.header)
				if w.Code != http.StatusUnauthorized {
					t.Errorf("%s %s with %q = %d, want 401", rt.method, rt.target, tt.header, w.Code)
				}
				if got := w.Header().Get("WWW-Authenticate"); !strings.HasPrefix(got, "Bearer") {
					t.Errorf("WWW-Authenticate = %q, want a Bearer challenge", got)
				}
			})
		}
	}
	if got := h.registry.seen(); len(got) != 0 {
		t.Errorf("a rejected request reached the registry: %+v", got)
	}
}

// TestAuth_Accepts covers the shapes that must be accepted, including the case-insensitive
// scheme RFC 6750 §2.1 specifies.
func TestAuth_Accepts(t *testing.T) {
	h := newHarness(t, nil)
	for _, header := range []string{"Bearer " + testToken, "bearer " + testToken, "BEARER " + testToken} {
		w := h.do(t, http.MethodGet, "/channels", "", "Authorization", header)
		if w.Code != http.StatusOK {
			t.Errorf("Authorization %q = %d, want 200", header, w.Code)
		}
	}
}

// TestAuth_ConstantTimeComparison_FR20 asserts the comparison itself, not just its
// outcome.
//
// A behavioural test cannot tell subtle.ConstantTimeCompare from ==: both accept the
// right token and reject the wrong one, and the difference is a timing side channel that
// no assertion on a status code can see. FR-20 names the function, so the test reads the
// source and asserts the function is the one being called and that the token is never
// compared with == or !=.
func TestAuth_ConstantTimeComparison_FR20(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}

	fset := token.NewFileSet()
	constantTime := false
	var offenders []string
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.SelectorExpr:
				if id, ok := node.X.(*ast.Ident); ok && id.Name == "subtle" && node.Sel.Name == "ConstantTimeCompare" {
					constantTime = true
				}
			case *ast.BinaryExpr:
				if node.Op != token.EQL && node.Op != token.NEQ {
					return true
				}
				if permittedComparison(node) {
					return true
				}
				if mentionsToken(node.X) || mentionsToken(node.Y) {
					offenders = append(offenders, name+": "+exprString(node.X)+" compared with "+exprString(node.Y))
				}
			}
			return true
		})
	}
	if !constantTime {
		t.Error("subtle.ConstantTimeCompare is not called anywhere in the package; FR-20 requires the bearer token be compared in constant time")
	}
	for _, o := range offenders {
		t.Errorf("the token is compared with an equality operator, which leaks its content in the comparison's timing: %s", o)
	}
}

// permittedComparison allows the two equality comparisons that are not a content
// comparison of the secret: testing whether admin.token is configured at all, which is a
// comparison against the empty literal, and testing the result of ConstantTimeCompare
// itself, which is an int.
func permittedComparison(node *ast.BinaryExpr) bool {
	for _, side := range []ast.Expr{node.X, node.Y} {
		if lit, ok := side.(*ast.BasicLit); ok && lit.Kind == token.STRING && lit.Value == `""` {
			return true
		}
		if call, ok := side.(*ast.CallExpr); ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "ConstantTimeCompare" {
				return true
			}
		}
	}
	return false
}

// mentionsToken reports whether an expression names a token field or variable. It is
// deliberately loose: a false positive here is a test failure a developer reads, and a
// false negative is a timing side channel nobody sees.
func mentionsToken(e ast.Expr) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && strings.Contains(strings.ToLower(id.Name), "token") {
			found = true
		}
		return true
	})
	return found
}

// exprString renders an expression for a failure message.
func exprString(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.SelectorExpr:
		return exprString(v.X) + "." + v.Sel.Name
	case *ast.BasicLit:
		return v.Value
	case *ast.CallExpr:
		return exprString(v.Fun) + "(…)"
	default:
		return "expression"
	}
}

// TestAuth_NoSecretInResponseOrLog_NFR7 drives every route with a cookie and an
// Authorization header and asserts that neither appears in any response body or any log
// line, at debug level.
func TestAuth_NoSecretInResponseOrLog_NFR7(t *testing.T) {
	const (
		cookie = "sessionid=9f8e7d6c5b4a39281706" //nolint:gosec // a fixture, not a credential
		wrong  = "not-the-admin-token-but-secret"
	)
	h := newHarness(t, nil)

	requests := []struct{ method, target, body string }{
		{http.MethodGet, "/health", ""},
		{http.MethodGet, "/ready", ""},
		{http.MethodGet, "/metrics", ""},
		{http.MethodGet, "/channels", ""},
		{http.MethodGet, "/channels/room-4410", ""},
		{http.MethodPost, "/disconnect", `{"user":"u-7"}`},
	}
	for _, rq := range requests {
		for _, auth := range []string{"Bearer " + testToken, "Bearer " + wrong} {
			w := h.do(t, rq.method, rq.target, rq.body, "Authorization", auth, "Cookie", cookie)
			for _, secret := range []string{cookie, "9f8e7d6c5b4a39281706", wrong, testToken} {
				if strings.Contains(w.Body.String(), secret) {
					t.Errorf("%s %s response body carries a secret", rq.method, rq.target)
				}
			}
		}
	}

	logs := h.logs.String()
	if logs == "" {
		t.Fatal("nothing was logged; the assertion below would pass vacuously")
	}
	for _, secret := range []string{cookie, "9f8e7d6c5b4a39281706", wrong, testToken} {
		if strings.Contains(logs, secret) {
			t.Errorf("a log line carries a secret (NFR-7):\n%s", logs)
		}
	}
}
