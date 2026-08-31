package bus

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// tcpProxy sits between the bus and Redis. It exists for two assertions that cannot be
// made any other way:
//
//   - what the bus actually put on the wire, so "Sync batches" and "Sync issues no second
//     round of commands" are assertions about round trips rather than about internal
//     bookkeeping that could be right while the wire is wrong (M7, S2);
//   - killing the connection under the bus mid-flight, which is the only honest way to
//     test NFR-8 against both miniredis and a real Redis.
//
// It speaks just enough RESP to name each command the client sends. Replies are copied
// back verbatim, so RESP3 push frames and anything else the server invents pass through
// untouched.
type tcpProxy struct {
	target string
	ln     net.Listener

	mu       sync.Mutex
	cmds     []busCommand
	live     []net.Conn
	conns    int
	refusing bool
	wake     chan struct{}
}

// busCommand is one command the bus sent, by name and argument count. The argument count
// is what makes the chunking assertion possible: a 256-channel SUBSCRIBE and 256
// single-channel SUBSCRIBEs are indistinguishable by name alone, and the difference
// between them is M7.
type busCommand struct {
	conn int
	name string
	args int
}

func newProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &tcpProxy{target: target, ln: ln, wake: make(chan struct{})}
	go p.serve()
	t.Cleanup(func() {
		_ = ln.Close()
		p.Break()
	})
	return p
}

// addr is the address the bus should be pointed at.
func (p *tcpProxy) addr() string { return p.ln.Addr().String() }

func (p *tcpProxy) serve() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(client)
	}
}

func (p *tcpProxy) handle(client net.Conn) {
	p.mu.Lock()
	refusing := p.refusing
	p.conns++
	id := p.conns
	p.mu.Unlock()
	if refusing {
		_ = client.Close()
		return
	}

	server, err := net.DialTimeout("tcp", p.target, 2*time.Second)
	if err != nil {
		_ = client.Close()
		return
	}
	p.track(client, server)
	defer func() {
		_ = client.Close()
		_ = server.Close()
	}()

	go func() {
		_, _ = io.Copy(client, server)
		_ = client.Close()
	}()

	r := bufio.NewReader(client)
	for {
		args, err := readCommand(r)
		if err != nil {
			return
		}
		p.record(id, args)
		if _, err := server.Write(encodeCommand(args)); err != nil {
			return
		}
	}
}

func (p *tcpProxy) track(conns ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.live = append(p.live, conns...)
}

func (p *tcpProxy) record(conn int, args []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cmds = append(p.cmds, busCommand{conn: conn, name: strings.ToLower(args[0]), args: len(args) - 1})
	close(p.wake)
	p.wake = make(chan struct{})
}

// Break closes every live connection, which is what losing Redis looks like from inside
// the process: reads fail, and nothing tells the bus why.
func (p *tcpProxy) Break() {
	p.mu.Lock()
	live := p.live
	p.live = nil
	p.mu.Unlock()
	for _, c := range live {
		_ = c.Close()
	}
}

// Refuse makes new connections fail, so a test can hold the bus disconnected for as long
// as it needs to.
func (p *tcpProxy) Refuse(refusing bool) {
	p.mu.Lock()
	p.refusing = refusing
	p.mu.Unlock()
	if refusing {
		p.Break()
	}
}

// commands returns every command recorded so far.
func (p *tcpProxy) commands() []busCommand {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]busCommand(nil), p.cmds...)
}

// waitFor blocks until the recorded commands satisfy want, waking on each new command
// rather than polling, and fails the test if that has not happened within receiveTimeout.
func (p *tcpProxy) waitFor(t *testing.T, what string, want func([]busCommand) bool) []busCommand {
	t.Helper()
	deadline := time.After(receiveTimeout)
	for {
		p.mu.Lock()
		cmds := append([]busCommand(nil), p.cmds...)
		wake := p.wake
		p.mu.Unlock()
		if want(cmds) {
			return cmds
		}
		select {
		case <-wake:
		case <-deadline:
			t.Fatalf("proxy: %s not seen within %s; commands so far: %v", what, receiveTimeout, summarise(cmds))
			return nil
		}
	}
}

// onConn returns the commands recorded on one connection, in order. The bus opens a
// connection per generation, and go-redis opens one of its own during a failed read (see
// TestRedisReconnectKeepsClientsAndRestoresDelivery), so "what did this bus put on the
// wire" is only answerable per connection.
func onConn(cmds []busCommand, conn int) []busCommand {
	var out []busCommand
	for _, c := range cmds {
		if c.conn == conn {
			out = append(out, c)
		}
	}
	return out
}

// connIDs returns every connection id seen, in ascending order.
func connIDs(cmds []busCommand) []int {
	var out []int
	for _, c := range cmds {
		if len(out) == 0 || out[len(out)-1] != c.conn {
			if !slices.Contains(out, c.conn) {
				out = append(out, c.conn)
			}
		}
	}
	slices.Sort(out)
	return out
}

// only returns the recorded commands with the given name, in order.
func only(cmds []busCommand, name string) []busCommand {
	var out []busCommand
	for _, c := range cmds {
		if c.name == name {
			out = append(out, c)
		}
	}
	return out
}

// argsOf returns the argument count of each command, which for SUBSCRIBE and UNSUBSCRIBE
// is the number of channels in that one round trip.
func argsOf(cmds []busCommand) []int {
	out := make([]int, 0, len(cmds))
	for _, c := range cmds {
		out = append(out, c.args)
	}
	return out
}

func total(counts []int) int {
	sum := 0
	for _, n := range counts {
		sum += n
	}
	return sum
}

func summarise(cmds []busCommand) string {
	var b strings.Builder
	for _, c := range cmds {
		fmt.Fprintf(&b, " %d:%s/%d", c.conn, c.name, c.args)
	}
	return b.String()
}

// readCommand reads one RESP array of bulk strings — the only shape a Redis client sends.
func readCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("proxy: want a command array, got %q", line)
	}
	n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil || n < 1 {
		return nil, fmt.Errorf("proxy: bad array header %q", line)
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		head, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if len(head) == 0 || head[0] != '$' {
			return nil, fmt.Errorf("proxy: want a bulk string, got %q", head)
		}
		size, err := strconv.Atoi(strings.TrimSpace(head[1:]))
		if err != nil || size < 0 {
			return nil, fmt.Errorf("proxy: bad bulk header %q", head)
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}

func encodeCommand(args []string) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	return []byte(b.String())
}

// proxiedURL points a Redis URL at the proxy while keeping its credentials and database.
func proxiedURL(t *testing.T, raw, addr string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	u.Host = addr
	return u.String()
}

// hostOf is the address the proxy should forward to.
func hostOf(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return u.Host
}
