package config

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that decodes from a YAML or environment string such as
// "25s", "200ms" or "6h".
//
// It exists because gopkg.in/yaml.v3 will not decode "25s" into a time.Duration, and
// every duration key in docs/08-config.md is written that way. The alternatives were
// integer seconds, which loses the 200ms floor on bus.reconnect_min and reads badly, or a
// shadow struct per block, which duplicates the whole tree. A named type is the smallest
// thing that works.
//
// The underlying type is time.Duration, so time.Duration(cfg.Server.PingInterval)
// converts for free; Duration returns the same value as a method where that reads better.
type Duration time.Duration

// Duration returns d as a time.Duration.
func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

// String returns the duration in Go's duration format, e.g. "25s".
func (d Duration) String() string {
	return time.Duration(d).String()
}

// UnmarshalYAML decodes a duration string. A value that is not a string, or that
// time.ParseDuration rejects, is an error naming the offending value — never a silent
// zero, which would turn a typo'd ping_interval into a gateway that never pings (NFR-5).
//
// The node's raw scalar text is used rather than a decode into string, so that the
// unquoted `ping_interval: 30` in docs/08-config.md §3's format fails with "invalid
// duration \"30\": missing unit" — which names the value an operator has to fix — instead
// of yaml.v3's "cannot unmarshal !!int into string", which does not.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode {
		return fmt.Errorf("line %d: a duration must be a scalar such as \"25s\"", value.Line)
	}
	parsed, err := time.ParseDuration(value.Value)
	if err != nil {
		return fmt.Errorf("line %d: invalid duration %q: %w", value.Line, value.Value, err)
	}
	*d = Duration(parsed)
	return nil
}

// MarshalYAML encodes the duration as a string, so that a config dumped for an operator
// round-trips through this package rather than coming back as a nanosecond count.
func (d Duration) MarshalYAML() (any, error) {
	return d.String(), nil
}
