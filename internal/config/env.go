package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// The environment convention from docs/08-config.md §1: an ST_ prefix, __ between levels,
// so server.path is ST_SERVER__PATH. Any key may instead be supplied from a file by
// appending _FILE, which is how Docker and Swarm mount secrets.
const (
	envPrefix     = "ST_"
	envSeparator  = "__"
	envFileSuffix = "_FILE"

	// envNamespacesJSON carries the whole namespaces list as JSON. It exists because a
	// list of objects cannot be expressed as environment scalars, and pretending
	// otherwise is what produced the earlier env-only example that configured no
	// namespaces at all (docs/08-config.md §1).
	envNamespacesJSON = envPrefix + "NAMESPACES_JSON"
)

var (
	durationType   = reflect.TypeOf(Duration(0))
	namespacesType = reflect.TypeOf([]Namespace(nil))
)

// applyEnv overlays the environment onto c, which already carries the defaults and
// anything the YAML file set. The environment wins, per docs/08-config.md §1.
//
// Unrecognised ST_ variables are ignored rather than rejected. The prefix is not
// exclusively ours in practice — docs/08-config.md §4's own worked example expects
// ST_WEBHOOK_SECRET to exist in the environment for the deployment to substitute — so
// refusing to start on one would break the documented deployment. A mistyped key is caught
// instead by the required-key rules in Validate.
func applyEnv(c *Config) error {
	if err := applyEnvStruct(reflect.ValueOf(c).Elem(), ""); err != nil {
		return err
	}
	return applyEnvNamespaces(c)
}

// applyEnvStruct walks one level of the config tree, recursing into nested blocks and
// building the environment key from the yaml tags as it goes.
func applyEnvStruct(v reflect.Value, prefix string) error {
	t := v.Type()
	for i := range t.NumField() {
		field := t.Field(i)
		if field.Type == namespacesType {
			// Handled by ST_NAMESPACES_JSON; a list of objects has no scalar spelling.
			continue
		}
		key := envKey(prefix, field.Tag.Get("yaml"))
		value := v.Field(i)
		if value.Kind() == reflect.Struct {
			if err := applyEnvStruct(value, key); err != nil {
				return err
			}
			continue
		}
		raw, ok, err := lookupEnv(key)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if err := setField(value, raw); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
	}
	return nil
}

// envKey builds ST_SERVER__PATH from the nesting path and a yaml tag.
func envKey(prefix, tag string) string {
	if prefix == "" {
		return envPrefix + strings.ToUpper(tag)
	}
	return prefix + envSeparator + strings.ToUpper(tag)
}

// lookupEnv resolves one key, preferring the _FILE form.
//
// _FILE wins over the plain variable when both are set: the file is the Docker or Swarm
// secret, and a stale plain variable left in a compose file must not silently outrank the
// mounted secret. The trailing newline that every editor and `echo` appends is stripped,
// because a secret with a newline on the end fails signature comparison in a way that is
// very hard to see.
func lookupEnv(key string) (string, bool, error) {
	if path, ok := os.LookupEnv(key + envFileSuffix); ok {
		// #nosec G304 -- reading an operator-named path is the whole feature: this is
		// the Docker and Swarm secret convention from docs/08-config.md §1.
		content, err := os.ReadFile(path)
		if err != nil {
			return "", false, fmt.Errorf("%s%s: %w", key, envFileSuffix, err)
		}
		return strings.TrimRight(string(content), "\r\n"), true, nil
	}
	value, ok := os.LookupEnv(key)
	return value, ok, nil
}

// setField decodes one environment string into one config field.
//
// The default arm is not reachable from the current Config tree, and that is the point:
// adding a field of a kind this cannot decode fails loudly at startup instead of leaving
// an environment key that silently does nothing.
func setField(target reflect.Value, raw string) error {
	switch {
	case target.Type() == durationType:
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", raw, err)
		}
		target.SetInt(int64(parsed))
	case target.Kind() == reflect.String:
		target.SetString(raw)
	case target.Kind() == reflect.Bool:
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("invalid boolean %q: %w", raw, err)
		}
		target.SetBool(parsed)
	case target.Kind() == reflect.Int:
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return fmt.Errorf("invalid integer %q: %w", raw, err)
		}
		target.SetInt(int64(parsed))
	case target.Kind() == reflect.Slice && target.Type().Elem().Kind() == reflect.String:
		target.Set(reflect.ValueOf(splitList(raw)))
	default:
		return fmt.Errorf("no environment decoding for a field of type %s", target.Type())
	}
	return nil
}

// splitList decodes a comma-separated scalar list (docs/08-config.md §1). Surrounding
// spaces are trimmed so "a, b" and "a,b" agree; an empty value is an empty list, which is
// how a list set in the file is cleared from the environment.
func splitList(raw string) []string {
	if raw == "" {
		return []string{}
	}
	parts := strings.Split(raw, ",")
	out := make([]string, len(parts))
	for i, part := range parts {
		out[i] = strings.TrimSpace(part)
	}
	return out
}

// applyEnvNamespaces replaces the namespaces list from ST_NAMESPACES_JSON, or from the
// file named by ST_NAMESPACES_JSON_FILE.
//
// The YAML decoder parses it, since YAML is a superset of JSON. That is not a shortcut:
// it means the list obeys exactly the same yaml tags and the same strict unknown-key rule
// as the file does, so a namespace key removed by a design decision — auth_required,
// which docs/13-review-findings.md S4 cut — fails startup naming itself here too, rather
// than through a second set of struct tags that can drift.
func applyEnvNamespaces(c *Config) error {
	raw, ok, err := lookupEnv(envNamespacesJSON)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	var namespaces []Namespace
	decoder := yaml.NewDecoder(strings.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&namespaces); err != nil {
		return fmt.Errorf("%s: %w", envNamespacesJSON, err)
	}
	c.Namespaces = namespaces
	return nil
}

// loadFile decodes the YAML file at path over the defaults already in c.
//
// Decoding is strict: an unrecognised key is an error naming it, so a typo, or a key a
// design decision removed, fails loudly instead of sitting in the file doing nothing. An
// empty document is not an error — it simply contributes nothing.
func loadFile(c *Config, path string) error {
	// #nosec G304 -- the path is the operator's --config argument; reading it is the point.
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	if err := decoder.Decode(c); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	return nil
}
