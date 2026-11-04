// Package override holds the scenario switches both E2E SUTs (the e2e build
// of cxp and newcore-sut) read from their environment, so a scenario can
// compress a production duration or give a helper another host name
// without a code path that production builds contain.
package override

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
)

const (
	// EnvTiming overrides engine.Timing fields after the scenario timing
	// is chosen: "Field=duration[,Field=duration...]" (int fields take a
	// number), e.g. "SilenceHint=2s,SilenceStuck=4s,StallLimit=8s".
	EnvTiming = "E2E_SUT_TIMING"
	// EnvHostname replaces os.Hostname() for the owner row, the machine
	// label default and the registry, so two helpers in one container act
	// as two hosts sharing one home.
	EnvHostname = "E2E_SUT_HOSTNAME"
)

// Timing applies EnvTiming to t.
func Timing(t *engine.Timing) error { return ApplyTiming(t, os.Getenv(EnvTiming)) }

// ApplyTiming applies spec ("Field=value,...") to t.
func ApplyTiming(t *engine.Timing, spec string) error {
	v := reflect.ValueOf(t).Elem()
	for _, item := range strings.Split(spec, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		name, val, ok := strings.Cut(item, "=")
		if !ok {
			return fmt.Errorf("%s: %q is not Field=value", EnvTiming, item)
		}
		f := v.FieldByName(strings.TrimSpace(name))
		if !f.IsValid() || !f.CanSet() {
			return fmt.Errorf("%s: engine.Timing has no field %q", EnvTiming, name)
		}
		val = strings.TrimSpace(val)
		switch {
		case f.Type() == reflect.TypeOf(time.Duration(0)):
			d, err := time.ParseDuration(val)
			if err != nil {
				return fmt.Errorf("%s: %s: %w", EnvTiming, name, err)
			}
			f.SetInt(int64(d))
		case f.Kind() == reflect.Int:
			n, err := strconv.Atoi(val)
			if err != nil {
				return fmt.Errorf("%s: %s: %w", EnvTiming, name, err)
			}
			f.SetInt(int64(n))
		default:
			return fmt.Errorf("%s: field %q has unsupported type %s", EnvTiming, name, f.Type())
		}
	}
	return nil
}

// Hostname is EnvHostname, or def when it is unset.
func Hostname(def string) string {
	if h := strings.TrimSpace(os.Getenv(EnvHostname)); h != "" {
		return h
	}
	return def
}
