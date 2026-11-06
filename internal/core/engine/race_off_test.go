//go:build !race

package engine

// raceEnabled reports whether the test binary was built with -race.
const raceEnabled = false
