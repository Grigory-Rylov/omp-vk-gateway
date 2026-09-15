// Package buildinfo carries build-time metadata stamped into the binaries
// by build.sh (-ldflags -X buildinfo.BuildTime=...).
package buildinfo

import (
	"time"
)

// BuildTime is the UTC build timestamp (RFC3339); "unknown" when the
// binary was built without ldflags.
var BuildTime = "unknown"

// HumanReadable formats BuildTime in the local timezone.
func HumanReadable() string {
	t, err := time.Parse(time.RFC3339, BuildTime)
	if err != nil {
		return BuildTime
	}
	return t.Local().Format("2006-01-02 15:04:05")
}
