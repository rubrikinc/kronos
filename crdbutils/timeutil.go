package leaktest

import "time"

// Now returns the current UTC time.
func Now() time.Time {
	return time.Now().UTC()
}
