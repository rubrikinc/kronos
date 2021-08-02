package tm

// Clock is used by Kronos for getting current time
type Clock interface {
	// Now returns time in UnixNanos
	Now() int64
	// Uptime returns the time since the clock was initialized.
	Uptime() int64
}
