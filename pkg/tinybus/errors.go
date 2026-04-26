package tinybus

import "errors"

// Sentinel errors. All errors returned from the public API wrap one of
// these (via fmt.Errorf("%w", ...)) so callers can match with errors.Is.
var (
	// ErrClosed is returned when an operation is attempted on a Queue
	// after Close has been called.
	ErrClosed = errors.New("tinybus: queue closed")

	// ErrInvalidQueue is returned when a queue name is empty or contains
	// disallowed characters.
	ErrInvalidQueue = errors.New("tinybus: invalid queue name")

	// ErrNoJobs is an internal sentinel returned by claimNext when the
	// queue has no eligible jobs. The Process loop catches it and sleeps
	// rather than propagating it to handlers.
	ErrNoJobs = errors.New("tinybus: no jobs ready")
)
