package tinybus

import (
	"math/rand/v2"
	"time"
)

// Tunable: knobs for the default backoff. Exported for documentation
// rather than configuration — change them by writing your own backoff
// function and calling it from your handler if you need different shape.
const (
	backoffBase = 1 * time.Second
	backoffCap  = 5 * time.Minute
)

// backoff returns the delay before the next retry of a job that has
// failed `attempts` times.
//
// Shape: equal-jitter exponential.
//
//	d    = base * 2^(attempts-1), capped
//	half = d / 2
//	out  = half + rand[0, half]
//
// Why equal jitter (vs full jitter): full jitter (`rand[0, d]`) gives
// occasional near-zero retries, which can cause hot retries to dogpile.
// Equal jitter guarantees at least `d/2` separation between consecutive
// retries from the same worker, while still spreading retries across
// workers — the AWS Architecture Blog argues this is the "sweet spot"
// for most queues.
func backoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	// shift by attempts-1; guard against overflow at large attempts.
	shift := attempts - 1
	if shift > 30 {
		shift = 30
	}
	d := backoffBase << shift
	if d <= 0 || d > backoffCap {
		d = backoffCap
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}
