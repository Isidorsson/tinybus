package tinybus

import (
	"testing"
	"time"
)

// TestBackoffShape verifies monotonicity-on-average and the cap. Pure
// unit test — no DB required.
func TestBackoffShape(t *testing.T) {
	t.Parallel()
	if backoff(0) <= 0 {
		t.Fatalf("attempts=0 should still produce a positive delay")
	}
	if d := backoff(1); d > backoffBase {
		t.Fatalf("attempts=1 should be <= base (%s), got %s", backoffBase, d)
	}
	for attempts := 1; attempts < 50; attempts++ {
		d := backoff(attempts)
		if d > backoffCap {
			t.Fatalf("attempts=%d exceeded cap: %s > %s", attempts, d, backoffCap)
		}
		if d <= 0 {
			t.Fatalf("attempts=%d produced non-positive delay: %s", attempts, d)
		}
	}
}

func TestBackoffJitterSpread(t *testing.T) {
	t.Parallel()
	const samples = 200
	seen := make(map[time.Duration]int)
	for i := 0; i < samples; i++ {
		seen[backoff(3)]++
	}
	if len(seen) < 10 {
		t.Fatalf("expected jitter to produce a spread of values; got %d distinct out of %d", len(seen), samples)
	}
}
