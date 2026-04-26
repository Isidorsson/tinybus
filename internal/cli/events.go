package cli

import (
	"sync"
	"time"
)

// EventKind names a job state transition recorded in the activity feed.
type EventKind string

const (
	EventEnqueued  EventKind = "enqueued"
	EventClaimed   EventKind = "claimed"
	EventCompleted EventKind = "completed"
	EventFailed    EventKind = "failed"
	EventDead      EventKind = "dead"
	EventReset     EventKind = "reset"
)

// Event is one entry in the dashboard activity feed.
type Event struct {
	Time     time.Time `json:"time"`
	Kind     EventKind `json:"kind"`
	JobID    int64     `json:"job_id"`
	Queue    string    `json:"queue"`
	Attempts int       `json:"attempts,omitempty"`
	WorkerID string    `json:"worker_id,omitempty"`
	Detail   string    `json:"detail,omitempty"`
}

// Recorder is a fixed-capacity ring buffer of recent events. Safe for
// concurrent use. Old entries are overwritten in FIFO order.
type Recorder struct {
	mu    sync.Mutex
	ring  []Event
	cap   int
	next  int
	count int
}

// NewRecorder creates a Recorder that retains the last `capacity` events.
func NewRecorder(capacity int) *Recorder {
	if capacity < 1 {
		capacity = 1
	}
	return &Recorder{cap: capacity, ring: make([]Event, capacity)}
}

// Record appends an event to the ring buffer. If Time is zero, it is set
// to time.Now().
func (r *Recorder) Record(e Event) {
	if r == nil {
		return
	}
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ring[r.next] = e
	r.next = (r.next + 1) % r.cap
	if r.count < r.cap {
		r.count++
	}
}

// Since returns events whose Time is strictly greater than the given
// UnixMilli timestamp. The result is in insertion (chronological) order.
func (r *Recorder) Since(sinceUnixMilli int64) []Event {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Event, 0, r.count)
	for i := 0; i < r.count; i++ {
		idx := (r.next - r.count + i + r.cap) % r.cap
		ev := r.ring[idx]
		if ev.Time.UnixMilli() > sinceUnixMilli {
			out = append(out, ev)
		}
	}
	return out
}
