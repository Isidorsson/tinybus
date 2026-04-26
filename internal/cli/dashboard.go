package cli

import (
	"context"
	_ "embed"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/Isidorsson/tinybus/pkg/tinybus"
)

//go:embed web/index.html
var dashboardHTML []byte

// dashboard owns the HTTP routes that back the live demo page.
type dashboard struct {
	q          *tinybus.Queue
	rec        *Recorder
	log        *slog.Logger
	queue      string // queue name used by the dashboard's enqueue buttons
	enqueueMax int    // per-request cap on the count parameter
}

func newDashboard(q *tinybus.Queue, rec *Recorder, log *slog.Logger, queue string) *dashboard {
	return &dashboard{
		q:          q,
		rec:        rec,
		log:        log,
		queue:      queue,
		enqueueMax: 100,
	}
}

func (d *dashboard) routes(mux *http.ServeMux) {
	// `/{$}` matches only "/" exactly, leaving /healthz, /stats, etc. alone.
	mux.HandleFunc("GET /{$}", d.serveIndex)
	mux.HandleFunc("POST /enqueue", d.handleEnqueue)
	mux.HandleFunc("POST /reset", d.handleReset)
	mux.HandleFunc("GET /events", d.handleEvents)
}

func (d *dashboard) serveIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(dashboardHTML)
}

type enqueueRequest struct {
	Queue   string `json:"queue"`
	Count   int    `json:"count"`
	Failing bool   `json:"failing"`
}

func (d *dashboard) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	var req enqueueRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if req.Queue == "" {
		req.Queue = d.queue
	}
	if req.Count < 1 {
		req.Count = 1
	}
	if req.Count > d.enqueueMax {
		req.Count = d.enqueueMax
	}

	// Payload tagged "fail":true makes the demo handler return an error
	// every time, exercising the retry → dead transition end-to-end.
	payload := []byte(`{"hello":"dashboard"}`)
	if req.Failing {
		payload = []byte(`{"fail":true}`)
	}

	for i := 0; i < req.Count; i++ {
		id, err := d.q.Enqueue(r.Context(), req.Queue, payload)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		detail := ""
		if req.Failing {
			detail = "marked to fail"
		}
		d.rec.Record(Event{
			Kind:   EventEnqueued,
			JobID:  id,
			Queue:  req.Queue,
			Detail: detail,
		})
	}
	w.WriteHeader(http.StatusNoContent)
}

func (d *dashboard) handleReset(w http.ResponseWriter, r *http.Request) {
	n, err := d.q.ClearDead(r.Context(), d.queue)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	d.rec.Record(Event{
		Kind:   EventReset,
		Queue:  d.queue,
		Detail: "cleared " + strconv.FormatInt(n, 10) + " dead jobs",
	})
	w.WriteHeader(http.StatusNoContent)
}

func (d *dashboard) handleEvents(w http.ResponseWriter, r *http.Request) {
	since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
	events := d.rec.Since(since)
	if events == nil {
		events = []Event{}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(events); err != nil {
		d.log.Warn("tinybus: write events response", slog.String("err", err.Error()))
	}
}

// recordingHandler decorates a tinybus.Handler with event recording, so
// each claim/complete/fail/dead transition appears in the dashboard feed.
func recordingHandler(rec *Recorder, workerID string, inner tinybus.Handler) tinybus.Handler {
	return func(ctx context.Context, job tinybus.Job) error {
		rec.Record(Event{
			Kind:     EventClaimed,
			JobID:    job.ID,
			Queue:    job.Queue,
			Attempts: job.Attempts,
			WorkerID: workerID,
		})
		err := inner(ctx, job)
		if err != nil {
			kind := EventFailed
			if job.Attempts >= job.MaxAttempts {
				kind = EventDead
			}
			rec.Record(Event{
				Kind:     kind,
				JobID:    job.ID,
				Queue:    job.Queue,
				Attempts: job.Attempts,
				WorkerID: workerID,
				Detail:   err.Error(),
			})
			return err
		}
		rec.Record(Event{
			Kind:     EventCompleted,
			JobID:    job.ID,
			Queue:    job.Queue,
			Attempts: job.Attempts,
			WorkerID: workerID,
		})
		return nil
	}
}
