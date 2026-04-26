package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/Isidorsson/tinybus/pkg/tinybus"
)

// Worker runs `tinybus worker --queue=X [--concurrency=N] [--http-addr=:8080]`.
//
// The default handler logs the job and either succeeds or fails based on
// `--fail-pct` and the payload (a payload containing `"fail":true` always
// fails — used by the dashboard's "Enqueue failing" button).
//
// HTTP: if --http-addr or $PORT is set, the worker exposes:
//   - GET  /          — embedded live dashboard (HTML)
//   - GET  /healthz   — Railway-style health check
//   - GET  /stats     — JSON queue counts
//   - GET  /events    — JSON activity feed for the dashboard
//   - POST /enqueue   — used by the dashboard buttons
//   - POST /reset     — clears dead jobs (used by the dashboard)
func Worker(args []string) {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	queue := fs.String("queue", "default", "queue name")
	concurrency := fs.Int("concurrency", 1, "in-flight jobs per worker")
	httpAddr := fs.String("http-addr", "", "if set, expose dashboard + JSON endpoints (default: $PORT or none)")
	leaseDuration := fs.Duration("lease", 5*time.Minute, "how long a claim is valid before reclaim")
	pollInterval := fs.Duration("poll", time.Second, "sleep between empty fetches")
	failPct := fs.Int("fail-pct", 0, "synthetic failure rate 0..100 (demo)")
	delay := fs.Duration("handler-delay", 400*time.Millisecond, "artificial handler runtime so demo transitions are visible")
	autoMigrate := fs.Bool("auto-migrate", true, "apply pending migrations at startup (idempotent; safe for solo deploys, set false if a separate migrator owns the schema)")
	if err := fs.Parse(args); err != nil {
		Die("parse flags", err)
	}

	ctx, cancel := SignalContext()
	defer cancel()

	pool, err := OpenPool(ctx)
	if err != nil {
		Die("open pool", err)
	}
	defer pool.Close()

	log := Logger()

	// Apply any pending migrations before workers begin claiming. This
	// makes single-replica deploys (Railway, fly.io, a bare VPS) work
	// out of the box without a separate migrator step. Disable with
	// --auto-migrate=false when a dedicated migrator service owns the
	// schema in multi-replica setups.
	if *autoMigrate {
		if err := tinybus.Migrate(ctx, pool, tinybus.Up); err != nil {
			Die("auto-migrate", err)
		}
		log.Info("tinybus: migrations applied at startup")
	}

	workerID := shortWorkerID()

	q, err := tinybus.New(ctx,
		tinybus.WithPool(pool),
		tinybus.WithLogger(log),
		tinybus.WithConcurrency(*concurrency),
		tinybus.WithLeaseDuration(*leaseDuration),
		tinybus.WithPollInterval(*pollInterval),
		tinybus.WithWorkerID(workerID),
	)
	if err != nil {
		Die("new queue", err)
	}
	defer q.Close()

	rec := NewRecorder(200)

	addr := resolveHTTPAddr(*httpAddr)
	if addr != "" {
		go runHTTPServer(ctx, addr, q, rec, *queue, log)
	}

	handler := recordingHandler(rec, workerID, demoHandler(log, *failPct, *delay))

	log.Info("tinybus: starting worker",
		slog.String("queue", *queue),
		slog.Int("concurrency", *concurrency),
		slog.Duration("lease", *leaseDuration),
		slog.Int("fail_pct", *failPct),
		slog.String("worker_id", workerID),
		slog.String("http_addr", addr),
	)

	if err := q.Process(ctx, *queue, handler); err != nil && !errors.Is(err, context.Canceled) {
		Die("process", err)
	}
	log.Info("tinybus: worker stopped")
}

// demoHandler is the default handler used by the CLI worker. Real
// applications embed pkg/tinybus and supply their own handler.
func demoHandler(log *slog.Logger, failPct int, delay time.Duration) tinybus.Handler {
	return func(ctx context.Context, job tinybus.Job) error {
		// Visible-transition delay. Without this, jobs flash through
		// states faster than the dashboard polling interval.
		if delay > 0 {
			t := time.NewTimer(delay)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
			}
		}
		log.Info("tinybus: handling job",
			slog.Int64("id", job.ID),
			slog.String("queue", job.Queue),
			slog.Int("attempts", job.Attempts),
			slog.String("payload", string(job.Payload)),
		)
		// Payload-driven failure: the dashboard's "enqueue failing"
		// button writes `{"fail":true}`. Used to deterministically
		// demonstrate the retry → dead transition.
		if isFailingPayload(job.Payload) {
			return fmt.Errorf("intentional demo failure (attempt %d/%d)", job.Attempts, job.MaxAttempts)
		}
		// Synthetic random failure.
		if failPct > 0 && (int(time.Now().UnixNano()/1000)%100) < failPct {
			return fmt.Errorf("synthetic failure (fail-pct=%d)", failPct)
		}
		return nil
	}
}

// isFailingPayload returns true if the payload looks like JSON tagged
// with "fail":true. Cheap substring match on a known synthetic format.
func isFailingPayload(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	if bytes.Contains(payload, []byte(`"fail":true`)) {
		return true
	}
	// Tolerate one space after the colon.
	if bytes.Contains(payload, []byte(`"fail": true`)) {
		return true
	}
	// Defensive: anything else with a "fail" key set to true.
	var probe struct {
		Fail bool `json:"fail"`
	}
	_ = json.Unmarshal(payload, &probe)
	return probe.Fail
}

// resolveHTTPAddr prefers explicit flag, then $PORT (Railway), else "".
func resolveHTTPAddr(flagVal string) string {
	if flagVal != "" {
		return flagVal
	}
	if port := os.Getenv("PORT"); port != "" {
		return ":" + port
	}
	return ""
}

// runHTTPServer wires up healthz, stats, and the dashboard routes, and
// blocks until ctx is cancelled.
func runHTTPServer(ctx context.Context, addr string, q *tinybus.Queue, rec *Recorder, queue string, log *slog.Logger) {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		stats, err := q.Stats(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(stats); err != nil {
			log.Warn("tinybus: write stats response", slog.String("err", err.Error()))
		}
	})

	dash := newDashboard(q, rec, log, queue)
	dash.routes(mux)

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	log.Info("tinybus: http listening", slog.String("addr", addr))
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("tinybus: http server", slog.String("err", err.Error()))
	}
}

// shortWorkerID returns a stable 4-character worker identifier suitable
// for the activity feed. Prefers $RAILWAY_REPLICA_ID, falls back to a
// hash of host+pid so two workers on the same machine differ.
func shortWorkerID() string {
	if v := os.Getenv("RAILWAY_REPLICA_ID"); len(v) >= 4 {
		return v[:4]
	}
	host, _ := os.Hostname()
	h := fnv.New32a()
	fmt.Fprintf(h, "%s-%d", host, os.Getpid())
	return fmt.Sprintf("%04x", h.Sum32()&0xffff)
}
