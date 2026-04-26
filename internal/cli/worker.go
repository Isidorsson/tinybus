package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/Isidorsson/tinybus/pkg/tinybus"
)

// Worker runs `tinybus worker --queue=X [--concurrency=N] [--http-addr=:8080]`.
//
// The default handler logs the job and succeeds. Real users embed
// pkg/tinybus and supply their own handler; the CLI worker is for the
// docker-compose demo and Railway ergonomics.
//
// HTTP server: if --http-addr or $PORT is set, exposes /healthz and
// /stats. Railway healthchecks the worker via /healthz.
func Worker(args []string) {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	queue := fs.String("queue", "default", "queue name")
	concurrency := fs.Int("concurrency", 1, "in-flight jobs per worker")
	httpAddr := fs.String("http-addr", "", "if set, expose /healthz and /stats on this address (default: $PORT or none)")
	leaseDuration := fs.Duration("lease", 5*time.Minute, "how long a claim is valid before reclaim")
	pollInterval := fs.Duration("poll", time.Second, "sleep between empty fetches")
	failPct := fs.Int("fail-pct", 0, "for demo only: synthetic failure rate 0..100")
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
	q, err := tinybus.New(ctx,
		tinybus.WithPool(pool),
		tinybus.WithLogger(log),
		tinybus.WithConcurrency(*concurrency),
		tinybus.WithLeaseDuration(*leaseDuration),
		tinybus.WithPollInterval(*pollInterval),
	)
	if err != nil {
		Die("new queue", err)
	}
	defer q.Close()

	addr := resolveHTTPAddr(*httpAddr)
	if addr != "" {
		go runHTTPServer(ctx, addr, q, log)
	}

	handler := demoHandler(log, *failPct)

	log.Info("tinybus: starting worker",
		slog.String("queue", *queue),
		slog.Int("concurrency", *concurrency),
		slog.Duration("lease", *leaseDuration),
		slog.String("http_addr", addr),
	)

	if err := q.Process(ctx, *queue, handler); err != nil && !errors.Is(err, context.Canceled) {
		Die("process", err)
	}
	log.Info("tinybus: worker stopped")
}

// demoHandler logs the job and either succeeds or fails based on
// failPct. Real applications supply their own.
func demoHandler(log *slog.Logger, failPct int) tinybus.Handler {
	return func(ctx context.Context, job tinybus.Job) error {
		log.Info("tinybus: handling job",
			slog.Int64("id", job.ID),
			slog.String("queue", job.Queue),
			slog.Int("attempts", job.Attempts),
			slog.String("payload", string(job.Payload)),
		)
		if failPct > 0 && (int(time.Now().UnixNano())%100) < failPct {
			return fmt.Errorf("synthetic failure (fail-pct=%d)", failPct)
		}
		return nil
	}
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

func runHTTPServer(ctx context.Context, addr string, q *tinybus.Queue, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		stats, err := q.Stats(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(stats)
	})

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
