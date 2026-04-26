package cli

import (
	"flag"
	"fmt"
	"log/slog"
	"time"

	"github.com/Isidorsson/tinybus/pkg/tinybus"
)

// Producer runs `tinybus producer --queue=X --interval=2s --payload=...`.
// Used by docker-compose to drive the demo without needing a shell.
func Producer(args []string) {
	fs := flag.NewFlagSet("producer", flag.ExitOnError)
	queue := fs.String("queue", "default", "queue name")
	interval := fs.Duration("interval", 2*time.Second, "delay between enqueues")
	payload := fs.String("payload", `{"hello":"world"}`, "raw payload bytes")
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
	q, err := tinybus.New(ctx, tinybus.WithPool(pool), tinybus.WithLogger(log))
	if err != nil {
		Die("new queue", err)
	}
	defer q.Close()

	log.Info("tinybus: producer starting", slog.String("queue", *queue), slog.Duration("interval", *interval))

	t := time.NewTicker(*interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			id, err := q.Enqueue(ctx, *queue, []byte(*payload))
			if err != nil {
				log.Warn("tinybus: producer enqueue failed", slog.String("err", err.Error()))
				continue
			}
			fmt.Printf("tinybus: enqueued id=%d\n", id)
		}
	}
}
