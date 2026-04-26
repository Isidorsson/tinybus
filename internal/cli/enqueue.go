package cli

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/Isidorsson/tinybus/pkg/tinybus"
)

// Enqueue runs `tinybus enqueue --queue=X --payload=Y [--run-in=DURATION] [--max-attempts=N]`.
func Enqueue(args []string) {
	fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
	queue := fs.String("queue", "default", "queue name")
	payload := fs.String("payload", "", "job payload (raw bytes)")
	runIn := fs.Duration("run-in", 0, "delay before the job becomes eligible")
	maxAttempts := fs.Int("max-attempts", 0, "override default max attempts (0 = use default)")
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

	q, err := tinybus.New(ctx, tinybus.WithPool(pool), tinybus.WithLogger(Logger()))
	if err != nil {
		Die("new queue", err)
	}
	defer q.Close()

	var opts []tinybus.EnqueueOption
	if *runIn > 0 {
		opts = append(opts, tinybus.RunIn(*runIn))
	}
	if *maxAttempts > 0 {
		opts = append(opts, tinybus.MaxAttempts(*maxAttempts))
	}

	id, err := q.Enqueue(ctx, *queue, []byte(*payload), opts...)
	if err != nil {
		Die("enqueue", err)
	}
	fmt.Fprintf(os.Stdout, "tinybus: enqueued id=%d queue=%s run_in=%s\n", id, *queue, time.Duration(*runIn))
}
