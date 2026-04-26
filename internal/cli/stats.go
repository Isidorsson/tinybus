package cli

import (
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/Isidorsson/tinybus/pkg/tinybus"
)

// Stats runs `tinybus stats` — prints per-queue counts as a table.
func Stats(args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
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

	stats, err := q.Stats(ctx)
	if err != nil {
		Die("stats", err)
	}
	if len(stats) == 0 {
		fmt.Println("(no jobs)")
		return
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "QUEUE\tREADY\tDELAYED\tIN-FLIGHT\tDEAD")
	for _, s := range stats {
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\n", s.Queue, s.Ready, s.Delayed, s.InFlight, s.Dead)
	}
	_ = w.Flush()
}
