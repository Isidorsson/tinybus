package cli

import (
	"flag"
	"fmt"
	"os"

	"github.com/Isidorsson/tinybus/pkg/tinybus"
)

// Migrate runs `tinybus migrate <up|down>`.
func Migrate(args []string) {
	fs := flag.NewFlagSet("migrate", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: tinybus migrate <up|down>")
	}
	if err := fs.Parse(args); err != nil {
		Die("parse flags", err)
	}
	if fs.NArg() != 1 {
		fs.Usage()
		os.Exit(2)
	}
	dir := fs.Arg(0)

	ctx, cancel := SignalContext()
	defer cancel()

	pool, err := OpenPool(ctx)
	if err != nil {
		Die("open pool", err)
	}
	defer pool.Close()

	switch dir {
	case "up":
		if err := tinybus.Migrate(ctx, pool, tinybus.Up); err != nil {
			Die("migrate up", err)
		}
		fmt.Println("tinybus: migrations applied")
	case "down":
		if err := tinybus.Migrate(ctx, pool, tinybus.Down); err != nil {
			Die("migrate down", err)
		}
		fmt.Println("tinybus: migrations rolled back")
	default:
		fs.Usage()
		os.Exit(2)
	}
}
