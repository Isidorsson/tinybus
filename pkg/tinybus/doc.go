// Package tinybus is a durable job queue backed by Postgres.
//
// tinybus uses a single jobs table and SELECT ... FOR UPDATE SKIP LOCKED
// to give each enqueued job to exactly one worker, even when multiple
// workers compete for the same row. There is no broker, no Redis, no
// AMQP — just Postgres.
//
// # Quick start
//
//	q, err := tinybus.New(ctx, tinybus.WithDSN(os.Getenv("DATABASE_URL")))
//	if err != nil { return err }
//	defer q.Close()
//
//	// Producer
//	id, err := q.Enqueue(ctx, "email", []byte(`{"to":"a@b.com"}`))
//
//	// Worker
//	err = q.Process(ctx, "email", func(ctx context.Context, job tinybus.Job) error {
//	    return sendEmail(job.Payload)
//	})
//
// # Why Postgres
//
// "Just use Postgres" is the right answer until your queue is doing
// thousands of jobs/second. tinybus targets the 99% of workloads that
// fit comfortably under that ceiling and benefit from not running a
// second piece of infrastructure.
package tinybus
