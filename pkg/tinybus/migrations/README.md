# Migrations

Versioned, paired up/down SQL files. The CLI's `migrate up` / `migrate down`
subcommand (step 8 of the build plan) reads these files via `embed.FS` and
applies them inside a transaction, tracking applied versions in a
`tinybus_migrations` ledger table.

## File naming

`NNNN_description.{up,down}.sql` — four-digit zero-padded version, snake_case
description, paired up and down files. Versions are applied in numerical order.

## Idempotency

Up migrations should be idempotent where reasonable (`CREATE TABLE IF NOT
EXISTS`, `CREATE INDEX IF NOT EXISTS`) so a partial failure is safely retryable.
Down migrations should be idempotent always (`DROP ... IF EXISTS`).

## Adding a migration

```
migrations/
├── 0001_init.up.sql
├── 0001_init.down.sql
├── 0002_<your_description>.up.sql       # next version
└── 0002_<your_description>.down.sql
```
