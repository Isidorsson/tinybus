-- 0001_init.down.sql
-- Rollback for 0001_init. Drops everything tinybus owns.

DROP INDEX IF EXISTS idx_jobs_in_flight;
DROP INDEX IF EXISTS idx_jobs_dead;
DROP INDEX IF EXISTS idx_jobs_ready;
DROP TABLE IF EXISTS jobs;
