DROP INDEX outbox_pending;
CREATE INDEX outbox_pending ON outbox_events (next_attempt_at) WHERE published_at IS NULL;
DROP INDEX outbox_events_seq_uq;
ALTER TABLE outbox_events DROP COLUMN seq;
