-- Ordem de gravação da outbox: os eventos de uma carteira são publicados na
-- ordem em que foram gravados (occurred_at pode empatar).
ALTER TABLE outbox_events ADD COLUMN seq BIGINT GENERATED ALWAYS AS IDENTITY;
CREATE UNIQUE INDEX outbox_events_seq_uq ON outbox_events (seq);
DROP INDEX outbox_pending;
CREATE INDEX outbox_pending ON outbox_events (seq) WHERE published_at IS NULL;
