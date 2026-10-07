DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS inbox_messages;
DROP TABLE IF EXISTS wallet_ledger_entries;
DROP TABLE IF EXISTS wager_transactions;
DROP TABLE IF EXISTS wallets;
DROP FUNCTION IF EXISTS outbox_snapshot_immutable();
DROP FUNCTION IF EXISTS forbid_terminal_update();
DROP FUNCTION IF EXISTS forbid_mutation();
