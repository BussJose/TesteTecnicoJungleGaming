-- Carteiras: saldo em centavos (BIGINT), nunca negativo.
CREATE TABLE wallets (
    id             UUID PRIMARY KEY,
    player_id      UUID        NOT NULL,
    currency       CHAR(3)     NOT NULL,
    balance_minor  BIGINT      NOT NULL DEFAULT 0,
    version        BIGINT      NOT NULL DEFAULT 1,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT wallets_balance_non_negative CHECK (balance_minor >= 0),
    CONSTRAINT wallets_version_positive     CHECK (version >= 1),
    CONSTRAINT wallets_player_currency_uq   UNIQUE (player_id, currency)
);

-- Transações de aposta (externas) e abertura de carteira (internas).
CREATE TABLE wager_transactions (
    id                                UUID PRIMARY KEY,
    origin                            TEXT        NOT NULL,
    provider_id                       TEXT,
    external_transaction_id           TEXT,
    idempotency_key                   TEXT,
    payload_hash                      TEXT,
    wallet_id                         UUID        NOT NULL REFERENCES wallets(id),
    player_id                         UUID        NOT NULL,
    round_id                          TEXT,
    game_id                           TEXT,
    kind                              TEXT        NOT NULL,
    amount_minor                      BIGINT      NOT NULL,
    currency                          CHAR(3)     NOT NULL,
    reference_external_transaction_id TEXT,
    reference_transaction_id          UUID        REFERENCES wager_transactions(id),
    status                            TEXT        NOT NULL DEFAULT 'PENDING',
    failure_code                      TEXT,
    result_balance_minor              BIGINT,
    attempts                          INT         NOT NULL DEFAULT 0,
    next_attempt_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at                        TIMESTAMPTZ,
    locked_by                         TEXT,
    locked_until                      TIMESTAMPTZ,
    created_at                        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                        TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at                      TIMESTAMPTZ,

    CONSTRAINT wt_origin_valid CHECK (origin IN ('EXTERNAL', 'INTERNAL')),
    CONSTRAINT wt_kind_valid   CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
    CONSTRAINT wt_status_valid CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),

    -- Interna (OPENING) x externa: campos aplicáveis a cada origem.
    CONSTRAINT wt_origin_shape CHECK (
        (origin = 'INTERNAL' AND kind = 'OPENING'
            AND provider_id IS NULL AND external_transaction_id IS NULL
            AND idempotency_key IS NULL AND payload_hash IS NULL
            AND round_id IS NULL AND game_id IS NULL
            AND reference_external_transaction_id IS NULL)
        OR
        (origin = 'EXTERNAL' AND kind <> 'OPENING'
            AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL
            AND idempotency_key IS NOT NULL AND payload_hash IS NOT NULL
            AND round_id IS NOT NULL AND game_id IS NOT NULL)
    ),
    -- LOSS tem valor zero; todos os demais, valor positivo.
    CONSTRAINT wt_amount_by_kind CHECK (
        (kind = 'LOSS' AND amount_minor = 0) OR (kind <> 'LOSS' AND amount_minor > 0)
    ),
    -- REFUND e ROLLBACK exigem referência externa.
    CONSTRAINT wt_reference_required CHECK (
        kind NOT IN ('REFUND','ROLLBACK') OR reference_external_transaction_id IS NOT NULL
    ),
    -- Rejeição ou falha exige código estável.
    CONSTRAINT wt_failure_code_required CHECK (
        status NOT IN ('REJECTED','FAILED') OR failure_code IS NOT NULL
    )
);

-- Idempotência persistente: mesma chave ou mesmo ID externo não entram duas vezes.
CREATE UNIQUE INDEX wt_provider_external_uq ON wager_transactions (provider_id, external_transaction_id);
CREATE UNIQUE INDEX wt_provider_idem_uq     ON wager_transactions (provider_id, idempotency_key);

-- Só uma abertura por carteira.
CREATE UNIQUE INDEX wt_one_opening_per_wallet ON wager_transactions (wallet_id) WHERE kind = 'OPENING';

-- No máximo uma reversão (REFUND ou ROLLBACK) bem-sucedida por transação referenciada.
CREATE UNIQUE INDEX wt_one_reversal_per_reference
    ON wager_transactions (reference_transaction_id)
    WHERE kind IN ('REFUND','ROLLBACK') AND status = 'PROCESSED';

-- Busca de trabalho pendente pelos workers.
CREATE INDEX wt_pending_work ON wager_transactions (next_attempt_at)
    WHERE status IN ('PENDING','PENDING_REFERENCE');

-- Ledger append-only. "seq" dá ordenação estável para a paginação por cursor.
CREATE TABLE wallet_ledger_entries (
    seq                  BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    id                   UUID PRIMARY KEY,
    wallet_id            UUID        NOT NULL REFERENCES wallets(id),
    transaction_id       UUID        NOT NULL REFERENCES wager_transactions(id),
    direction            TEXT        NOT NULL,
    amount_minor         BIGINT      NOT NULL,
    balance_before_minor BIGINT      NOT NULL,
    balance_after_minor  BIGINT      NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT wle_direction_valid CHECK (direction IN ('DEBIT','CREDIT')),
    CONSTRAINT wle_amount_positive CHECK (amount_minor > 0),
    CONSTRAINT wle_balances_non_negative CHECK (balance_before_minor >= 0 AND balance_after_minor >= 0),
    CONSTRAINT wle_math CHECK (
        (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor) OR
        (direction = 'DEBIT'  AND balance_after_minor = balance_before_minor - amount_minor)
    ),
    CONSTRAINT wle_wallet_tx_uq UNIQUE (wallet_id, transaction_id)
);
CREATE INDEX wle_wallet_seq ON wallet_ledger_entries (wallet_id, seq);

-- Inbox: deduplicação durável de mensagens consumidas.
CREATE TABLE inbox_messages (
    consumer_name TEXT        NOT NULL,
    message_id    TEXT        NOT NULL,
    payload_hash  TEXT        NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ,
    PRIMARY KEY (consumer_name, message_id)
);

-- Outbox transacional. locked_* permitem que outro publisher assuma trabalho abandonado.
CREATE TABLE outbox_events (
    event_id        UUID PRIMARY KEY,
    aggregate_id    UUID        NOT NULL,
    event_type      TEXT        NOT NULL,
    version         INT         NOT NULL,
    correlation_id  TEXT        NOT NULL,
    causation_id    TEXT,
    payload         JSONB       NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    attempts        INT         NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by       TEXT,
    locked_until    TIMESTAMPTZ,
    published_at    TIMESTAMPTZ
);
CREATE INDEX outbox_pending ON outbox_events (next_attempt_at) WHERE published_at IS NULL;

-- ===== Proteções por trigger =====

CREATE FUNCTION forbid_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% on % is not allowed (append-only)', TG_OP, TG_TABLE_NAME
        USING ERRCODE = 'integrity_constraint_violation';
END $$;

CREATE TRIGGER wle_no_update_delete BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
CREATE TRIGGER wle_no_truncate BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_mutation();
CREATE TRIGGER wt_no_delete BEFORE DELETE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION forbid_mutation();

-- Transação em estado terminal não sofre novas transições.
CREATE FUNCTION forbid_terminal_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status IN ('PROCESSED','REJECTED','FAILED') THEN
        RAISE EXCEPTION 'transaction % is in terminal state %', OLD.id, OLD.status
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER wt_terminal_immutable BEFORE UPDATE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION forbid_terminal_update();

-- Outbox: o snapshot do evento não pode ser alterado; só o controle de publicação muda.
CREATE FUNCTION outbox_snapshot_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.event_id <> OLD.event_id OR NEW.aggregate_id <> OLD.aggregate_id
       OR NEW.event_type <> OLD.event_type OR NEW.version <> OLD.version
       OR NEW.payload <> OLD.payload OR NEW.occurred_at <> OLD.occurred_at THEN
        RAISE EXCEPTION 'outbox event % snapshot is immutable', OLD.event_id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER outbox_snapshot_guard BEFORE UPDATE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION outbox_snapshot_immutable();
