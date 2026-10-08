# Wager Wallet Service (backend-challenge-go)

Serviço em Go que mantém **carteiras** de jogadores e processa transações de aposta
(`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`) com **dinheiro exato**, **ledger append-only**,
**idempotência persistente**, **inbox/outbox transacionais** e filas **SQS FIFO**.

Stack: Go · Uber Fx · PostgreSQL (pgx, SQL explícito) · AWS SQS FIFO (LocalStack) ·
Keycloak (OAuth2/OIDC `client_credentials`) · Docker Compose · golang-migrate.

O desenho e as decisões estão em [`ARCHITECTURE.md`](ARCHITECTURE.md).

## 1. Pré-requisitos

| Para quê | O que precisa |
|---|---|
| Subir tudo | Docker Desktop (com `docker compose`) |
| Testes no seu computador | Go 1.22 ou mais novo |
| Testes com `-race` | Docker (o `-race` do Go exige cgo; use o serviço `test` do compose) |

## 2. Subir o ambiente

```bash
docker compose up --build
```

Sobe, nesta ordem: PostgreSQL → `migrate` (aplica as migrations e termina) → Keycloak
(importa o realm `wager`) → LocalStack (cria as filas via `scripts/localstack/init-sqs.sh`) → API.
A API está pronta quando o log mostra `http server listening`.

| Serviço | Endereço |
|---|---|
| API | http://localhost:8080 |
| Keycloak | http://localhost:8081 (admin / admin) |
| LocalStack (SQS) | http://localhost:4566 |
| PostgreSQL | localhost:5432 (usuário/senha/banco: `wager`) |

Três instâncias da API ao mesmo tempo (portas 8080 a 8082):

```bash
docker compose up --build --scale api=3
```

Parar e apagar tudo (inclusive o banco): `docker compose down -v`.

## 3. Variáveis de ambiente

O arquivo [`.env.example`](.env.example) lista todas; o compose já o usa como base.

| Variável | Padrão | Descrição |
|---|---|---|
| `HTTP_ADDR` | `:8080` | endereço HTTP |
| `DATABASE_URL` | `postgres://wager:wager@localhost:5432/wager?sslmode=disable` | PostgreSQL |
| `DB_MAX_CONNS` | `20` | tamanho do pool |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` (logs em JSON) |
| `INSTANCE_ID` | nome da máquina | aparece nos logs e nos *leases* |
| `AWS_REGION` | `us-east-1` | região SQS |
| `AWS_ENDPOINT_URL` | vazio | `http://localhost:4566` para LocalStack |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | `test` / `test` | credenciais (LocalStack aceita qualquer uma) |
| `SQS_WAGER_QUEUE_URL` | `…/wager-transactions.fifo` | fila de **entrada** (transações) |
| `SQS_EVENTS_QUEUE_URL` | `…/wager-events.fifo` | fila de **saída** (eventos da outbox) |
| `CONSUMER_WORKERS` | `4` | consumidores paralelos por instância |
| `OUTBOX_POLL_INTERVAL` / `OUTBOX_BATCH` | `200ms` / `50` | publicador da outbox |
| `KEYCLOAK_URL`, `KEYCLOAK_REALM` | `http://localhost:8081`, `wager` | de onde vêm as chaves (JWKS) |
| `KEYCLOAK_ISSUER` | `<url>/realms/<realm>` | valor exigido na claim `iss` |
| `KEYCLOAK_JWKS_URL` | `<issuer>/protocol/openid-connect/certs` | URL das chaves públicas |
| `KEYCLOAK_AUDIENCE` | `wager-api` | claim `aud` exigida |
| `REFERENCE_TTL` | `5m` | quanto tempo um estorno/rollback espera a transação referenciada |
| `REFERENCE_MAX_ATTEMPTS` | `10` | tentativas máximas |
| `REFERENCE_BACKOFF_BASE` / `_MAX` | `1s` / `30s` | backoff exponencial |
| `PENDING_POLL_INTERVAL` | `1s` | ciclo do worker de referências pendentes |

> Dentro do Docker o `KEYCLOAK_ISSUER` (o que os tokens dizem, `localhost:8081`) é diferente do endereço
> usado para buscar as chaves (`keycloak:8080`). Por isso são duas variáveis.

## 4. Filas SQS (inicialização)

Criadas automaticamente pelo LocalStack ao ficar pronto, por
[`scripts/localstack/init-sqs.sh`](scripts/localstack/init-sqs.sh):

| Fila | Papel |
|---|---|
| `wager-transactions.fifo` | entrada; `RedrivePolicy` com `maxReceiveCount=5` para a DLQ |
| `wager-transactions-dlq.fifo` | mensagens que falharam 5 vezes |
| `wager-events.fifo` | saída: eventos publicados pela outbox |

Recriar manualmente (se precisar): `docker compose exec localstack /etc/localstack/init/ready.d/init-sqs.sh`.

Enviar uma transação pela fila (a carteira precisa existir; veja o §6 para criá-la). O corpo é o envelope
`WagerTransactionRequested`; o `messageId` do envelope é a identidade da mensagem na inbox:

```bash
docker compose exec localstack awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id <WALLET_ID> --message-deduplication-id dedup-1 \
  --message-body '{"messageId":"msg-q1","type":"WagerTransactionRequested","occurredAt":"2026-10-08T12:00:00Z","data":{"idempotencyKey":"k-q1","providerId":"provider-a","externalTransactionId":"bet-q1","playerId":"<PLAYER_ID>","walletId":"<WALLET_ID>","roundId":"r1","gameId":"g1","kind":"BET","money":{"amount":"10.00","currency":"BRL"}}}'
```

Regras de consumo: sucesso, rejeição de negócio definitiva (`REJECTED`) e reentrega reconhecida pela inbox apagam a
mensagem; mensagem inválida ou conflitante (mesmo `messageId` com conteúdo diferente) volta à fila na hora e, após 5
recebimentos, a própria fila a move para a DLQ; falha temporária (banco fora do ar, carteira ainda inexistente) mantém a
mensagem invisível por 2 s, 4 s, 8 s… (até 60 s) e, esgotadas as 5 tentativas, ela também vai para a DLQ.
`MessageGroupId` = id da carteira (operações da mesma carteira em ordem) e `MessageDeduplicationId` = o que o produtor
definir (a fila descarta repetições por 5 minutos; a inbox cobre o resto).

Ver os eventos publicados:

```bash
docker compose exec localstack awslocal sqs receive-message \
  --queue-url http://localhost:4566/000000000000/wager-events.fifo --max-number-of-messages 10
```

Ver mensagens que foram para a DLQ:

```bash
docker compose exec localstack awslocal sqs receive-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions-dlq.fifo
```

## 5. Migrations (aplicar e reverter)

Ficam em [`migrations/`](migrations) (formato golang-migrate, `NNNNNN_nome.up.sql` / `.down.sql`) e são
aplicadas automaticamente pelo serviço `migrate` em todo `docker compose up`.

```bash
docker compose run --rm migrate up               # aplica todas as pendentes
docker compose run --rm migrate down 1           # reverte a última
docker compose run --rm migrate version          # mostra a versão atual
docker compose run --rm migrate goto 1           # vai para a versão 1
```

(`migrate` já recebe `-path` e `-database` no compose; os argumentos acima são os subcomandos.)

## 6. Autenticação e exemplos de chamadas

Keycloak, realm `wager`, fluxo `client_credentials`:

| Client | Secret | Perfil | Pode chamar |
|---|---|---|---|
| `wager-internal` | `internal-secret` | role `wallet-internal` | `/wallets/**` |
| `provider-a` | `provider-a-secret` | role `wager-provider`, `provider_id=provider-a` | `/wagering/**` |
| `provider-b` | `provider-b-secret` | role `wager-provider`, `provider_id=provider-b` | `/wagering/**` |

A API valida assinatura RS256 (JWKS), `iss`, `aud` e validade. Um provedor só enxerga as próprias transações.

Teste rápido completo (PowerShell): `powershell -ExecutionPolicy Bypass -File scripts/smoke.ps1`

### Com curl (bash)

```bash
KC=http://localhost:8081/realms/wager/protocol/openid-connect/token
API=http://localhost:8080

INTERNAL=$(curl -s -d grant_type=client_credentials -d client_id=wager-internal -d client_secret=internal-secret $KC | jq -r .access_token)
PROVIDER=$(curl -s -d grant_type=client_credentials -d client_id=provider-a -d client_secret=provider-a-secret $KC | jq -r .access_token)

# 1) abrir carteira com 100.00
curl -s -X POST $API/wallets -H "Authorization: Bearer $INTERNAL" -H 'Content-Type: application/json' \
  -d '{"playerId":"8d7b172a-dfdd-43c3-8bff-57cf3703e20b","initialBalance":{"amount":"100.00","currency":"BRL"}}'

# 2) aposta de 30.00 (repita a chamada: devolve o mesmo resultado, "idempotentReplay": true)
curl -s -X POST $API/wagering/transactions -H "Authorization: Bearer $PROVIDER" \
  -H 'Idempotency-Key: bet-1' -H 'Content-Type: application/json' \
  -d '{"externalTransactionId":"bet-1","playerId":"8d7b172a-dfdd-43c3-8bff-57cf3703e20b","walletId":"<WALLET_ID>","roundId":"r1","gameId":"g1","kind":"BET","money":{"amount":"30.00","currency":"BRL"}}'

# 3) estorno que chega antes da aposta (referência ainda inexistente) -> 202 PENDING_REFERENCE; conclui sozinho quando a aposta chegar
curl -s -X POST $API/wagering/transactions -H "Authorization: Bearer $PROVIDER" \
  -H 'Idempotency-Key: refund-1' -H 'Content-Type: application/json' \
  -d '{"externalTransactionId":"refund-1","playerId":"8d7b172a-dfdd-43c3-8bff-57cf3703e20b","walletId":"<WALLET_ID>","roundId":"r1","gameId":"g1","kind":"REFUND","referenceExternalTransactionId":"bet-2","money":{"amount":"10.00","currency":"BRL"}}'

# 4) consultas e conciliação
curl -s $API/providers/provider-a/wagering/transactions/bet-1 -H "Authorization: Bearer $PROVIDER"   # por id externo
curl -s $API/wagering/transactions/<TRANSACTION_ID> -H "Authorization: Bearer $PROVIDER"            # por id interno
curl -s $API/wallets/<WALLET_ID>          -H "Authorization: Bearer $INTERNAL"
curl -s "$API/wallets/<WALLET_ID>/ledger?limit=50" -H "Authorization: Bearer $INTERNAL"
curl -s -X POST $API/wallets/<WALLET_ID>/reconciliation -H "Authorization: Bearer $INTERNAL"
```

### Endpoints

| Método e caminho | Autorização | Descrição |
|---|---|---|
| `GET /health/live`, `GET /health/ready` | pública | liveness; readiness (Postgres e as duas filas) |
| `GET /metrics` | pública | métricas em formato Prometheus |
| `POST /wallets` | `wallet-internal` | abre carteira (`playerId`, `initialBalance` em dinheiro) |
| `GET /wallets/{id}` | `wallet-internal` | saldo e versão |
| `GET /wallets/{id}/ledger` | `wallet-internal` | lançamentos (paginação por `cursor`) |
| `POST /wallets/{id}/reconciliation` | `wallet-internal` | reconstrói o saldo pelo ledger e compara: `storedBalance`, `calculatedBalance`, `difference` (armazenado − reconstruído), `consistent`, `checkedEntries`. Não altera saldos; divergência vai para o log e para a métrica `reconciliation_divergences_total` |
| `POST /wagering/transactions` | `wager-provider` | processa uma operação (header `Idempotency-Key` obrigatório; `providerId` no corpo é opcional e, se vier, deve ser o do token) |
| `GET /wagering/transactions/{transactionId}` | `wager-provider` | consulta por id interno (só do próprio provedor) |
| `GET /providers/{providerId}/wagering/transactions/{externalId}` | `wager-provider` | consulta por id externo; `providerId` deve ser o do token (senão 403) |

Valores monetários são sempre texto: `{"amount":"25.00","currency":"BRL"}`.

Status HTTP: `200` processada · `202` aguardando a transação referenciada · `422` rejeitada
(`INSUFFICIENT_FUNDS`, `REFERENCE_MISMATCH`, …; o corpo traz `failureCode`) · `400` entrada inválida (não é gravada) ·
`401` sem token ou token inválido/expirado · `403` papel insuficiente, `providerId` diferente do token ou provedor de outro caminho · `404` · `409` mesma chave com conteúdo diferente · `503` falha temporária.

## 7. Testes

```bash
go vet ./...
gofmt -l .                                   # não deve listar nenhum arquivo
go test ./...                                # unitários + suíte em memória (rápido, sem Docker)
```

Testes de **integração** (PostgreSQL e SQS reais, sem mocks) e `-race` — dentro do Docker:

```bash
docker compose run --rm test                 # go test -race -tags integration ./...
```

Ou, no seu computador, com a infraestrutura de pé (`docker compose up -d postgres localstack keycloak`):

```bash
# Linux/macOS
DATABASE_URL='postgres://wager:wager@localhost:5432/wager?sslmode=disable' \
AWS_ENDPOINT_URL=http://localhost:4566 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
KEYCLOAK_URL=http://localhost:8081 \
go test -race -tags integration -count=1 ./...
```
```powershell
# PowerShell (sem -race: exige cgo)
$env:DATABASE_URL="postgres://wager:wager@localhost:5432/wager?sslmode=disable"
$env:AWS_ENDPOINT_URL="http://localhost:4566"; $env:AWS_ACCESS_KEY_ID="test"; $env:AWS_SECRET_ACCESS_KEY="test"
$env:KEYCLOAK_URL="http://localhost:8081"
go test -tags integration -count=1 ./...
```

Testes de integração sem as variáveis correspondentes (`DATABASE_URL`, `AWS_ENDPOINT_URL`, `KEYCLOAK_URL`) são ignorados (`SKIP`); o serviço `test` do compose define todas. Cada teste de integração cria o próprio banco temporário (e filas com nome único), aplica as migrations de
`migrations/` e apaga tudo no fim.

### Onde está cada teste exigido

| Cenário | Teste |
|---|---|
| 50 apostas idênticas em paralelo, várias instâncias | `Parallel50IdenticalBetsAcrossInstances` |
| Duas apostas de 80,00 com saldo 100,00 | `TwoBets80Over100`, `ManyParallelBetsNeverGoNegative` |
| ≥ 3 instâncias reais da API sobre o mesmo banco (HTTP + JWT) | `TestHTTPConcurrencyAcrossInstances` |
| Consumidor interrompido após o commit | `TestConsumerInterruptedAfterCommit` (SQS real) e `TestInterruptedAfterCommit` |
| Duas instâncias publicando a outbox | `OutboxPublishesOnceInOrderWithTwoPublishers`, `TestTwoOutboxPublishers` |
| Refund/rollback antes da referência | `RefundBeforeBetThenResolve`, `PendingReferenceExpires`, `TwoConcurrentPendingResolvers` |
| Mensagem duplicada / veneno → DLQ | `InboxDeduplicatesMessages`, `TestDuplicateMessagesMoveMoneyOnce`, `TestPoisonMessageEndsInDLQ` |
| Reinício (lease expirado, outro publicador assume) | `OutboxLeaseTakeover`, `OutboxRetriesAfterPublishFailure` |
| Proteções do banco (ledger/outbox imutáveis, saldo ≥ 0) | `TestDatabaseGuards` |
| Autenticação e papéis com o **Keycloak real** (sem token, adulterado, papel errado) | `TestKeycloakAuthentication` |
| Isolamento entre provedores (leitura, replay, `providerId` forjado) | `TestKeycloakAuthentication`, `TestProviderIsolation` |
| Mesma operação por HTTP e SQS, inclusive em paralelo | `SameOperationOverHTTPAndSQS` |

As suítes `RunSuite` (pasta `internal/application/wagering/wageringtest`) rodam **as mesmas regras** em memória
e no PostgreSQL real.

## 8. Estrutura

```
cmd/server                 ponto de entrada (Fx)
internal/domain            dinheiro, ids, carteira, transação, ledger, eventos (sem I/O)
internal/application       serviço de aplicação (wagering) e publicador da outbox (outbox)
internal/messaging         consumidor da fila de entrada (inbox)
internal/http              API, middlewares, health
internal/platform          postgres, sqs, auth (JWT/JWKS), metrics
internal/app               montagem com Uber Fx
migrations                 SQL versionado (up/down)
keycloak, scripts          realm importado, init das filas, smoke test
```
