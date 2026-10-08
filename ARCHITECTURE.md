# Arquitetura

## 1. Visão geral

```
 Provedor ──HTTP+JWT──►  API  ──┐
                                ├─► serviço de aplicação (wagering) ─► PostgreSQL
 Provedor ──SQS FIFO───► Consumidor (inbox) ─┘        │  carteira · transações · ledger
   wager-transactions.fifo  └─ falhou 5x ► DLQ        │  inbox · outbox · idempotência
                                                      ▼
                                   Publicador da outbox ──► wager-events.fifo ──► consumidores externos
 Worker de referências pendentes ─► serviço de aplicação (conclui/expira REFUND/ROLLBACK adiantados)
```

Cada instância do serviço roda **tudo** (API, consumidor, publicador, worker). Não há estado em memória que
importe para a correção: qualquer número de instâncias pode rodar sobre o mesmo banco.

Camadas (dependências apontam para dentro):

| Camada | Pacote | Responsabilidade |
|---|---|---|
| Domínio | `internal/domain/{money,id,wager}` | regras puras, sem I/O: dinheiro, carteira, transação, ledger, eventos |
| Aplicação | `internal/application/{wagering,outbox}` | casos de uso, portas (interfaces) e transação de negócio |
| Adaptadores | `internal/platform/{postgres,sqs,auth,metrics}`, `internal/http`, `internal/messaging` | detalhes de infraestrutura |
| Montagem | `internal/app` (Uber Fx) | cria as dependências e liga o ciclo de vida |

## 2. Dinheiro

`money.Money` guarda **inteiros em unidades menores** (centavos) e a moeda; não existe `float` no caminho.
Entrada e saída são texto (`"25.00"`). A entrada é lida com gramática estrita (`NaN`, `1e3`, `-1.00`, casas
decimais a mais, espaços… são rejeitados). No banco o saldo é `BIGINT` com `CHECK (balance >= 0)`.

## 3. Carteira, ledger e consistência sob concorrência

- `wallets`: única por `(player_id, currency)`; `version` começa em 1 e só aumenta quando o saldo muda
  (`LOSS` não altera saldo, ledger nem versão).
- `wallet_ledger_entries`: **append-only**. Cada lançamento guarda `balance_before` e `balance_after`; triggers
  do banco proíbem `UPDATE`/`DELETE`. A reconciliação compara o saldo da carteira com a soma do ledger.
- **Duas camadas contra saldo negativo/duplicidade**:
  1. *Lock pessimista por carteira*: a transação de negócio começa com `SELECT … FOR UPDATE` da carteira. Duas
     apostas da mesma carteira são serializadas pelo banco, em qualquer instância.
  2. `UPDATE wallets … WHERE version = $esperada` + `CHECK (balance >= 0)` + `UNIQUE (wallet_id, transaction_id)`
     no ledger. Se algo escapasse da camada 1, o banco recusa.
- Nível de isolamento `READ COMMITTED`; a correção vem do lock e das restrições, não do isolamento.
  Erros transitórios (violação de unicidade em corrida, deadlock, falha de serialização) são tentados de novo
  (até 4 vezes) pelo serviço.

Por que lock pessimista e não otimista? Em carteiras “quentes” (muitas apostas por segundo), o otimismo gera
tempestade de conflitos e retentativas; o lock enfileira de forma barata dentro do banco. A checagem de versão
fica como rede de proteção.

## 4. Idempotência persistente

Duas chaves, ambas **por provedor** (um provedor não enxerga nem colide com o outro):

- `(provider_id, idempotency_key)` — header `Idempotency-Key` (ou campo `idempotencyKey` na mensagem);
- `(provider_id, external_transaction_id)`.

Ambas são `UNIQUE` em `wager_transactions`. Cada transação guarda o **hash SHA-256 do JSON canônico** do pedido:
mesma chave + mesmo conteúdo → devolve o resultado original (`idempotentReplay: true`, sem novo efeito);
mesma chave + conteúdo diferente → `409 IDEMPOTENCY_KEY_CONFLICT`. Nada fica só em memória: um reinício, ou outra
instância, responde igual. 50 requisições idênticas em paralelo geram **uma** movimentação.

Entrada inválida (400) **não é gravada**. Rejeições de negócio (ex.: `INSUFFICIENT_FUNDS`) **são gravadas**
(`REJECTED`), de forma que a repetição devolva a mesma resposta.

## 5. Estados e tipos de transação

Tipos: `OPENING` (interno), `BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`.
Estados: `PENDING` → `PROCESSED` | `REJECTED` | `FAILED`; `PENDING_REFERENCE` → `PROCESSED` | `REJECTED`.

- `BET` debita; `WIN` credita; `LOSS` só registra; `REFUND` e `ROLLBACK` exigem `referenceExternalTransactionId`
  e desfazem uma transação já `PROCESSED` (mesma carteira, mesmo valor, uma única vez: `ALREADY_REVERSED`).
  `REFUND` devolve o valor de uma `BET`; `ROLLBACK` aplica a direção oposta à da transação referenciada.
- Códigos de rejeição: `INSUFFICIENT_FUNDS`, `REVERSAL_INSUFFICIENT_FUNDS`, `REFERENCE_NOT_FOUND`,
  `REFERENCE_NOT_PROCESSED`, `REFERENCE_MISMATCH`, `REFERENCE_INVALID_KIND`, `ALREADY_REVERSED`, `WALLET_MISMATCH`.

### Referências pendentes

Se um `REFUND`/`ROLLBACK` chega **antes** da transação que referencia, ele vira `PENDING_REFERENCE`
(HTTP 202, evento `WagerTransactionPendingReference`), com `expires_at` (TTL) e `next_attempt_at`.

- Quando a transação referenciada é processada, os dependentes são “acordados” na mesma transação.
- Um worker (`ResolvePendingReferences`) faz o *claim* com `FOR UPDATE SKIP LOCKED` + *lease* (`locked_until`):
  várias instâncias pegam conjuntos diferentes, e se uma cair o lease expira e outra assume.
- Falhas de resolução usam **backoff exponencial** limitado (`REFERENCE_BACKOFF_BASE`…`_MAX`); passado o TTL ou
  o máximo de tentativas, vira `REJECTED` (`REFERENCE_NOT_FOUND`, ou `REFERENCE_NOT_PROCESSED` se a referência existe mas não foi concluída).

## 6. Inbox e outbox transacionais

### Entrada: SQS → inbox

O consumidor lê `wager-transactions.fifo` e chama `SubmitMessage`. Na **mesma transação PostgreSQL** do
processamento, é inserida a linha em `inbox_messages (consumer_name, message_id)` com o hash do corpo.

| Situação | O que acontece |
|---|---|
| Primeira entrega | processa, grava inbox, `COMMIT`, depois `DeleteMessage` |
| Cai depois do `COMMIT`, antes do `DeleteMessage` | a fila reentrega; a inbox reconhece (`DuplicateMessage`), nada é refeito, a mensagem é apagada |
| Mesmo `MessageId` com corpo diferente | `ErrInboxConflict` → mensagem “veneno” |
| Corpo inválido / conflito de idempotência | “veneno”: `ChangeMessageVisibility(0)`; após `maxReceiveCount=5` a fila a move para a **DLQ** |
| Carteira inexistente, banco fora do ar | não apaga; volta após o *visibility timeout* (e vai à DLQ se persistir) |
| Produtor reenviou o mesmo negócio como mensagem nova | `MessageId` novo, mas a idempotência de negócio devolve o resultado original |

Se o processamento falha, a transação inteira (inclusive a linha da inbox) sofre rollback — uma mensagem nunca
fica “marcada como feita” sem o efeito correspondente.

### Saída: outbox → SQS

Os eventos (`WagerTransactionProcessed`, `WagerTransactionRejected`, `WalletBalanceChanged`,
`WagerTransactionPendingReference`) são gravados em `outbox_events` **na mesma transação** da mudança de estado.
Nada é publicado antes do `COMMIT`: o publicador só enxerga linhas já confirmadas.

Envelope: `eventId, eventType, aggregateId (carteira), correlationId, causationId, occurredAt, version, data`.
O corpo do evento (`payload`) é imutável (trigger no banco); só os campos de controle de publicação mudam.

Publicador (qualquer número de instâncias):

1. `Claim`: `UPDATE … WHERE event_id IN (SELECT … ORDER BY seq LIMIT n FOR UPDATE SKIP LOCKED)`, com *lease*.
2. Só é elegível o evento **mais antigo ainda não publicado de cada carteira** (`NOT EXISTS` evento anterior não
   publicado do mesmo agregado). Isso preserva a ordem por carteira mesmo com vários publicadores.
3. Publica na fila FIFO com `MessageGroupId = aggregateId` e `MessageDeduplicationId = eventId`.
4. `MarkPublished`. Em falha de publicação: `attempts + 1` e `next_attempt_at` com backoff exponencial.

Garantia: **pelo menos uma vez**. Se o processo cair entre o envio e o `MarkPublished`, o evento é reenviado com o
mesmo `eventId` (a fila FIFO descarta repetições dentro de 5 minutos; os consumidores devem ser idempotentes por
`eventId`). *Exatamente uma vez* de ponta a ponta não é prometido — é inalcançável sem cooperação do destino.

## 7. Segurança

OAuth2 `client_credentials` no Keycloak. A API valida o JWT **sem biblioteca externa**: assinatura RS256 pelas
chaves do JWKS (cache; recarrega no máximo a cada 10 s ao ver um `kid` desconhecido), `iss`, `aud`, `exp`/`nbf`
(tolerância de 30 s). `alg=none` e HS256 são recusados. Roles de realm separam o uso interno
(`wallet-internal`) do uso por provedores (`wager-provider`); o `provider_id` vem de uma claim fixa no token, nunca
do corpo da requisição. Falha ao buscar chaves devolve `503`, não `401` (não é culpa do cliente).
`/health/*` e `/metrics` são públicos.

## 8. Banco de dados (resumo)

Tabelas: `wallets`, `wager_transactions`, `wallet_ledger_entries`, `inbox_messages`, `outbox_events`.
O banco também defende as invariantes (defesa em profundidade): `CHECK (balance >= 0)`, `UNIQUE`s de
idempotência, triggers que impedem alterar/apagar ledger e alterar o corpo da outbox. SQL é explícito (pgx), sem
ORM. Migrations versionadas em `migrations/` com `up`/`down`.

## 9. Observabilidade

- Logs JSON (`slog`) com `instance`, `requestId`/`correlationId`.
- `/metrics`: `http_requests_total`, `http_request_duration_seconds`, `wager_transactions_total{kind,status}`,
  `queue_messages_total{result}`, `outbox_events_published_total`, `outbox_publish_failures_total`.
- `/health/live` (processo vivo) e `/health/ready` (PostgreSQL e as duas filas respondem).

## 10. Estratégia de testes

Mesmas regras, três níveis, **sem mocks de PostgreSQL/SQS/IdP** nos testes de integração:

1. Domínio: unitários puros.
2. Suíte de aplicação (`wageringtest.RunSuite`): roda em memória (rápida, com `-race`) **e** no PostgreSQL real —
   incluindo 50 requisições idênticas, duas apostas de 80,00 sobre 100,00, várias instâncias do serviço,
   referências pendentes, inbox, e dois publicadores de outbox.
3. Integração: HTTP com ≥ 3 instâncias reais + JWT; SQS real (LocalStack) para consumidor interrompido após o
   commit, mensagens duplicadas, DLQ e publicadores concorrentes; guardas do banco.

## 11. Decisões e limites conhecidos

- **Lock por carteira**: o throughput de *uma* carteira é limitado pela serialização (aceitável: apostas de um
  jogador são sequenciais por natureza). Carteiras diferentes não se bloqueiam.
- **Ordem na saída**: garantida por carteira (FIFO group + regra de elegibilidade); não há ordem global.
- **Entrega pelo menos uma vez** na saída; consumidores externos devem deduplicar por `eventId`.
- **Polling** da outbox (200 ms) em vez de `LISTEN/NOTIFY`: mais simples e robusto; custo é uma pequena latência.
- **Sem retenção/arquivamento**: ledger e outbox crescem; em produção seriam particionados e a outbox publicada
  seria podada.
- **Segredos de desenvolvimento** (realm do Keycloak, `test/test` da AWS) são só para o ambiente local.
- Suporte multimoeda: uma carteira por `(jogador, moeda)`; não há conversão.
