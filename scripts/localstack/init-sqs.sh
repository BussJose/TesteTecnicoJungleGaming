#!/bin/bash
# Cria as filas SQS no LocalStack. O LocalStack executa este script sozinho
# quando fica pronto (pasta /etc/localstack/init/ready.d). É seguro rodar de
# novo: se a fila já existe, create-queue devolve a mesma fila.
#
#   wager-transactions-dlq.fifo  fila de mensagens "mortas" (DLQ)
#   wager-transactions.fifo      entrada: transações enviadas pelos provedores;
#                                após 5 recebimentos sem sucesso vai para a DLQ
#   wager-events.fifo            saída: eventos publicados pela outbox
set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ACCOUNT="000000000000"
TMP="$(mktemp -d)"

echo "[init-sqs] criando DLQ"
awslocal sqs create-queue --queue-name wager-transactions-dlq.fifo \
  --attributes '{"FifoQueue":"true","MessageRetentionPeriod":"1209600"}' >/dev/null

DLQ_ARN="arn:aws:sqs:${REGION}:${ACCOUNT}:wager-transactions-dlq.fifo"

# O RedrivePolicy é um JSON dentro de outro JSON, por isso vai em arquivo.
cat > "${TMP}/main.json" <<JSON
{
  "FifoQueue": "true",
  "VisibilityTimeout": "30",
  "RedrivePolicy": "{\"deadLetterTargetArn\":\"${DLQ_ARN}\",\"maxReceiveCount\":\"5\"}"
}
JSON

echo "[init-sqs] criando fila principal com redrive para a DLQ"
awslocal sqs create-queue --queue-name wager-transactions.fifo \
  --attributes "file://${TMP}/main.json" >/dev/null

echo "[init-sqs] criando fila de eventos"
awslocal sqs create-queue --queue-name wager-events.fifo \
  --attributes '{"FifoQueue":"true","VisibilityTimeout":"30"}' >/dev/null

awslocal sqs list-queues
echo "[init-sqs] pronto"
