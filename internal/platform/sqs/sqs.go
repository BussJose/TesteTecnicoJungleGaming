// Package sqs encapsula o cliente AWS SQS (LocalStack em desenvolvimento).
package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/monii/backend-challenge-go/internal/domain/wager"
	"github.com/monii/backend-challenge-go/internal/messaging"
)

// Options configura o cliente. As credenciais vêm das variáveis padrão da
// AWS (AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY).
type Options struct {
	Region         string
	Endpoint       string // vazio = AWS real; http://localhost:4566 = LocalStack
	WagerQueueURL  string // entrada: transações enviadas pelos provedores
	EventsQueueURL string // saída: eventos publicados pela outbox
}

// Client é o cliente SQS ligado às duas filas.
type Client struct {
	API            *awssqs.Client
	WagerQueueURL  string
	EventsQueueURL string
}

// NewClient cria o cliente.
func NewClient(ctx context.Context, o Options) (*Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(o.Region))
	if err != nil {
		return nil, fmt.Errorf("carregar configuração AWS: %w", err)
	}
	api := awssqs.NewFromConfig(cfg, func(so *awssqs.Options) {
		if o.Endpoint != "" {
			so.BaseEndpoint = aws.String(o.Endpoint)
		}
	})
	return &Client{API: api, WagerQueueURL: o.WagerQueueURL, EventsQueueURL: o.EventsQueueURL}, nil
}

// Ready confirma que as filas existem e respondem (usado em /health/ready).
func (c *Client) Ready(ctx context.Context) error {
	for _, u := range []string{c.WagerQueueURL, c.EventsQueueURL} {
		if _, err := c.API.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{QueueUrl: aws.String(u)}); err != nil {
			return fmt.Errorf("fila %s: %w", u, err)
		}
	}
	return nil
}

// Queue lê a fila de entrada (implementa messaging.Queue).
type Queue struct {
	api *awssqs.Client
	url string
}

var _ messaging.Queue = (*Queue)(nil)

// WagerQueue devolve o leitor da fila de transações.
func (c *Client) WagerQueue() *Queue { return &Queue{api: c.API, url: c.WagerQueueURL} }

// Receive faz long polling.
func (q *Queue) Receive(ctx context.Context, max int, wait time.Duration) ([]messaging.Message, error) {
	secs := int32(wait / time.Second)
	if secs > 20 {
		secs = 20
	}
	out, err := q.api.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.url),
		MaxNumberOfMessages: int32(max),
		WaitTimeSeconds:     secs,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameApproximateReceiveCount,
		},
	})
	if err != nil {
		return nil, err
	}
	msgs := make([]messaging.Message, 0, len(out.Messages))
	for _, m := range out.Messages {
		count, _ := strconv.Atoi(m.Attributes["ApproximateReceiveCount"])
		msgs = append(msgs, messaging.Message{
			ID: aws.ToString(m.MessageId), ReceiptHandle: aws.ToString(m.ReceiptHandle),
			Body: aws.ToString(m.Body), ReceiveCount: count,
		})
	}
	return msgs, nil
}

// Delete apaga a mensagem.
func (q *Queue) Delete(ctx context.Context, receiptHandle string) error {
	_, err := q.api.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl: aws.String(q.url), ReceiptHandle: aws.String(receiptHandle),
	})
	return err
}

// Release torna a mensagem visível de novo agora. Quando o número de
// recebimentos passa de maxReceiveCount, a própria fila a move para a DLQ.
func (q *Queue) Release(ctx context.Context, receiptHandle string) error {
	_, err := q.api.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(q.url), ReceiptHandle: aws.String(receiptHandle), VisibilityTimeout: 0,
	})
	return err
}

// EventPublisher publica os eventos da outbox na fila FIFO de eventos
// (implementa outbox.Publisher).
type EventPublisher struct {
	api *awssqs.Client
	url string
}

// EventPublisher devolve o publicador de eventos.
func (c *Client) EventPublisher() *EventPublisher {
	return &EventPublisher{api: c.API, url: c.EventsQueueURL}
}

// Publish envia o envelope. MessageGroupId = carteira (ordem por carteira);
// MessageDeduplicationId = eventId (reenvios do mesmo evento são ignorados
// pela fila dentro da janela de deduplicação).
func (p *EventPublisher) Publish(ctx context.Context, env wager.Envelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	str := func(v string) types.MessageAttributeValue {
		return types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
	}
	_, err = p.api.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:               aws.String(p.url),
		MessageBody:            aws.String(string(body)),
		MessageGroupId:         aws.String(env.AggregateID.String()),
		MessageDeduplicationId: aws.String(env.EventID.String()),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"eventType":     str(string(env.EventType)),
			"correlationId": str(env.CorrelationID),
		},
	})
	return err
}
