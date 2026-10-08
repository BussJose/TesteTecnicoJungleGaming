// Package sqs encapsula o cliente AWS SQS (LocalStack em desenvolvimento).
package sqs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
)

// Options configura o cliente. As credenciais vêm das variáveis padrão da
// AWS (AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY).
type Options struct {
	Region   string
	Endpoint string // vazio = AWS real; http://localhost:4566 = LocalStack
	QueueURL string
}

// Client é o cliente SQS ligado à fila principal.
type Client struct {
	API      *awssqs.Client
	QueueURL string
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
	return &Client{API: api, QueueURL: o.QueueURL}, nil
}

// Ready confirma que a fila existe e responde (usado em /health/ready).
func (c *Client) Ready(ctx context.Context) error {
	_, err := c.API.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{QueueUrl: aws.String(c.QueueURL)})
	return err
}
