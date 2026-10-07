package sqs

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Client struct {
	client   *sqs.Client
	queueURL string
}

type Options struct {
	Region       string
	EndpointURL  string
	QueueURL     string
	StaticKey    string
	StaticSecret string
}

func New(ctx context.Context, opts Options) (*Client, error) {
	if opts.Region == "" {
		opts.Region = "us-east-1"
	}
	if opts.QueueURL == "" {
		return nil, fmt.Errorf("SQS queue URL is required for readiness checks")
	}

	loadOpts := []func(*config.LoadOptions) error{
		config.WithRegion(opts.Region),
	}
	if opts.EndpointURL != "" {
		loadOpts = append(loadOpts, config.WithBaseEndpoint(opts.EndpointURL))
	}
	if opts.StaticKey != "" && opts.StaticSecret != "" {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(opts.StaticKey, opts.StaticSecret, ""),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	return &Client{
		client:   sqs.NewFromConfig(awsCfg),
		queueURL: opts.QueueURL,
	}, nil
}

func (c *Client) Ready(ctx context.Context) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("sqs client not initialized")
	}
	checkCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	_, err := c.client.GetQueueAttributes(checkCtx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(c.queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return fmt.Errorf("sqs get queue attributes: %w", err)
	}
	return nil
}
