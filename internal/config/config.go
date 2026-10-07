package config

import (
	"fmt"
	"os"
)

type Config struct {
	HTTPAddr         string
	DatabaseURL      string
	AWSRegion        string
	AWSEndpointURL   string
	SQSWagerQueueURL string
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:         envOr("HTTP_ADDR", ":8080"),
		DatabaseURL:      envOr("DATABASE_URL", "postgres://wager:wager@localhost:5432/wager?sslmode=disable"),
		AWSRegion:        envOr("AWS_REGION", "us-east-1"),
		AWSEndpointURL:   os.Getenv("AWS_ENDPOINT_URL"),
		SQSWagerQueueURL: envOr("SQS_WAGER_QUEUE_URL", "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-transactions.fifo"),
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	return cfg, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
