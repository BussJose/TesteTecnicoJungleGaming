// Package config lê a configuração do serviço a partir de variáveis de ambiente.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config reúne todos os parâmetros do serviço.
type Config struct {
	HTTPAddr    string
	DatabaseURL string
	DBMaxConns  int
	LogLevel    string
	InstanceID  string

	AWSRegion         string
	AWSEndpointURL    string
	SQSWagerQueueURL  string // entrada (transações)
	SQSEventsQueueURL string // saída (eventos da outbox)

	// Keycloak: Issuer é o valor esperado na claim "iss"; JWKSURL é onde buscar
	// as chaves públicas (dentro do Docker costuma diferir do Issuer).
	KeycloakIssuer   string
	KeycloakJWKSURL  string
	KeycloakAudience string

	// Referências pendentes (REFUND/ROLLBACK/WIN antes da aposta).
	ReferenceTTL         time.Duration
	MaxReferenceAttempts int
	BackoffBase          time.Duration
	BackoffMax           time.Duration
	PendingPollInterval  time.Duration

	ConsumerWorkers    int
	OutboxPollInterval time.Duration
	OutboxBatch        int
}

// Load lê o ambiente real do processo.
func Load() (Config, error) { return LoadFrom(os.LookupEnv) }

// LoadFrom lê a configuração de uma função de busca (facilita testes).
func LoadFrom(lookup func(string) (string, bool)) (Config, error) {
	get := func(key, def string) string {
		if v, ok := lookup(key); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
		return def
	}
	var errs []string
	dur := func(key, def string) time.Duration {
		d, err := time.ParseDuration(get(key, def))
		if err != nil || d <= 0 {
			errs = append(errs, fmt.Sprintf("%s: duração inválida %q (exemplo: 30s)", key, get(key, def)))
		}
		return d
	}
	integer := func(key string, def int) int {
		n, err := strconv.Atoi(get(key, strconv.Itoa(def)))
		if err != nil || n < 1 {
			errs = append(errs, fmt.Sprintf("%s: número inteiro positivo esperado", key))
		}
		return n
	}

	hostname, _ := os.Hostname()
	kcURL := strings.TrimRight(get("KEYCLOAK_URL", "http://localhost:8081"), "/")
	realm := get("KEYCLOAK_REALM", "wager")
	realmURL := kcURL + "/realms/" + realm

	c := Config{
		HTTPAddr:    get("HTTP_ADDR", ":8080"),
		DatabaseURL: get("DATABASE_URL", "postgres://wager:wager@localhost:5432/wager?sslmode=disable"),
		DBMaxConns:  integer("DB_MAX_CONNS", 20),
		LogLevel:    strings.ToLower(get("LOG_LEVEL", "info")),
		InstanceID:  get("INSTANCE_ID", hostname),

		AWSRegion:         get("AWS_REGION", "us-east-1"),
		AWSEndpointURL:    get("AWS_ENDPOINT_URL", ""),
		SQSWagerQueueURL:  get("SQS_WAGER_QUEUE_URL", "http://localhost:4566/000000000000/wager-transactions.fifo"),
		SQSEventsQueueURL: get("SQS_EVENTS_QUEUE_URL", "http://localhost:4566/000000000000/wager-events.fifo"),

		KeycloakIssuer:   get("KEYCLOAK_ISSUER", realmURL),
		KeycloakJWKSURL:  get("KEYCLOAK_JWKS_URL", realmURL+"/protocol/openid-connect/certs"),
		KeycloakAudience: get("KEYCLOAK_AUDIENCE", get("KEYCLOAK_CLIENT_ID", "wager-api")),

		ReferenceTTL:         dur("REFERENCE_TTL", "5m"),
		MaxReferenceAttempts: integer("REFERENCE_MAX_ATTEMPTS", 10),
		BackoffBase:          dur("REFERENCE_BACKOFF_BASE", "1s"),
		BackoffMax:           dur("REFERENCE_BACKOFF_MAX", "30s"),
		PendingPollInterval:  dur("PENDING_POLL_INTERVAL", "1s"),

		ConsumerWorkers:    integer("CONSUMER_WORKERS", 4),
		OutboxPollInterval: dur("OUTBOX_POLL_INTERVAL", "200ms"),
		OutboxBatch:        integer("OUTBOX_BATCH", 50),
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, "LOG_LEVEL: use debug, info, warn ou error")
	}
	if c.InstanceID == "" {
		c.InstanceID = "instance-1"
	}
	if len(errs) > 0 {
		return Config{}, fmt.Errorf("configuração inválida:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return c, nil
}
