// Package postgres contém a conexão com o PostgreSQL e o Store que implementa
// as portas da aplicação com SQL explícito (pgx).
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool é o pool de conexões.
type Pool struct{ *pgxpool.Pool }

// NewPool abre o pool e confirma que o banco responde.
func NewPool(ctx context.Context, databaseURL string, maxConns int) (*Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("DATABASE_URL inválida: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = int32(maxConns)
	}
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("abrir pool postgres: %w", err)
	}
	if err := p.Ping(ctx); err != nil {
		p.Close()
		return nil, fmt.Errorf("postgres não responde: %w", err)
	}
	return &Pool{p}, nil
}

// Ready verifica a conexão (usado em /health/ready).
func (p *Pool) Ready(ctx context.Context) error { return p.Ping(ctx) }
