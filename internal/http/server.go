package httpapi

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/monii/backend-challenge-go/internal/http/health"
)

type Server struct {
	addr   string
	server *http.Server
}

func NewServer(addr string, healthHandler *health.Handler) *Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", healthHandler.Live)
	mux.HandleFunc("GET /health/ready", healthHandler.Ready)

	return &Server{
		addr: addr,
		server: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
}

func (s *Server) Start() error {
	if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
