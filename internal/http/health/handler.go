// Package health implementa /health/live (o processo está de pé) e
// /health/ready (as dependências respondem).
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Check verifica uma dependência.
type Check struct {
	Name string
	Fn   func(ctx context.Context) error
}

// Handler responde aos endpoints de saúde.
type Handler struct {
	checks  []Check
	timeout time.Duration
}

// NewHandler cria o handler com as verificações de prontidão.
func NewHandler(checks ...Check) *Handler {
	return &Handler{checks: checks, timeout: 2 * time.Second}
}

// Live responde 200 enquanto o processo estiver rodando.
func (h *Handler) Live(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"up"}`))
}

// Ready roda as verificações em paralelo; responde 503 se alguma falhar.
func (h *Handler) Ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	results := make(map[string]string, len(h.checks))
	var mu sync.Mutex
	var wg sync.WaitGroup
	ok := true
	for _, c := range h.checks {
		wg.Add(1)
		go func(c Check) {
			defer wg.Done()
			status := "up"
			if err := c.Fn(ctx); err != nil {
				status = "down"
			}
			mu.Lock()
			results[c.Name] = status
			if status == "down" {
				ok = false
			}
			mu.Unlock()
		}(c)
	}
	wg.Wait()

	names := make([]string, 0, len(results))
	for n := range results {
		names = append(names, n)
	}
	sort.Strings(names)
	checks := make(map[string]string, len(names))
	for _, n := range names {
		checks[n] = results[n]
	}
	body := map[string]any{"status": "up", "checks": checks}
	code := http.StatusOK
	if !ok {
		body["status"] = "down"
		code = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
