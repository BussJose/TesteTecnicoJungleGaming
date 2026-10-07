package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
)

type PostgresReady interface {
	Ready(context.Context) error
}

type SQSReady interface {
	Ready(context.Context) error
}

type Handler struct {
	pg  PostgresReady
	sqs SQSReady
}

func NewHandler(pg PostgresReady, sqs SQSReady) *Handler {
	return &Handler{pg: pg, sqs: sqs}
}

func (h *Handler) Live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) Ready(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	checks := map[string]string{}
	status := http.StatusOK

	var mu sync.Mutex
	var wg sync.WaitGroup
	run := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := fn(ctx)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				checks[name] = err.Error()
				status = http.StatusServiceUnavailable
				return
			}
			checks[name] = "ok"
		}()
	}

	run("postgres", h.pg.Ready)
	run("sqs", h.sqs.Ready)
	wg.Wait()

	body := map[string]any{
		"status": "ready",
		"checks": checks,
	}
	if status != http.StatusOK {
		body["status"] = "not_ready"
	}
	writeJSON(w, status, body)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
