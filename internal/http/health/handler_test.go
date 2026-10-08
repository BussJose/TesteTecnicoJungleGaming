package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func call(h http.HandlerFunc) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest("GET", "/", nil))
	return rec
}

func TestReady(t *testing.T) {
	up := Check{"postgres", func(context.Context) error { return nil }}
	down := Check{"sqs", func(context.Context) error { return errors.New("boom") }}

	rec := call(NewHandler(up).Ready)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"postgres":"up"`) {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	rec = call(NewHandler(up, down).Ready)
	if rec.Code != 503 || !strings.Contains(rec.Body.String(), `"sqs":"down"`) || strings.Contains(rec.Body.String(), "boom") {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	if rec := call(NewHandler(down).Live); rec.Code != 200 {
		t.Fatalf("live must not depend on checks: %d", rec.Code)
	}
}
