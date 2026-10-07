package id

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestNewIsV7AndUnique(t *testing.T) {
	seen := map[ID]bool{}
	for i := 0; i < 1000; i++ {
		u := New()
		if u.IsZero() {
			t.Fatal("id zerado")
		}
		if u[6]>>4 != 7 {
			t.Fatalf("versão deveria ser 7, veio %d", u[6]>>4)
		}
		if u[8]>>6 != 0b10 {
			t.Fatalf("variante inválida: %b", u[8]>>6)
		}
		if seen[u] {
			t.Fatal("id repetido")
		}
		seen[u] = true
	}
}

func TestParseRoundTrip(t *testing.T) {
	in := "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"
	u, err := Parse(in)
	if err != nil {
		t.Fatal(err)
	}
	if u.String() != in {
		t.Errorf("got %s", u.String())
	}
	up, err := Parse("0192F28F-5DC0-7D58-BDB2-814AD6A0F4A1")
	if err != nil || up != u {
		t.Error("maiúsculas deveriam ser aceitas e normalizadas")
	}
}

func TestParseInvalid(t *testing.T) {
	for _, s := range []string{"", "abc", "0192f28f5dc07d58bdb2814ad6a0f4a1", "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a", "zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz", "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1 "} {
		if _, err := Parse(s); !errors.Is(err, ErrInvalid) {
			t.Errorf("Parse(%q) deveria falhar, veio %v", s, err)
		}
	}
}

func TestJSON(t *testing.T) {
	u := New()
	b, err := json.Marshal(struct {
		ID ID `json:"id"`
	}{u})
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		ID ID `json:"id"`
	}
	if err := json.Unmarshal(b, &out); err != nil || out.ID != u {
		t.Errorf("round trip falhou: %s %v", b, err)
	}
}
