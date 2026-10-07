package money

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func TestParse(t *testing.T) {
	ok := map[string]int64{
		"0": 0, "0.00": 0, "25": 2500, "25.5": 2550, "25.00": 2500, "1000.99": 100099,
	}
	for in, want := range ok {
		m, err := Parse(in, "BRL")
		if err != nil || m.Minor() != want {
			t.Errorf("Parse(%q) = %v, %v; want %d", in, m.Minor(), err, want)
		}
	}
	bad := []string{"", " ", "abc", "NaN", "Infinity", "1e3", "1E3", "25.123", "007.00", "1,50", ".5", "5.", "+5"}
	for _, in := range bad {
		if _, err := Parse(in, "BRL"); !errors.Is(err, ErrInvalidAmount) {
			t.Errorf("Parse(%q) deveria falhar com ErrInvalidAmount, veio %v", in, err)
		}
	}
	if _, err := Parse("-1.00", "BRL"); !errors.Is(err, ErrNegativeAmount) {
		t.Error("negativo deveria ser rejeitado")
	}
	if _, err := Parse("1.00", "XXX"); !errors.Is(err, ErrInvalidCurrency) {
		t.Error("moeda inválida deveria ser rejeitada")
	}
	if _, err := Parse("99999999999999999999.00", "BRL"); !errors.Is(err, ErrOverflow) {
		t.Error("overflow deveria ser detectado")
	}
}

func TestArithmetic(t *testing.T) {
	a, _ := Parse("100.00", "BRL")
	b, _ := Parse("80.00", "BRL")
	s, _ := a.Sub(b)
	if s.Amount() != "20.00" {
		t.Errorf("got %s", s.Amount())
	}
	d, _ := b.Sub(a)
	if d.Amount() != "-20.00" {
		t.Errorf("got %s", d.Amount())
	}
	usd, _ := Parse("1.00", "USD")
	if _, err := a.Add(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Error("moedas diferentes deveriam falhar")
	}
	var zero Money
	if _, err := a.Add(zero); err == nil {
		t.Error("valor zero do tipo (sem moeda) deveria ser inválido")
	}
}

func TestOverflow(t *testing.T) {
	maxM, _ := New(math.MaxInt64, "BRL")
	minM, _ := New(math.MinInt64, "BRL")
	one, _ := New(1, "BRL")
	if _, err := maxM.Add(one); !errors.Is(err, ErrOverflow) {
		t.Error("Add deveria estourar")
	}
	if _, err := minM.Sub(one); !errors.Is(err, ErrOverflow) {
		t.Error("Sub deveria estourar")
	}
	if _, err := minM.Neg(); !errors.Is(err, ErrOverflow) {
		t.Error("Neg deveria estourar")
	}
	if minM.Amount() != "-92233720368547758.08" {
		t.Errorf("formatação de MinInt64: %s", minM.Amount())
	}
}

func TestJSON(t *testing.T) {
	var m Money
	if err := json.Unmarshal([]byte(`{"amount":"25.00","currency":"BRL"}`), &m); err != nil {
		t.Fatal(err)
	}
	out, _ := json.Marshal(m)
	if string(out) != `{"amount":"25.00","currency":"BRL"}` {
		t.Errorf("got %s", out)
	}
	// número JSON (float) não pode ser aceito
	if err := json.Unmarshal([]byte(`{"amount":25.0,"currency":"BRL"}`), &m); err == nil {
		t.Error("amount numérico deveria ser rejeitado")
	}
}
