// Package money implementa um value object imutável para dinheiro.
//
// Representação: int64 em unidades mínimas (centavos) + código de moeda
// ISO 4217. Escala fixa de 2 casas. Nunca usa float32/float64.
// Limite: |valor| <= 92.233.720.368.547.758,07 (MaxInt64 centavos).
package money

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

var (
	ErrInvalidAmount    = errors.New("money: invalid amount")
	ErrNegativeAmount   = errors.New("money: negative amount not allowed")
	ErrInvalidCurrency  = errors.New("money: invalid currency")
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	ErrOverflow         = errors.New("money: overflow")
)

// Moedas suportadas (todas com 2 casas decimais).
var supported = map[string]bool{"BRL": true, "USD": true, "EUR": true}

// Formato aceito: inteiro sem zeros à esquerda, com 0 a 2 casas decimais.
// Entradas equivalentes ("25", "25.5", "25.50") são normalizadas para
// centavos, então o hash de idempotência (calculado sobre os centavos)
// é o mesmo para todas elas.
var amountRe = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]{1,2})?$`)

// Money é imutável: valor em centavos (int64) + moeda.
// O valor zero do tipo (Money{}) não tem moeda e é inválido.
type Money struct {
	minor    int64
	currency string
}

func validCurrency(c string) error {
	if !supported[c] {
		return fmt.Errorf("%w: %q", ErrInvalidCurrency, c)
	}
	return nil
}

// New cria a partir de centavos (uso interno e reidratação). Aceita negativos.
func New(minor int64, currency string) (Money, error) {
	if err := validCurrency(currency); err != nil {
		return Money{}, err
	}
	return Money{minor: minor, currency: currency}, nil
}

// Zero devolve o valor zero de uma moeda.
func Zero(currency string) (Money, error) { return New(0, currency) }

// Parse lê uma string decimal de entrada externa (não aceita negativos).
func Parse(amount, currency string) (Money, error) {
	if err := validCurrency(currency); err != nil {
		return Money{}, err
	}
	if strings.HasPrefix(amount, "-") {
		return Money{}, ErrNegativeAmount
	}
	if !amountRe.MatchString(amount) {
		return Money{}, fmt.Errorf("%w: %q", ErrInvalidAmount, amount)
	}
	whole, frac, _ := strings.Cut(amount, ".")
	for len(frac) < 2 {
		frac += "0"
	}
	w, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return Money{}, ErrOverflow
	}
	f, _ := strconv.ParseInt(frac, 10, 64)
	if w > (math.MaxInt64-f)/100 {
		return Money{}, ErrOverflow
	}
	return Money{minor: w*100 + f, currency: currency}, nil
}

func (m Money) IsValid() bool    { return m.currency != "" }
func (m Money) Currency() string { return m.currency }
func (m Money) Minor() int64     { return m.minor }
func (m Money) IsZero() bool     { return m.minor == 0 }
func (m Money) IsPositive() bool { return m.minor > 0 }
func (m Money) IsNegative() bool { return m.minor < 0 }

func (m Money) check(o Money) error {
	if !m.IsValid() || !o.IsValid() {
		return ErrInvalidCurrency
	}
	if m.currency != o.currency {
		return fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.currency, o.currency)
	}
	return nil
}

func (m Money) Add(o Money) (Money, error) {
	if err := m.check(o); err != nil {
		return Money{}, err
	}
	if (o.minor > 0 && m.minor > math.MaxInt64-o.minor) ||
		(o.minor < 0 && m.minor < math.MinInt64-o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor + o.minor, currency: m.currency}, nil
}

func (m Money) Sub(o Money) (Money, error) {
	if err := m.check(o); err != nil {
		return Money{}, err
	}
	if (o.minor > 0 && m.minor < math.MinInt64+o.minor) ||
		(o.minor < 0 && m.minor > math.MaxInt64+o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor - o.minor, currency: m.currency}, nil
}

func (m Money) Neg() (Money, error) {
	if !m.IsValid() {
		return Money{}, ErrInvalidCurrency
	}
	if m.minor == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

// Cmp devolve -1, 0 ou 1.
func (m Money) Cmp(o Money) (int, error) {
	if err := m.check(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	}
	return 0, nil
}

// Amount devolve a string decimal com 2 casas, ex.: "25.00".
func (m Money) Amount() string {
	neg := m.minor < 0
	var u uint64
	if neg {
		u = uint64(-(m.minor + 1)) + 1 // seguro para MinInt64
	} else {
		u = uint64(m.minor)
	}
	s := fmt.Sprintf("%d.%02d", u/100, u%100)
	if neg {
		return "-" + s
	}
	return s
}

func (m Money) String() string { return m.Amount() + " " + m.currency }

type wire struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func (m Money) MarshalJSON() ([]byte, error) {
	if !m.IsValid() {
		return nil, ErrInvalidCurrency
	}
	return json.Marshal(wire{Amount: m.Amount(), Currency: m.currency})
}

// UnmarshalJSON exige string em "amount": número JSON (float) é rejeitado.
func (m *Money) UnmarshalJSON(b []byte) error {
	var w wire
	if err := json.Unmarshal(b, &w); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAmount, err)
	}
	v, err := Parse(w.Amount, w.Currency)
	if err != nil {
		return err
	}
	*m = v
	return nil
}
