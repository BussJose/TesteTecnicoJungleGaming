// Package id implementa identificadores UUID (RFC 9562) sem dependências
// externas. Os IDs gerados são UUID v7: ordenáveis por tempo de criação.
package id

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// ID é um UUID de 16 bytes. O valor zero (Nil) é inválido para o domínio.
type ID [16]byte

// Nil é o UUID zerado.
var Nil ID

// ErrInvalid indica um UUID malformado.
var ErrInvalid = errors.New("id: invalid uuid")

// New gera um UUID v7 (48 bits de timestamp em ms + aleatório).
func New() ID {
	var u ID
	_, _ = rand.Read(u[:])
	ms := uint64(time.Now().UnixMilli())
	u[0] = byte(ms >> 40)
	u[1] = byte(ms >> 32)
	u[2] = byte(ms >> 24)
	u[3] = byte(ms >> 16)
	u[4] = byte(ms >> 8)
	u[5] = byte(ms)
	u[6] = (u[6] & 0x0f) | 0x70 // versão 7
	u[8] = (u[8] & 0x3f) | 0x80 // variante RFC 4122/9562
	return u
}

// Parse lê a forma canônica "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".
func Parse(s string) (ID, error) {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return Nil, fmt.Errorf("%w: %q", ErrInvalid, s)
	}
	hexStr := s[0:8] + s[9:13] + s[14:18] + s[19:23] + s[24:36]
	b, err := hex.DecodeString(hexStr)
	if err != nil || len(b) != 16 {
		return Nil, fmt.Errorf("%w: %q", ErrInvalid, s)
	}
	var u ID
	copy(u[:], b)
	return u, nil
}

// IsZero informa se o ID é o UUID zerado.
func (i ID) IsZero() bool { return i == Nil }

// String devolve a forma canônica em minúsculas.
func (i ID) String() string {
	var buf [36]byte
	hex.Encode(buf[0:8], i[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], i[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], i[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], i[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], i[10:16])
	return string(buf[:])
}

// MarshalText permite que o ID seja serializado como string em JSON.
func (i ID) MarshalText() ([]byte, error) { return []byte(i.String()), nil }

// UnmarshalText lê a forma canônica.
func (i *ID) UnmarshalText(b []byte) error {
	v, err := Parse(string(b))
	if err != nil {
		return err
	}
	*i = v
	return nil
}
