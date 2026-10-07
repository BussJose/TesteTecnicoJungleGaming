package wager

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalid é a base de todo erro de validação (entrada corrigível).
	ErrInvalid = errors.New("wager: invalid")
	// ErrInsufficientFunds indica que um débito deixaria o saldo negativo.
	ErrInsufficientFunds = errors.New("wager: insufficient funds")
	// ErrTerminalState indica tentativa de alterar uma transação terminal.
	ErrTerminalState = errors.New("wager: transaction is in a terminal state")
	// ErrInvalidTransition indica uma transição de estado não permitida.
	ErrInvalidTransition = errors.New("wager: invalid state transition")
)

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}
