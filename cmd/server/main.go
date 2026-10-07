package main

import (
	"github.com/monii/backend-challenge-go/internal/app"
	"go.uber.org/fx"
)

func main() {
	fx.New(app.Module()).Run()
}
