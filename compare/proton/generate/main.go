package main

import (
	"github.com/outofforest/proton"
	compareproton "github.com/outofforest/resonance/compare/proton"
)

//go:generate go run .

func main() {
	proton.Generate("../types.proton.go",
		compareproton.Transaction{},
		compareproton.TransactionResponse{},
	)
}
