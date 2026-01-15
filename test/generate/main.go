package main

import (
	"github.com/outofforest/proton"
	"github.com/outofforest/resonance/test"
)

//go:generate go run .

func main() {
	proton.Generate("../types.proton.go",
		proton.Message[test.Message](),
	)
}
