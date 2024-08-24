package build

import (
	"context"

	"github.com/outofforest/build"
	"github.com/outofforest/proton"

	compareproton "github.com/outofforest/resonance/compare/proton"

	"github.com/outofforest/resonance/test"
)

// Generate generates protos.
func Generate(_ context.Context, _ build.DepsFunc) error {
	if err := proton.Generate(
		"test/types.proton.go",
		test.Message{},
	); err != nil {
		return err
	}

	return proton.Generate(
		"compare/proton/types.proton.go",
		compareproton.Signature{},
		compareproton.TransactionHeader{},
		compareproton.Transaction{},
		compareproton.TransactionResponse{},
	)
}
