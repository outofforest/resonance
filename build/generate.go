package build

import (
	"context"

	"github.com/outofforest/build/v2/pkg/types"
	"github.com/outofforest/proton"
	compareproton "github.com/outofforest/resonance/compare/proton"
	"github.com/outofforest/resonance/test"
)

func generate(_ context.Context, _ types.DepsFunc) error {
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
