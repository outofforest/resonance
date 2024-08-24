package build

import (
	"context"

	"github.com/outofforest/build"
	"github.com/outofforest/proton"

	"github.com/outofforest/resonance/test"
)

// Generate generates protos.
func Generate(_ context.Context, _ build.DepsFunc) error {
	return proton.Generate(
		"test/types.proton.go",
		test.Message{},
	)
}
