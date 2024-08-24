package build

import (
	"context"

	"github.com/outofforest/build"
	"github.com/outofforest/buildgo"
)

// Setup sets up the environment.
func Setup(ctx context.Context, deps build.DepsFunc) error {
	deps(buildgo.InstallAll)
	return nil
}
