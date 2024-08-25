package sim

import (
	"context"
	"testing"

	"github.com/outofforest/parallel"
	"github.com/stretchr/testify/require"
)

// NewParallel returns new parallel group to be used in tests.
func NewParallel(ctx context.Context, t *testing.T) *parallel.Group {
	group := parallel.NewGroup(ctx)
	t.Cleanup(func() {
		group.Exit(nil)
		require.NoError(t, group.Wait())
	})
	return group
}
