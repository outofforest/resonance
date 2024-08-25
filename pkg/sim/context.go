package sim

import (
	"context"
	"testing"

	"github.com/outofforest/logger"
)

// NewContext returns new context for simulations in tests.
func NewContext(t *testing.T) context.Context {
	ctx, cancel := context.WithCancel(logger.WithLogger(context.Background(), logger.New(logger.DefaultConfig)))
	t.Cleanup(cancel)

	return ctx
}
