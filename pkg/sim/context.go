package sim

import (
	"context"
	"testing"

	"github.com/outofforest/logger"
)

// NewContext returns new context for simulations in tests.
func NewContext(t *testing.T) context.Context {
	return logger.WithLogger(t.Context(), logger.New(logger.DefaultConfig))
}
