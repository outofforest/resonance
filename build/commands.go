package build

import "github.com/outofforest/build/v2/pkg/types"

// Commands is the list of commands.
var Commands = map[string]types.Command{
	"generate": {Fn: generate, Description: "Generates go code from protos"},
}
