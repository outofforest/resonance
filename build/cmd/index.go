package main

import (
	"github.com/outofforest/build"
	"github.com/outofforest/buildgo"

	me "build"
)

var commands = map[string]build.Command{
	"setup":        {Fn: me.Setup, Description: "Installs tools required by development environment"},
	"dev/generate": {Fn: me.Generate, Description: "Generates go code from protos"},
}

func init() {
	buildgo.AddCommands(commands)
	build.RegisterCommands(commands)
}
