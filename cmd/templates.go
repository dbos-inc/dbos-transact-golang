package main

import (
	"embed"
)

//go:embed templates/dbos-toolbox/*
var templateFS embed.FS