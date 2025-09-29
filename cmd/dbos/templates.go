package main

import (
	"embed"
)

//go:embed templates/dbos-go-starter/*
var templateFS embed.FS
