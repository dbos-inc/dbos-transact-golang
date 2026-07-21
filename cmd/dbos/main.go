package main

import (
	"os"

	_ "github.com/dbos-inc/dbos-transact-golang/dbos/sqlite"
)

func main() {
	if err := Execute(); err != nil {
		os.Exit(1)
	}
}

func Execute() error {
	return rootCmd.Execute()
}
