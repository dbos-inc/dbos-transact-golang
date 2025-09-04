package main

// Config represents the dbos-config.yaml structure
type Config struct {
	Name          string        `mapstructure:"name"`
	DatabaseURL   string        `mapstructure:"database_url"`
	RuntimeConfig RuntimeConfig `mapstructure:"runtimeConfig"`
	Database      Database      `mapstructure:"database"`
}

type RuntimeConfig struct {
	Start []string `mapstructure:"start"`
}

type Database struct {
	Migrate []string `mapstructure:"migrate"`
}
