package dbos

import (
	"fmt"
	"net/url"
	"os"

	"github.com/spf13/viper"
)

const dbosConfigFileName = "dbos_config.yaml"

type configFile struct {
	Name        string `yaml:"name"`
	DatabaseURL string `yaml:"database_url"`
}

func LoadConfigFile() (*configFile, error) {
	v := viper.New()
	v.SetConfigFile(dbosConfigFileName)
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	var config configFile
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	return &config, nil
}

// NewConfig merges configuration from three sources in order of precedence:
// 1. programmatic configuration (highest precedence)
// 2. configuration file
// 3. environment variables (lowest precedence)
func NewConfig(programmaticConfig config) *config {
	fileConfig, err := LoadConfigFile()
	if err != nil && !os.IsNotExist(err) {
		panic(err)
	}

	dbosConfig := &config{}

	// Start with environment variables (lowest precedence)
	if dbURL := os.Getenv("DBOS_DATABASE_URL"); dbURL != "" {
		dbosConfig.databaseURL = dbURL
	}

	// Override with file configuration if available
	if fileConfig != nil {
		if fileConfig.DatabaseURL != "" {
			dbosConfig.databaseURL = fileConfig.DatabaseURL
		}
		if fileConfig.Name != "" {
			dbosConfig.appName = fileConfig.Name
		}
	}

	// Override with programmatic configuration (highest precedence)
	if len(programmaticConfig.databaseURL) > 0 {
		dbosConfig.databaseURL = programmaticConfig.databaseURL
	}
	if len(programmaticConfig.appName) > 0 {
		dbosConfig.appName = programmaticConfig.appName
	}
	// Copy over parameters that can only be set programmatically
	dbosConfig.logger = programmaticConfig.logger
	dbosConfig.adminServer = programmaticConfig.adminServer

	// Load defaults
	if len(dbosConfig.databaseURL) == 0 {
		fmt.Println("DBOS_DATABASE_URL not set, using default: postgres://postgres:${PGPASSWORD}@localhost:5432/dbos?sslmode=disable")
		password := url.QueryEscape(os.Getenv("PGPASSWORD"))
		dbosConfig.databaseURL = fmt.Sprintf("postgres://postgres:%s@localhost:5432/dbos?sslmode=disable", password)
	}
	return dbosConfig
}
