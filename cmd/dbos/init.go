package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"text/template"

	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init [project-name]",
	Short: "Initialize a new DBOS application from a template",
	RunE:  runInit,
}

type templateData struct {
	ProjectName string
}

func runInit(cmd *cobra.Command, args []string) error {
	var projectName string
	if len(args) > 0 {
		projectName = args[0]
	} else {
		projectName = "dbos-go-starter"
	}

	// Check if directory already exists
	if _, err := os.Stat(projectName); err == nil {
		return fmt.Errorf("directory '%s' already exists", projectName)
	}

	// Create project directory
	if err := os.MkdirAll(projectName, 0755); err != nil {
		return fmt.Errorf("failed to create directory '%s': %w", projectName, err)
	}

	// Template data
	data := templateData{
		ProjectName: projectName,
	}

	// Process and write each template file
	templates := map[string]string{
		"templates/dbos-go-starter/go.mod.tmpl":           "go.mod",
		"templates/dbos-go-starter/main.go.tmpl":          "main.go",
		"templates/dbos-go-starter/dbos-config.yaml.tmpl": "dbos-config.yaml",
		"templates/dbos-go-starter/app.html":              "html/app.html",
	}

	for tmplPath, outputFile := range templates {
		// Read template from embedded FS
		tmplContent, err := templateFS.ReadFile(tmplPath)
		if err != nil {
			return fmt.Errorf("failed to read template %s: %w", tmplPath, err)
		}

		// Parse and execute template
		tmpl, err := template.New(outputFile).Parse(string(tmplContent))
		if err != nil {
			return fmt.Errorf("failed to parse template %s: %w", tmplPath, err)
		}

		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, data); err != nil {
			return fmt.Errorf("failed to execute template %s: %w", tmplPath, err)
		}

		// Write output file
		outputPath := filepath.Join(projectName, outputFile)
		if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
			return fmt.Errorf("failed to create directory for %s: %w", outputFile, err)
		}
		if err := os.WriteFile(outputPath, buf.Bytes(), 0644); err != nil {
			return fmt.Errorf("failed to write %s: %w", outputFile, err)
		}
	}

	fmt.Printf("Created new DBOS application: %s\n", projectName)
	fmt.Println("To get started:")
	fmt.Printf("  cd %s\n", projectName)
	fmt.Println("  go mod tidy")
	fmt.Println("  export DBOS_SYSTEM_DATABASE_URL=\"postgres://<user>:<password>@<host>:<port>/<db>\"")
	fmt.Println("  go run main.go")

	return nil
}
