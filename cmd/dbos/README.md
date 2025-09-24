# DBOS CLI

The DBOS CLI is a command-line interface for managing DBOS workflows.

## Installation

### From Source
```bash
go install github.com/dbos-inc/dbos-transact-golang/cmd/dbos@latest
```

### Build Locally
```bash
go build -o dbos .
```

## Configuration

### Database URL Configuration

The CLI requires a DBOS system database URL to operate. The URL is resolved in the following order of precedence:

1. **Command-line flag**: `--db-url` or `-D` flag
2. **Configuration file**: `database_url` field in `dbos-config.yaml`
3. **Environment variable**: `DBOS_SYSTEM_DATABASE_URL`

Example:
```bash
# Using command-line flag (highest priority)
dbos migrate --db-url postgres://user:pass@localhost/mydb

# Using config file (dbos-config.yaml)
database_url: postgres://user:pass@localhost/mydb

# Using environment variable (lowest priority)
export DBOS_SYSTEM_DATABASE_URL=postgres://user:pass@localhost/mydb
dbos migrate
```

### Database Schema Configuration

By default, DBOS creates its system tables in the `dbos` schema. You can specify a custom schema name using the `--schema` global flag.

Example:
```bash
# Use a custom schema for all DBOS system tables
dbos migrate --schema myapp_schema

# All workflow commands will use the specified schema
dbos workflow list --schema myapp_schema
```

### Configuration File

The CLI uses a `dbos-config.yaml` file for configuration. By default, it looks for this file in the current directory. You can specify a different config file using the `--config` flag.

```yaml
name: my-dbos-app
database_url: ${DBOS_SYSTEM_DATABASE_URL}

runtimeConfig:
  start:
    - go run .

database:
  migrate:
    - echo "Running migrations..."
```

## Global Options

These options are available for all commands:

- `-D, --db-url <url>` - Your DBOS system database URL
- `--config <file>` - Config file (default: dbos-config.yaml)
- `--schema <name>` - Database schema name (default: dbos)
- `--verbose` - Enable verbose mode (DEBUG level logging)

## Commands

### `dbos init [project-name]`
Initialize a new DBOS application from a template.

**Usage:**
```bash
dbos init myapp
```

Creates a new DBOS application with:
- `main.go` - Entry point with example workflow
- `go.mod` - Go module file
- `dbos-config.yaml` - DBOS configuration

### `dbos migrate`
Create DBOS system tables in your database. This command runs the migration commands specified in the `database.migrate` section of your config file.

**Options:**
- `-r, --app-role <role>` - The role with which you will run your DBOS application

**Usage:**
```bash
dbos migrate
dbos migrate --app-role myapp_role
dbos migrate --schema custom_schema
dbos migrate --schema custom_schema --app-role myapp_role
```

The migrate commands are defined in `dbos-config.yaml`:
```yaml
database:
  migrate:
    - echo "Running migrations..."
    - go run ./migrations
```

### `dbos reset`
Reset the DBOS system database, deleting metadata about past workflows and steps. _This is a permanent, destructive action!_

**Options:**
- `-y, --yes` - Skip confirmation prompt

**Usage:**
```bash
dbos reset
dbos reset --yes  # Skip confirmation
```

### `dbos start`
Start your DBOS application using the start commands defined in the `runtimeConfig.start` section of your `dbos-config.yaml`.

**Usage:**
```bash
dbos start
```

The start commands are defined in `dbos-config.yaml`:
```yaml
runtimeConfig:
  start:
    - go run .
    # Can have multiple commands that run in sequence
```

### `dbos postgres`
Manage a local PostgreSQL database with Docker for development.

#### `dbos postgres start`
Start a local Postgres database container with pgvector extension.

**Usage:**
```bash
dbos postgres start
```

Creates a PostgreSQL container with:
- Container name: `dbos-db`
- Port: 5432
- Default database: `dbos`
- Default user: `postgres`
- Default password: `dbos`

#### `dbos postgres stop`
Stop the local Postgres database container.

**Usage:**
```bash
dbos postgres stop
```

### `dbos workflow`
Manage DBOS workflows.

#### `dbos workflow list`
List workflows for your application.

**Options:**
- `-l, --limit <number>` - Limit the results returned (default: 10)
- `-o, --offset <number>` - Offset for pagination
- `-S, --status <status>` - Filter by status (PENDING, SUCCESS, ERROR, ENQUEUED, CANCELLED, or MAX_RECOVERY_ATTEMPTS_EXCEEDED)
- `-n, --name <name>` - Retrieve workflows with this name
- `-u, --user <user>` - Retrieve workflows run by this user
- `-v, --application-version <version>` - Retrieve workflows with this application version
- `-s, --start-time <timestamp>` - Retrieve workflows starting after this timestamp (ISO 8601)
- `-e, --end-time <timestamp>` - Retrieve workflows starting before this timestamp (ISO 8601)
- `-q, --queue <queue>` - Retrieve workflows on this queue
- `-Q, --queues-only` - Retrieve only queued workflows
- `-d, --sort-desc` - Sort the results in descending order (older first)

**Usage:**
```bash
dbos workflow list
dbos workflow list --limit 50 --status SUCCESS
dbos workflow list --name "ProcessOrder" --user "admin"
```

#### `dbos workflow get [workflow-id]`
Retrieve the status of a specific workflow.

**Usage:**
```bash
dbos workflow get abc123def456
```

Returns detailed information about the workflow including its status, start time, end time, and other metadata.

#### `dbos workflow steps [workflow-id]`
List the steps of a workflow.

**Usage:**
```bash
dbos workflow steps abc123def456
```

Shows all the steps executed within a workflow, their status, and execution details.

#### `dbos workflow cancel [workflow-id]`
Cancel a workflow so it is no longer automatically retried or restarted.

**Usage:**
```bash
dbos workflow cancel abc123def456
```

#### `dbos workflow resume [workflow-id]`
Resume a workflow that has been cancelled.

**Usage:**
```bash
dbos workflow resume abc123def456
```

#### `dbos workflow fork [workflow-id]`
Fork a workflow from the beginning or from a specific step.

**Options:**
- `-s, --step <number>` - Restart from this step (default: 1)
- `-f, --forked-workflow-id <id>` - Custom workflow ID for the forked workflow
- `-a, --application-version <version>` - Application version for the forked workflow

**Usage:**
```bash
dbos workflow fork abc123def456
dbos workflow fork abc123def456 --step 3
dbos workflow fork abc123def456 --forked-workflow-id custom-id-123
```

### `dbos version`
Show the version and exit.

**Usage:**
```bash
dbos version
```

## License

See the main project LICENSE file for details.