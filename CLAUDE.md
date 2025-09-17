# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the Databricks Lakebase Accelerator - a project designed to streamline testing and migration of customer OLTP workloads to Lakebase, Databricks' managed Postgres solution. The accelerator focuses on reverse ETL use cases and provides cost estimation, table synchronization, and query conversion capabilities.

## Architecture

The project follows a Databricks Asset Bundle structure with:

- **Core Scripts** (`src/`): Python utilities for cost estimation, table generation, and query conversion
- **Resources** (`resources/`): Databricks bundle YAML configurations for Lakebase instances and synced tables
- **Configuration Management**: YAML-based workload configurations and Databricks bundle definitions

### Key Components

1. **Cost Estimator** (`src/lakebase_cost_estimator.py`): Calculates Lakebase Postgres costs based on workload characteristics, with optional real table size calculation from Databricks
2. **Table Generator** (`src/generate_synced_tables.py`): Generates Databricks bundle configurations from workload definitions
3. **Query Converter** (`src/convert_queries.py`): Converts Databricks SQL to Postgres-compatible SQL using LLM endpoints
4. **Lakebase Connection Service**: Singleton-pattern database connection service for Postgres queries

## Common Commands

### Environment Setup
```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On macOS/Linux

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env  # Edit with your credentials
```

### Cost Estimation
```bash
# Cost estimation with automatic table size calculation
python src/lakebase_cost_estimator.py --config workload_config.yml --output cost_report.json

# Table size calculation happens automatically when .env contains Databricks credentials:
# DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, DATABRICKS_ACCESS_TOKEN
```

### Table Configuration Generation
```bash
# Generate synced tables configuration
python src/generate_synced_tables.py --config workload_config.yml

# With custom output path
python src/generate_synced_tables.py --config workload_config.yml --output synced_tables.yml
```

### Query Conversion
```bash
# Convert directory of SQL files
python src/convert_queries.py --source-dir queries/source --target-dir queries/target

# Convert single file
python src/convert_queries.py --file queries/source/query.sql --output queries/target/converted.sql
```

### Databricks Bundle Operations
```bash
# Deploy development environment
databricks bundle deploy --target dev --debug

# Deploy production environment
databricks bundle deploy --target prod

# Authenticate with Databricks
databricks configure --token --profile DEFAULT
# or
databricks auth login --host https://your-workspace.cloud.databricks.com --profile DEFAULT
```

### Testing
```bash
# Run cost estimator unit tests
python src/_cost_estimator_unittest.py

# Run concurrency tests (requires deployed Lakebase instance)
python src/run_concurrency_test.py
```

## Development Guidelines

### Python Code Standards
- Run all Python code in virtual environment (`.venv`)
- Use meaningful variable and function names with type hints
- Include proper error handling with try-catch blocks
- Use environment variables for sensitive credentials (.env file)
- Follow PEP 8 style guidelines
- Ignore the `scratch/` folder - it's for temporary work and testing

### Environment Variables Required
```bash
# For query conversion
DATABRICKS_ACCESS_TOKEN=your_databricks_pat
DATABRICKS_ENDPOINT=https://your-workspace.cloud.databricks.com/serving-endpoints
MODEL_NAME=databricks-meta-llama-3-1-70b-instruct  # Optional, defaults to this

# For table size calculation
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
```

### Lakebase Database Connection Pattern
When implementing database connections, use singleton pattern with:
- Single connection instance per service
- Connection refresh after 59 minutes
- Always call both `cursor.fetchall()` and `connection.commit()` for all queries
- Use `RETURNING *` clauses for INSERT/UPDATE operations

### Configuration Files
- Workload configurations use YAML format with sections for database_instance, database_storage, and delta_synchronization
- Table sync configurations specify primary_keys and scheduling_policy (SNAPSHOT, TRIGGERED, or CONTINUOUS)
- Databricks bundle configurations follow standard asset bundle structure

## Project Structure

```
├── src/                          # Core Python utilities
│   ├── lakebase_cost_estimator.py    # Cost calculation with table sizing
│   ├── generate_synced_tables.py     # Bundle config generation
│   ├── convert_queries.py            # SQL conversion utility
│   └── run_concurrency_test.py       # Performance testing
├── resources/                    # Databricks bundle resources
│   ├── lakebase_instance.yml         # Lakebase instance definition
│   └── synced_delta_tables.yml       # Table sync configurations
├── scratch/                      # Temporary work folder (ignore)
├── databricks.yml               # Main bundle configuration
├── requirements.txt             # Python dependencies
└── .env                        # Environment variables (create from template)
```

## Important Notes

- Lakebase instances take 3-5 minutes to spin up after deployment
- Delta tables require Change Data Feed enabled for TRIGGERED/CONTINUOUS sync modes
- Table size calculations provide uncompressed sizes close to Postgres storage requirements
- Sync modes cannot be changed after pipeline creation - requires table deletion and recreation
- Maximum supported dataset size is 2TB in Postgres (may expand 5x-10x when migrated due to compression differences)