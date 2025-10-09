# DATABRICKS LAKEBASE ACCELERATOR

This project is designed to streamline the testing and deployment of customer OLTP workloads to Lakebase, Databricks' managed Postgres solution. It is particularly focused on supporting reverse ETL use cases. The accelerator provides an easy way for users to evaluate Lakebase and quickly get started with their POC and testing needs.

## Prerequesites

- Python3.11
- Databricks Workspace Requirements:
   - Unity Catalog enabled: CREATE CATALOG, USE CATALOG, CREATE SCHEMA permission
   - Lakebase Service: CREATE DATABASE INSTANCE, USE DATABASE INSTANCE permission
   - Delta Tables: For source data synchronization
   - Databricks SQL Warehouse: For table size calculations (optional but recommended)
   - Databricks SDK: For programmatic workspace access

## Environments Setup

1. Setup Python virtual environment

```
# Install uv (if not already installed)
pip install uv
# or
curl -LsSf https://astral.sh/uv/install.sh | sh
```
Set up Python environment and install required packages
```
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

2. Install the Databricks CLI from <https://docs.databricks.com/dev-tools/cli/databricks-cli.html>

```bash
$ brew install databricks
$ databricks --version
```

Databricks CLI v0.267+ is required, if you have older version, upgrade the CLI version

```bash
$ brew update && brew upgrade databricks && databricks --version | cat
```

3. Authenticate to your Databricks workspace, if you have not done so already:

   #### Option A: Personal Access Token (PAT)
   **Configure CLI with PAT:**

   ```bash
   databricks configure --token --profile DEFAULT
   ```

   You'll be prompted for:
   - **Databricks Host**: `https://your-workspace.cloud.databricks.com`
   - **Token**: Paste your generated token

   This will update DEFAULT profile in `~/.databrickscfg`

   #### Option B: OAuth Authentication

   Configure OAuth:

   ```bash
   databricks auth login --host https://your-workspace.cloud.databricks.com --profile DEFAULT
   ```

   This will:

   - Open your browser for authentication
   - Create a profile in `~/.databrickscfg`
   - Store OAuth credentials securely

   #### Verify Databricks Auth

   ```bash
   $ databricks auth profiles
   ```

## Install pgbench for concurrency testing
pgbench comes bundled with PostgreSQL. Install PostgreSQL client tools:

#### macOS:
```bash
# Install PostgreSQL (includes pgbench)
brew install postgresql

# Verify installation
pgbench --version
```
## Application Features

The project includes a full-stack web application for interactive workload configuration, cost estimation, and deployment automation using the Databricks Python SDK.

- **🧮 Lakebase Calculator**: Interactive cost estimation with real table size calculation
- **🚀 Automatic Deployment**: Direct deployment using Databricks Python SDK
- **📁 Manual Deployment**: Generate and download Databricks Asset Bundle files
- **🧪 Concurrency Testing**: Upload and execute SQL queries for performance testing

## Option 1: Starting the Web Application on Databricks Apps

Follow instruction on [DEPLOY_WITH_DAB.md](./DEPLOY_WITH_DAB.md)

## Option 2: Starting the Web Application (self-hosted on local machine)

Ensure you have completed the [Environment Setup](#environments-setup) and authenticated with Databricks CLI.

Then on root rirectory, run
```
# build frontend
./npm-build.sh

# Start the app
python app.py
```

The app will run on host: http://0.0.0.0:8000

### Authentication

If self-host on local machine, authentication is handled via your Databricks CLI profiles, as set up in the [Environment Setup](#environments-setup) section. The backend uses these CLI profiles to authenticate with the Databricks Python SDK (WorkspaceClient) using provided user credential.

When running the app on Databricks, the service principal assigned to the app will perform all the actions, hence it might need following permissions: Database Instance Management (see [Database instance ACLs](https://docs.databricks.com/aws/en/security/auth/access-control/#database-instance-acls)), Unity Catalog privileges including CREATE CATALOG, USE CATALOG, and CREATE SCHEMA on the target catalog, SELECT on any source Delta tables to be synced, USE SCHEMA and CREATE TABLE on the storage catalog and schema for Lakeflow-synced Delta pipelines, databricks-superuser permission to query tables, and "Allow unrestricted cluster creation" enabled.