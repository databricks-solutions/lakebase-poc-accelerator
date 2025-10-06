# DATABRICKS LAKEBASE ACCELERATOR

This project is designed to streamline the testing and deployment of customer OLTP workloads to Lakebase, Databricks' managed Postgres solution. It is particularly focused on supporting reverse ETL use cases. The accelerator provides an easy way for users to evaluate Lakebase and quickly get started with their POC and testing needs.

## Prerequesites

- Python3.13
- Databricks Workspace Requirements:
   - Unity Catalog enabled: CREATE CATALOG, USE CATALOG, CREATE SCHEMA permission
   - Lakebase Service: CREATE DATABASE INSTANCE, USE DATABASE INSTANCE permission
   - Delta Tables: For source data synchronization
   - Databricks SQL Warehouse: For table size calculations (optional but recommended)
   - Databricks SDK: For programmatic workspace access

## Environments Setup

1. Setup Python virtual environment

```
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
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

## Starting the Web Application

The project includes a full-stack web application for interactive workload configuration, cost estimation, and deployment automation using the Databricks Python SDK.

### Prerequisites

Ensure you have completed the [Environment Setup](#environments-setup) and authenticated with Databricks CLI.

### Development Setup

#### Full Stack Development

```bash
# Terminal 1 - Start Backend API
cd app/backend
# Run development server
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# Terminal 2 - Start Frontend
cd app/frontend
npm install
npm start
```

**Access Points:**
- **Frontend App**: http://localhost:3000
- **Backend API**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs


### Application Features

- **🧮 Lakebase Calculator**: Interactive cost estimation with real table size calculation
- **🚀 Automatic Deployment**: Direct deployment using Databricks Python SDK
- **📁 Manual Deployment**: Generate and download Databricks Asset Bundle files
- **🧪 Concurrency Testing**: Upload and execute SQL queries for performance testing

### Authentication

Authentication is handled via your Databricks CLI profiles, as set up in the [Environment Setup](#environments-setup) section. The backend uses these CLI profiles to authenticate with the Databricks Python SDK (WorkspaceClient). No extra environment variables or config files are required.