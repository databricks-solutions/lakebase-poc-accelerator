# DATABRICKS LAKEBASE ACCELERATOR

This project is designed to streamline the testing and migration of customer OLTP workloads to Lakebase, Databricks' managed Postgres solution. It is particularly focused on supporting reverse ETL use cases. The accelerator provides an easy way for users to evaluate Lakebase and quickly get started with their migration or testing needs.


## Environments Setup 

1. Install the Databricks CLI from <https://docs.databricks.com/dev-tools/cli/databricks-cli.html>

```bash
$ brew install databricks
$ databricks --version
```

Databricks CLI v0.267+ is required, if you have older version, upgrade the CLI version

```bash
$ brew update && brew upgrade databricks && databricks --version | cat
```

2. Authenticate to your Databricks workspace, if you have not done so already:

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

## Starting the Web Application

The project includes a full-stack web application for interactive workload configuration, cost estimation, and deployment automation using the Databricks Python SDK.

### Prerequisites

Ensure you have completed the [Environment Setup](#environments-setup) and authenticated with Databricks CLI.

### Development Setup

#### Full Stack Development

```bash
# Terminal 1 - Start Backend API
cd app/backend
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r ../requirements.txt
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
- **📊 Real-time Progress**: Live deployment progress tracking with detailed status updates

**Key Improvements:**
- **SDK-based deployment**: Direct deployment without CLI bundle commands
- **Enhanced UI feedback**: Real-time progress tracking and better error handling
- **Table size calculation**: Automatic calculation of actual Delta table sizes for accurate cost estimation

### Authentication

Authentication is handled via your Databricks CLI profiles, as set up in the [Environment Setup](#environments-setup) section. The backend uses these CLI profiles to authenticate with the Databricks Python SDK (WorkspaceClient). No extra environment variables or config files are required.