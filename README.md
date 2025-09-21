# DATABRICKS LAKEBASE ACCELERATOR

This project is designed to streamline the testing and migration of customer OLTP workloads to Lakebase, Databricks' managed Postgres solution. It is particularly focused on supporting reverse ETL use cases. The accelerator provides an easy way for users to evaluate Lakebase and quickly get started with their migration or testing needs.

## Qualifications
- Supported dataset size: up to 2TB in Postgres. **Be aware that Delta tables are highly compressed on cloud storage, when migrated to Lakebase, the physical table size may increase by 5x-10x.** 
Use [Cost Estimation with Table Size Calculation](#cost-estimation-with-table-size-calculation) to estimate table size
- Throughput: supports up to 100k queries per second (QPS)
- Reverse ETL is supported: data can be synchronized from Delta tables to Postgres in Lakebase to enable low-latency application serving.
- Column with UTF8 encoding is not supported

### Lakebase performance

1. Assume 1CU of compute capacity or 16GB Memory and uncompressed 1KB row size
   * Latency: <10ms
   * Max Connection: 1000
   * Read QPS: 10K point lookup. can vary [2k-30k] depending on data size & cache hit ratio
   * Write Rows/s (initial): ~15k 1KB  rows / sec per Capacity Unit (CU)
   * Write Rows/s (incremental): ~1.2k 1KB rows / sec per Capacity Unit (CU)

2. Max size: 2TB across all databases in the instance
3. Instances per workspace: 10 
4. Max connections: 1000 per database

See [LAKEBASE_BENCHMARK.md](LAKEBASE_BENCHMARK.md)

## Delta Sync

Delta table can be synced to Lakebase by 3 modes: Snapshot, Triggered, Continuous. See [Sync Mode Explained](https://docs.databricks.com/aws/en/oltp/sync-data/sync-table#sync-modes-explained)
   * Snapshot: The pipeline runs once to take a snapshot of the source table and copy it to the Postgres tables.This mode is 10 times more efficient than Triggered or Continuous sync modes because it recreates data from scratch. If you're modifying more than 10% of the source table, consider using this mode.
   * Triggered: User triggers the sync manually or according to a schedule, preferably after table is updated. 
   * Continuous: All new changes in the delta table are synced with the Postgres table with a pipeline running continuously. Can be expensive but ensure lowest lag between changes in Delta tables and Postgres tables (up to 10-15 secs lags).

To support Triggered or Continuous sync mode, the source table must have [Change data feed enabled](https://docs.databricks.com/aws/en/delta/delta-change-data-feed#enable-change-data-feed). Certain sources (like Views) do not support change data feed so they can only be synced in Snapshot mode.

Note: Sync mode cannot be changed after pipeline is created. Synced table need to be deleted and create a new one. 

Sync table creates a partitioned table in Postgres, to see the size of the underlying partitions, run below command:

```sql
select pg_total_relation_size(pi.inhrelid::regclass) as size, pc.relname from pg_inherits pi join pg_class pc on pi.inhparent = pc.oid;
```

The pg_total_relation_size of the table contains both the data and the primary key

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

3. Environment Variables (.env)

Create a `.env` file in the project root with the following variables:

````bash
# Required for query conversion (src/convert_queries.py)
DATABRICKS_ACCESS_TOKEN=your_databricks_pat
DATABRICKS_ENDPOINT=https://your-workspace.cloud.databricks.com/serving-endpoints
# LLM Model name for Query Conversion, defaults to databricks-meta-llama-3-1-70b-instruct
MODEL_NAME=databricks-meta-llama-3-1-70b-instruct

# Required for table size calculation (app/backend/services/lakebase_cost_estimator.py)
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
````

**Environment Variable Details:**
- `DATABRICKS_ACCESS_TOKEN`: Your Databricks Personal Access Token (used for both query conversion and table size calculation)
- `DATABRICKS_SERVER_HOSTNAME`: Your Databricks workspace hostname (e.g., `adb-1234567890123456.7.azuredatabricks.net`)
- `DATABRICKS_HTTP_PATH`: SQL warehouse HTTP path (e.g., `/sql/1.0/warehouses/abc123def456`)

**Note:** The same `DATABRICKS_ACCESS_TOKEN` is used for both query conversion (`src/convert_queries.py`) and table size calculation (`app/backend/services/lakebase_cost_estimator.py`). This simplifies configuration by using a single token for all Databricks API operations.

## Usage

### Cost Estimation

The project includes a comprehensive cost estimator (`app/backend/services/lakebase_cost_estimator.py`) that can calculate Lakebase Postgres costs based on workload characteristics. It also supports calculating actual uncompressed table sizes from Databricks Delta tables. 

#### Basic Cost Estimation

```bash
# Basic cost estimation using workload configuration
python app/backend/services/lakebase_cost_estimator.py --config workload_config.yml --output cost_report.json
```


#### Cost Estimation Output

The cost estimator provides detailed breakdown including:
- **Compute costs**: Main instance and readable secondaries
- **Storage costs**: Based on data size and retention policies
- **Sync costs**: Delta synchronization costs
- **Table size analysis**: Total compressed size and per-table details
- **Cost efficiency metrics**: Cost per GB, QPS, and Capacity Unit

### Table Configuration Generator

The project includes a table configuration generator (`app/backend/services/generate_synced_tables.py`) that creates Databricks bundle configuration files from workload definitions.

#### Basic Usage

```bash
# Generate synced tables from workload config
python app/backend/services/generate_synced_tables.py --config workload_config.yml

# Specify custom output path
python app/backend/services/generate_synced_tables.py --config workload_config.yml --output synced_tables.yml

# Enable verbose output for debugging
python app/backend/services/generate_synced_tables.py --config workload_config.yml --verbose
```

#### Features

- **Flexible input**: Accepts any workload configuration file with `tables_to_sync` section
- **Auto-generated output**: Automatically determines output path based on input file location
- **Custom output**: Allows specifying custom output file paths
- **Verbose mode**: Provides detailed error information for debugging
- **YAML validation**: Ensures proper formatting of generated configuration files


## Deployment

To deploy a development copy of this project, use below command:

```bash
$ databricks bundle deploy --target dev --debug
```

(Note that "dev" is the default target, so the `--target` parameter
is optional here. `--debug` flag is optional to view the deployment log)

This deploys everything that's defined for this project. Note that Lakebase instance will take ~3-5 minutes to spin up. You can view the deployment in Databricks workspace: Compute > 

Similarly, to deploy a production copy, use below command:

```bash
$ databricks bundle deploy --target prod
```

## Starting the Web Application

The project includes a full-stack web application for interactive workload configuration, cost estimation, and configuration file generation.

### Development Setup

#### Option 1: Full Stack Development (Recommended)

```bash
# Terminal 1 - Start Backend
cd app/backend
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp ../../.env.example .env  # Edit with your Databricks credentials
python main.py

# Terminal 2 - Start Frontend
cd app/frontend
npm install
npm start
```

- Backend API: http://localhost:8000
- Frontend App: http://localhost:3000
- API Docs: http://localhost:8000/docs

#### Option 2: Backend Only (API Service)

```bash
cd app/backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp ../../.env.example .env  # Edit with your credentials
python main.py
```

Access API documentation at: http://localhost:8000/docs

### Application Features

- **Interactive Configuration**: Web form for workload parameters
- **Real-time Cost Estimation**: Calculate Lakebase costs with table size analysis
- **Configuration Generation**: Download YAML files for Databricks bundle deployment
- **Professional UI**: Enterprise-grade interface with Ant Design components

For detailed deployment options including Databricks Apps, see [app/README.md](app/README.md).
