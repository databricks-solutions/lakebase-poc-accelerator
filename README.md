# DATABRICKS LAKEBASE ACCELERATOR

This project is designed to streamline the testing and migration of customer OLTP workloads to Lakebase, Databricks' managed Postgres solution. It is particularly focused on supporting reverse ETL use cases. The accelerator provides an easy way for users to evaluate Lakebase and quickly get started with their migration or testing needs.

## Qualifications
- Supported dataset size: up to 2TB in Postgres. **Be aware that Delta tables are highly compressed on cloud storage, when migrated to Lakebase, the physical table size may increase by 10x to 20x.**
- Throughput: supports up to 100k queries per second (QPS)
- Reverse ETL is supported: data can be synchronized from Delta tables to Postgres in Lakebase to enable low-latency application serving.

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

   **Generate Personal Access Token:**
      - Log into your Databricks workspace
      - Click on your username in the top-right corner
      - SELECT **User Settings** → **Developer** → **Access tokens**
      - Click **Generate new token**
      - Give it a name (e.g., "Local Development") and set expiration
      - Copy the generated token

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

Create a `.env` file in the project root with the following variables (used by `src/convert_queries.py`):

````bash
DATABRICKS_TOKEN=your_databricks_pat
DATABRICKS_ENDPOINT=https://your-workspace.cloud.databricks.com/serving-endpoints
# LLM Model name for Query Conversion, defaults to databricks-meta-llama-3-1-70b-instruct
MODEL_NAME=databricks-meta-llama-3-1-70b-instruct
````


## 🚀 Quickstarts (optional)

This project includes a separate Databricks bundle configuration in the `quickstarts/` folder that contains starter resources and examples for testing the solution

### Deploying Quickstarts Resources

To deploy the quickstarts bundle instead of the main project bundle, change to quickstarts directory and run below
```bash
cd quickstarts
databricks bundle deploy
```

### Quickstarts Bundle Contents

For detailed information about the quickstarts features, see the [quickstarts/README.md](quickstarts/README.md) file.


## Usage



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
