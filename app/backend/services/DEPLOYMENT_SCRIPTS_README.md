# Lakebase Deployment Scripts Usage Guide

This guide covers how to use the four deployment scripts for testing Lakebase performance.

## Quick Start

**All scripts automatically load environment variables from a `.env` file in the same directory.** Simply create your `.env` file from the provided template and run any script - no need to manually export variables!

```bash
# 1. Copy the template
cp environment_variables_template.txt .env

# 2. Edit .env with your configuration
nano .env

# 3. Run any script - it will automatically load the .env file
python3 deploy_pgbench_job.py
python3 deploy_provisioned_psycopg.py
python3 deploy_autoscaling_psycopg.py
```

## Scripts Overview

| Script | Purpose | Instance Type | Test Method |
|--------|---------|---------------|-------------|
| `deploy_pgbench_job.py` | pgbench benchmark via Databricks Jobs | Provisioned or Autoscaling | Databricks Job |
| `deploy_provisioned_psycopg.py` | Concurrent query testing | Provisioned | Direct Python |
| `deploy_autoscaling_psycopg.py` | Concurrent query testing | Autoscaling | Direct Python |

## When to Use Each Script

### pgbench Script (Databricks Jobs)
Use `deploy_pgbench_job.py` when you want to:
- Run standardized pgbench benchmarks (works for both Provisioned and Autoscaling Lakebase)
- Test TPS (transactions per second) and latency
- Have tests run remotely on Databricks infrastructure
- Get results through Databricks Jobs UI

### psycopg Scripts (Direct Execution)
Use when you want to:
- Test custom query patterns with parameters
- Control concurrency levels directly
- Run tests from your local machine
- Test connection pooling behavior

---

## 1. pgbench (Provisioned or Autoscaling)

Single script `deploy_pgbench_job.py` for both instance types. Uses PostgreSQL credentials (PGUSER, PGPASSWORD). Provide host either directly (PGHOST) or via instance name (LAKEBASE_INSTANCE_NAME).

### Usage

```bash
# Load .env and run
python3 deploy_pgbench_job.py
```

### Option A: Direct host (Autoscaling or any endpoint)

```bash
PGHOST=ep-xxx.databricks.com   # or provisioned instance hostname
PGUSER=analyst
PGPASSWORD=your-password
PGDATABASE=databricks_postgres  # optional, default above
python3 deploy_pgbench_job.py
```

### Option B: Provisioned instance name (host resolved via API)

```bash
LAKEBASE_INSTANCE_NAME=my-instance
PGUSER=analyst
PGPASSWORD=your-password
PGDATABASE=databricks_postgres  # optional
python3 deploy_pgbench_job.py
```

### Optional configuration

```bash
PGPORT=5432                    # default 5432
PGSSLMODE=require              # default require
DATABRICKS_PROFILE=DEFAULT
PGBENCH_CLIENTS=5
PGBENCH_JOBS=4
PGBENCH_DURATION=30
DATABRICKS_CLUSTER_ID=...      # optional; omit for auto job cluster
```

### What It Does

1. Connects to your Databricks workspace (and resolves host from instance name if using LAKEBASE_INSTANCE_NAME)
2. Submits a pgbench job with your PostgreSQL credentials and test config
3. Monitors execution with real-time status updates
4. Displays TPS, latency, and per-query statistics

---

## 2. Provisioned Lakebase with psycopg

### Usage

```bash
# Script automatically loads .env file
python3 deploy_provisioned_psycopg.py
```

### Required Configuration in `.env`

```bash
PROVISIONED_LAKEBASE_INSTANCE_NAME=lakebase-instance-name
PROVISIONED_LAKEBASE_DATABASE=databricks_postgres
```

### Optional Configuration in `.env`

```bash
CONCURRENCY_LEVEL=10        # Number of concurrent queries
```

**Note**: Connection pool settings are calculated automatically based on concurrency level.

### What It Does

1. Initializes async connection pool using SQLAlchemy + asyncpg
2. Creates parameterized test queries
3. Executes queries concurrently with specified concurrency level
4. Measures latency, throughput, and success rates
5. Displays per-query breakdown and recommendations

### Sample Queries

The script includes:
- Point lookups with parameters
- Range queries with multiple parameters
- Queries without parameters
- Each query can have multiple test scenarios

---

## 3. Autoscaling Lakebase with psycopg

### Usage

```bash
# Script automatically loads .env file
python3 deploy_autoscaling_psycopg.py
```

### Required Configuration in `.env`

```bash
AUTOSCALING_PGHOST=your-autosc-lakebase-123.database.us-west-2.cloud.databricks.com
AUTOSCALING_PGDATABASE=databricks_postgres
AUTOSCALING_PGUSER=analyst
AUTOSCALING_PGPASSWORD=erh_OLYf4Ko3tAfh
```

### Optional Configuration in `.env`

```bash
AUTOSCALING_PGPORT=5432              # PostgreSQL port
AUTOSCALING_PGSSLMODE=require        # SSL mode
AUTOSCALING_PGCHANNELBINDING=require # Channel binding
```

**Note**: This script does NOT read concurrency or pool settings from `.env`. Use command-line arguments instead.

### Command-Line Arguments (Required for Concurrency Control)

Override defaults or control test parameters via command-line:

```bash
python3 deploy_autoscaling_psycopg.py \
  --concurrency 20 \      # Concurrency level (default: 10)
  --pool-size 8 \         # Connection pool base size (default: 5)
  --max-overflow 15       # Max additional connections (default: 10)
```

You can also override connection settings:

```bash
python3 deploy_autoscaling_psycopg.py \
  --host ep-abc-123.databricks.com \
  --user analyst \
  --password your-password \
  --concurrency 20
```

### What It Does

1. Establishes direct PostgreSQL connection to Autoscaling endpoint
2. Supports OAuth or native password authentication
3. Creates async connection pool
4. Executes concurrent queries with timing
5. Calculates throughput, latency percentiles, and success rates
6. Shows per-query statistics

### OAuth vs Native Authentication

**OAuth Mode** (default):
- Uses Databricks identity
- Requires OAuth token from Lakebase UI
- User is your Databricks email

**Native Mode** (--no-oauth flag):
- Uses PostgreSQL role/password
- Direct database authentication



## Output Interpretation

### pgbench Scripts Output

```
✅ Job submitted successfully!
   Job ID: 123456789
   Run ID: 987654321
   
📊 Test Results:
   Performance Metrics:
   - TPS: 1234.56
   - Avg Latency: 45.67ms
   - P95 Latency: 78.90ms
   - P99 Latency: 123.45ms
   - Total Transactions: 37000
```

**Key Metrics:**
- **TPS**: Transactions per second (higher is better)
- **Avg Latency**: Average response time
- **P95/P99**: 95th/99th percentile latency (tail latencies)

### psycopg Scripts Output

```
📊 TEST RESULTS
⏱️  Duration: 15.23s
🔢 Total Queries: 500
✅ Successful: 495
❌ Failed: 5
📈 Success Rate: 99.00%

⚡ Performance Metrics:
   Throughput: 32.83 queries/sec
   Avg Latency: 45.67ms
   P95 Latency: 78.90ms
```

**Key Metrics:**
- **Throughput**: Queries per second
- **Success Rate**: Percentage of successful queries
- **Latencies**: Response time distribution
