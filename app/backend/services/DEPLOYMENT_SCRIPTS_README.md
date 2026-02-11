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
python3 deploy_provisioned_pgbench_job.py
python3 deploy_autoscaling_pgbench_job.py
python3 deploy_provisioned_psycopg.py
python3 deploy_autoscaling_psycopg.py
```

## Scripts Overview

| Script | Purpose | Instance Type | Test Method |
|--------|---------|---------------|-------------|
| `deploy_provisioned_pgbench_job.py` | pgbench benchmark via Databricks Jobs | Provisioned | Databricks Job |
| `deploy_autoscaling_pgbench_job.py` | pgbench benchmark via Databricks Jobs | Autoscaling | Databricks Job |
| `deploy_provisioned_psycopg.py` | Concurrent query testing | Provisioned | Direct Python |
| `deploy_autoscaling_psycopg.py` | Concurrent query testing | Autoscaling | Direct Python |

## When to Use Each Script

### pgbench Scripts (Databricks Jobs)
Use when you want to:
- Run standardized pgbench benchmarks
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

## 1. Provisioned Lakebase with pgbench

### Usage

```bash
# Script automatically loads .env file
python3 deploy_provisioned_pgbench_job.py
```

### Required Configuration in `.env`

```bash
PROVISIONED_LAKEBASE_INSTANCE_NAME=lakebase-instance-name
PROVISIONED_LAKEBASE_DATABASE=databricks_postgres
PROVISIONED_DATABRICKS_WORKSPACE_URL=https://cust-success.cloud.databricks.com/
```

### Optional Configuration in `.env`

```bash
DATABRICKS_PROFILE=DEFAULT     # Databricks CLI profile
PGBENCH_CLIENTS=5              # Number of concurrent clients
PGBENCH_JOBS=4                 # Number of worker threads
PGBENCH_DURATION=30            # Test duration in seconds
DATABRICKS_CLUSTER_ID=...      # Use existing cluster (optional)
```

### What It Does

1. Connects to your Databricks workspace
2. Verifies the Lakebase instance exists
3. Creates a Databricks Job with pgbench installed
4. Submits the job with your test configuration
5. Monitors execution and displays results
6. Shows TPS, latency (avg, p95, p99), and per-query statistics

---

## 2. Autoscaling Lakebase with pgbench

### Usage

```bash
# Script automatically loads .env file
python3 deploy_autoscaling_pgbench_job.py
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
DATABRICKS_PROFILE=DEFAULT           # Databricks CLI profile
PGBENCH_CLIENTS=5                    # Number of concurrent clients
PGBENCH_JOBS=4                       # Number of worker threads
PGBENCH_DURATION=30                  # Test duration in seconds
DATABRICKS_CLUSTER_ID=...            # Use existing cluster (optional)
```

### What It Does

1. Validates compute endpoint hostname (ep-* format)
2. Creates PostgreSQL connection configuration
3. Submits pgbench job to Databricks
4. Monitors execution with real-time status updates
5. Displays comprehensive performance metrics

### Notes

- Autoscaling endpoints use direct PostgreSQL connections
- Hostname format: `ep-<id>.databricks.com`
- Supports both OAuth tokens and password authentication

---

## 3. Provisioned Lakebase with psycopg

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

## 4. Autoscaling Lakebase with psycopg

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
