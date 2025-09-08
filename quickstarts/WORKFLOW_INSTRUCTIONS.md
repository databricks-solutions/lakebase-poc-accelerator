# Lakebase Postgres Testing Quickstarts - AI Implementation Guide

## Project Overview

**This file provides workflow instructions for automated implementation of the Lakebase Postgres testing quickstart project using Cursor.**

Create a comprehensive quickstart solution for testing Lakebase (managed Postgres database) on Databricks. This includes sizing guidance, query conversion, configuration generation, and concurrency testing.

**Note for AI Agent:** This implementation requires Postgres expertise. Provide detailed explanations and best practices for each Postgres-related task.

## Implementation Tasks

### Task 1: Create Sizing Guide
I want to help users decide Postgres instance sizing and estimate Postgres cost based on a user  inputs. 

Step 1: Create a workload_sizing.yml file that collect user input:

Database Instance Unit
- Bulk writes (rows per second) (delta sync)
- Continuous writes (rows per second)
- Reads per second (point lookups) (qps)
- Number of Readable Secondaries (On top of main instance)
- Readable Secondary size (CU)
Database Storage
- Data stored in the database (GB) 
- Estimated data deleted each day (GB)
Delta Synchronization
- Number of Continuous pipeline (optional)
- Expected data to be written each time (Triggered or Snapshot) (GB)
- Sync Mode (Triggered or Snapshot)
- Frequency of running the pipeline (Triggered or Snapshot) (Per week, Per day, Per Hour)

Step 2: Provide Python scripts to estimate cost of Lakebase

- The number of CU for Postgres sizing will be based on below formula

  ```
  bulkCU = bulk / 14000; (Bulk writes (rows per second) (delta sync))
  contCU = cont / 1500;
  readCU = read / 10000;
  totalCU = bulkCU + contCU + readCU;

  // Round up to nearest valid tier: 1, 2, 4, or 8 CU
  if (totalCU <= 1) {
    return "1CU";
  } else if (totalCU <= 2) {
    return "2CU";
  } else if (totalCU <= 4) {
    return "4CU";
  } else {
    return "8CU";
  }
  ```

- compute_cost_monthly = 3500/12 * (CUs + Number of Readable Secondaries (On top of main instance) * Readable Secondary size)

- storage_cost = 0.35 * Data stored in the database (GB) 

- Delta Synchronization = Total Continuous Synchronization Costs (monthly) + Triggered Synchronization Costs (monthly)
  - Total Continuous Synchronization Costs (monthly) = $548 * Number of continuous pipelines
  - Triggered Synchronization Costs (monthly) = 0.75 * Sync time in hours * (7 if run per day, 168 if run per hour, 1 if run per week)
  - Sync time in hour is estimated based on estimate_sync_time python function
  - 7 if run per day, 168 if run per hour, 1 if run per week is Frequency of running the pipeline

```
def estimate_sync_time(expected_data_gb, cus, sync_mode):
    """
    Estimate sync time in hours.

    Parameters:
        expected_data_gb (float): Expected data to be written (GB).
        cus (int): Capacity Units (1, 2, or 4).
        sync_mode (str): "Snapshot" or "Triggered".

    Returns:
        float: Estimated time in hours.
    """
    # fixed overhead: 10 minutes
    overhead_hours = 10 / 60  

    # throughput table (GB per hour)
    throughput = {
        "Snapshot": {1: 54, 2: 108, 4: 216},
        "Triggered": {1: 4.5, 2: 9, 4: 18}
    }

    if sync_mode not in throughput:
        raise ValueError("Invalid sync mode. Use 'Snapshot' or 'Triggered'.")
    if cus not in throughput[sync_mode]:
        raise ValueError("Invalid CU. Use 1, 2, or 4.")

    transfer_rate = throughput[sync_mode][cus]

    # formula: overhead + (data ÷ rate)
    return overhead_hours + (expected_data_gb / transfer_rate)
```


Step 3: Create a short benchmark guide (LAKEBASE_BENCHMARK.md) to summarize Lakebase performance for light reading. Keep this short and to the point: 

1. Benchmarks (loading via sync)
- Synced table: initial ingestion (snapshot, first run of triggered/continuous, full refreshes)  (latest as of 2024-12-13, doc)
= ~15k 1KB  rows / sec per Capacity Unit (CU)
- Synced table: Incremental ingestion (triggered/continuous updates)  (latest as of 2024-12-13)
= ~1.2k 1KB rows / sec per Capacity Unit (CU)

2. Benchmarks (read/write from the application)

YCSB point (YCSB, or the Yahoo! Cloud Serving Benchmark, is an open-source, standardized benchmark framework designed to evaluate the performance of cloud-based data storage systems, particularly NoSQL and key-value databases)
- ~1.7k rows /CU (if data does not fit in RAM)
- 30k rows @ 1KB / CU (if data fits in RAM)

PG sequential scan (as of Jun 23, 2025, doc)
- no data cached in RAM: ~ 18 MB/s/CU
- data in RAM: ~2GB/s/CU

PageServer  (latest as of 2024-12-13)
PageServer is the service that:
Stores all table/index pages (the “durable storage” for Postgres data).
Serves pages on demand to Postgres compute nodes via the GetPage protocol.
Reconstructs historical versions of pages using WAL from safekeepers (time travel).
Handles garbage collection and compaction of data.
- 176K GetPage QPS on 16-cores
- GetPage throttling (timeline_get_throttle): 5.5k / CU 
- Backpressure  
  * max_replication_write_lag=500MB
  * max_replication_flush_lag=10GB

Safekeeper
- max_wal_rate:
  - 1 CU = 25 MB/s, 2CU = 50MB/s, 4CU = 75MB/s, 8CU = 100 MB/s
  - FYI: this is the limiting factor for workloads that write a lot of large rows to PG (row size >100KB)


### Task 2: Create Configuration System
**Files to create:**
1. `quickstarts/config_template.yaml` - Template configuration file
2. `quickstarts/generate_databricks_yml.py` - Python script to generate databricks.yml

**Configuration Template Requirements:**
- `database_instance`: Lakebase Postgres instance identifier
- `database_catalog`: Target Databricks catalog name
- `postgres_settings`: Postgres-specific configurations
  - Connection parameters (host, port, database, ssl settings)
  - Performance tuning (max_connections, shared_buffers, etc.)
- `synced_tables`: List of tables to sync between Postgres and Delta
  - Source Postgres table name
  - Target Delta table schema
  - Sync frequency and method
- `performance_settings`: CU allocation and Postgres optimization

**Python Script Requirements:**
- Read YAML configuration file
- Validate Postgres connection parameters
- Generate databricks.yml with Postgres-specific workflows
- Include error handling for invalid Postgres configurations
- Support multiple table sync configurations
- Generate appropriate Databricks job definitions for Postgres integration

### Task 3: Query Conversion System
**Files to create:**
1. `quickstarts/convert_queries.py` - Main query conversion script
2. `quickstarts/queries/target/` - Directory for converted Postgres queries

**Requirements:**
- Read all SQL files from `quickstarts/queries/source/`
- Use Databricks LLM endpoint to convert queries from Databricks SQL to Postgres SQL
- Handle Postgres-specific syntax differences:
  - Date/time functions
  - String functions
  - Window functions
  - Data types (BIGINT vs INTEGER, etc.)
  - LIMIT syntax
- Save converted queries to `quickstarts/queries/target/`
- Implement syntax validation (without execution)
- Generate conversion report with any issues or warnings

**Template code to modify:**
```python
from openai import OpenAI
import os
import glob
from pathlib import Path

# Use provided Databricks endpoint and model
client = OpenAI(
    api_key=os.environ.get('DATABRICKS_TOKEN'),
    base_url="https://e2-demo-field-eng.cloud.databricks.com/serving-endpoints"
)

# Process all .sql files in queries/source/
# Convert each query with Postgres-specific instructions
# Validate syntax and save to queries/target/
```

### Task 4: Concurrency Testing System
**Files to create:**
1. `quickstarts/concurrency_config.yaml` - Concurrency test configuration
2. `quickstarts/run_concurrency_test.py` - Concurrency testing script

**Concurrency Configuration Requirements:**
- `test_scenarios`: List of test scenarios
  - Scenario name and description
  - Number of concurrent connections
  - Query patterns to execute
  - Test duration
  - Ramp-up/ramp-down patterns
- `postgres_connection`: Connection details for test database
- `queries`: List of queries to use in testing
- `metrics`: Performance metrics to collect
  - Response time percentiles
  - Throughput (QPS)
  - Connection pool utilization
  - Error rates

**Python Script Requirements:**
- Read concurrency configuration
- Establish multiple Postgres connections
- Execute queries concurrently using threading/asyncio
- Collect performance metrics
- Generate detailed test report
- Handle connection failures and timeouts
- Implement proper connection pooling

### Task 5: Create Documentation
**File to create:** `quickstarts/README.md`

**Content requirements:**
- Quick start guide for Postgres testing on Databricks
- Prerequisites (Postgres knowledge, Databricks setup)
- Step-by-step usage instructions
- Configuration examples for different scenarios
- Troubleshooting section for common Postgres issues
- Links to sizing guide and other resources

## File Structure to Create
```
quickstarts/
├── README.md
├── sizing_guide.md
├── config_template.yaml
├── generate_databricks_yml.py
├── convert_queries.py
├── concurrency_config.yaml
├── run_concurrency_test.py
└── queries/
    ├── source/           # Existing Databricks SQL queries
    └── target/           # Converted Postgres queries
```

## Implementation Order
1. **Create Postgres sizing guide** with CU recommendations and memory configurations
2. **Create configuration system** (YAML template + Python script)
3. **Implement query conversion system** using Databricks LLM
4. **Create concurrency testing framework** with config and execution script
5. **Write comprehensive documentation** with Postgres-specific guidance
6. **Test all components** with existing queries and validate outputs

## Validation Criteria
- [ ] Sizing guide provides Postgres-specific CU recommendations
- [ ] Configuration template covers Postgres connection and performance settings
- [ ] Python script generates valid databricks.yml with Postgres workflows
- [ ] Query conversion successfully processes queries from source folder
- [ ] Converted queries use proper Postgres syntax
- [ ] Concurrency testing framework handles multiple scenarios
- [ ] Documentation is complete and Postgres-focused
- [ ] All components integrate properly

## Technical Requirements
**General:**
- Use Python 3.8+ for all scripts
- Include comprehensive error handling and logging
- Add detailed comments explaining Postgres-specific logic
- Follow YAML best practices for configuration files

**Postgres-Specific:**
- Use psycopg2 or asyncpg for database connections
- Implement proper connection pooling (pgbouncer patterns)
- Handle Postgres-specific data types and functions
- Include SSL/TLS connection options
- Consider Postgres performance tuning parameters

**LLM Integration:**
- Use provided Databricks endpoint configuration
- Implement retry logic for API calls
- Handle rate limiting and token limits
- Validate converted SQL syntax

---
**Instructions for AI Assistant:**
When implementing this workflow, please:

1. **Provide Postgres expertise** - Include detailed explanations of Postgres concepts, performance tuning, and best practices
2. **Follow implementation order** - Complete each task fully before moving to the next
3. **Focus on practical usage** - Create working examples that users can immediately apply
4. **Include error handling** - Add comprehensive validation and error messages
5. **Test systematically** - Validate each component works with existing queries
6. **Document thoroughly** - Explain Postgres-specific configurations and decisions
7. **Use existing structure** - Reference the current databricks.yml and queries for consistency
