# pgbench Usage Guide for Lakebase Testing

This guide provides comprehensive instructions for using pgbench to test PostgreSQL performance against your Databricks Lakebase instance.

## 🔧 pgbench Setup and Usage Instructions

### 1. Installation

pgbench comes bundled with PostgreSQL. Install PostgreSQL client tools:

#### macOS:
```bash
# Install PostgreSQL (includes pgbench)
brew install postgresql

# Verify installation
pgbench --version
```

### 2. Basic Connection Test

First, test connectivity to your Lakebase instance:

```bash
# Test basic connection
psql -h your-lakebase-instance.databricks.com \
     -p 5432 \
     -U your-username \
     -d your-database \
     -c "SELECT version();"
```

You can locate your connection parameters in the **Connection Details** tab of your Lakebase instance within the Databricks Workspace. For the password, use the value provided by **Get OAuth Token**.

### 3. Initialize pgbench Test Database

pgbench requires specific tables for its built-in workload:

```bash
# Initialize pgbench tables (optional for built-in tests)
pgbench -i -s 10 \
  -h your-lakebase-instance.databricks.com \
  -p 5432 \
  -U your-username \
  your-database

# -i: initialize mode
# -s 10: scale factor (10 = ~1MB test data)
```

**Note:** For Lakebase, you might want to skip initialization if testing existing tables.

### 4. Basic Performance Tests

#### Simple Throughput Test:
```bash
# 10 clients, 60 seconds, built-in workload
pgbench -c 10 -T 60 \
  -h your-lakebase-instance.databricks.com \
  -p 5432 \
  -U your-username \
  your-database
```

#### High Concurrency Test:
```bash
# 50 clients, 4 worker threads, 5-minute test
pgbench -c 50 -j 4 -T 300 -P 10 \
  -h your-lakebase-instance.databricks.com \
  -p 5432 \
  -U your-username \
  your-database

# -c 50: 50 concurrent clients
# -j 4: 4 worker threads
# -T 300: run for 300 seconds
# -P 10: progress report every 10 seconds
```

#### Read-Only Test:
```bash
# Test read performance only
pgbench -c 20 -T 120 -S \
  -h your-lakebase-instance.databricks.com \
  -p 5432 \
  -U your-username \
  your-database

# -S: select-only (read-only) test
```

### 5. Custom Query Testing

Create a custom SQL file for your specific workload:

#### Create custom_queries.sql:
```sql
-- Custom query file
\set customer_id random(1, 1000000)
\set order_date '2024-01-01'::date + random(0, 365) * interval '1 day'

SELECT c.customer_name, o.order_total
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE c.customer_id = :customer_id
  AND o.order_date >= :order_date;
```

#### Run Custom Queries:
```bash
# Test with custom queries
pgbench -c 25 -T 180 \
  -f custom_queries.sql \
  -h your-lakebase-instance.databricks.com \
  -p 5432 \
  -U your-username \
  your-database

# -f: specify custom script file
```

pgbench -c 10 -T 60 \
  -h instance-703e1295-f2c4-488f-bf9c-7d32908e9491.database.azuredatabricks.net\
  -p 5432 \
  -U anhhoang.chu@databricks.com \
  databricks_postgres \
  -f scratch/pgbench_queries/pgbench_customer_query.sql

### 6. Advanced Testing Scenarios

#### Connection Pool Testing:
```bash
# Test connection scaling
for clients in 5 10 25 50 100; do
  echo "Testing with $clients clients:"
  pgbench -c $clients -T 60 -P 10 \
    -h your-lakebase-instance.databricks.com \
    -p 5432 \
    -U your-username \
    your-database
  echo "---"
done
```

#### Latency Analysis:
```bash
# Detailed latency reporting
pgbench -c 20 -T 300 \
  --sampling-rate=1 \
  --aggregate-interval=10 \
  -h your-lakebase-instance.databricks.com \
  -p 5432 \
  -U your-username \
  your-database

# --sampling-rate=1: sample all transactions
# --aggregate-interval=10: report every 10 seconds
```

#### Rate-Limited Testing:
```bash
# Test at specific rate (100 TPS)
pgbench -c 10 -T 300 \
  --rate=100 \
  -h your-lakebase-instance.databricks.com \
  -p 5432 \
  -U your-username \
  your-database

# --rate=100: limit to 100 transactions per second
```

### 7. Lakebase-Specific Considerations

#### Authentication Setup:
Since Lakebase uses token-based auth, you'll need to handle credentials:

```bash
# Set password via environment variable
export PGPASSWORD="your-lakebase-token"

# Or use .pgpass file
echo "your-lakebase-instance.databricks.com:5432:your-database:your-username:your-token" >> ~/.pgpass
chmod 600 ~/.pgpass
```

#### SSL Configuration:
```bash
# Enable SSL for Lakebase
pgbench -c 10 -T 60 \
  -h your-lakebase-instance.databricks.com \
  -p 5432 \
  -U your-username \
  your-database \
  "sslmode=require"
```

### 8. Interpreting Results

#### Sample Output:
```
transaction type: <builtin: TPC-B (sort of)>
scaling factor: 1
query mode: simple
number of clients: 10
number of threads: 1
duration: 60 s
number of transactions actually processed: 12843
latency average = 46.672 ms
latency stddev = 25.330 ms
tps = 214.050000 (including connections establishing)
tps = 214.102520 (excluding connections establishing)
```

#### Key Metrics:
- **TPS**: Transactions per second
- **Latency average**: Mean response time
- **Latency stddev**: Response time variance
- **Connection overhead**: Time spent establishing connections

### 9. Comparison with Lakebase Framework

#### Run Both Tools for Comparison:

**pgbench:**
```bash
pgbench -c 50 -T 300 -P 30 -f your_queries.sql your-database
```

**Lakebase Framework:**
- Upload same queries via web interface
- Set concurrency: 50
- Duration: 300 seconds
- Compare results

### 10. Common pgbench Options Reference

| Option | Description | Example |
|--------|-------------|---------|
| `-c N` | Number of clients | `-c 50` |
| `-j N` | Number of worker threads | `-j 4` |
| `-T N` | Duration in seconds | `-T 300` |
| `-t N` | Number of transactions per client | `-t 1000` |
| `-S` | Select-only (read-only) | `-S` |
| `-P N` | Progress report interval | `-P 10` |
| `-f FILE` | Custom script file | `-f queries.sql` |
| `--rate=N` | Rate limit (TPS) | `--rate=100` |
| `-i` | Initialize test tables | `-i -s 10` |

### 11. Best Practices for Lakebase Testing

1. **Start Small**: Begin with low concurrency (5-10 clients)
2. **Monitor Resources**: Watch Lakebase instance metrics during tests
3. **Use Realistic Queries**: Test with your actual application queries
4. **Test Different Patterns**: Mix read/write operations
5. **Consider Token Refresh**: Long tests may need credential renewal
6. **Respect Limits**: Stay under Lakebase's 1000 connection limit

## 📊 Framework Comparison

### pgbench vs Lakebase Concurrency Framework

| Aspect | **pgbench** | **Lakebase Framework** |
|--------|-------------|----------------------|
| **Performance** | ✅ Lower overhead (C implementation) | ❌ Higher overhead (Python) |
| **Ease of Use** | ❌ Command-line only | ✅ Web interface |
| **Enterprise Integration** | ❌ Generic PostgreSQL | ✅ Databricks OAuth |
| **Custom Queries** | ✅ Script files | ✅ Web upload |
| **Real-time Monitoring** | ❌ Text output | ✅ Live progress |
| **Statistical Analysis** | ✅ Advanced metrics | ❌ Basic percentiles |

### When to Use Each Tool

**Use pgbench for:**
- Raw performance baselines
- Standardized benchmarks
- Maximum throughput testing
- Technical performance validation

**Use Lakebase Framework for:**
- Application-specific testing
- User-friendly demos
- Integrated Databricks workflows
- Business stakeholder presentations

## 🔗 Related Documentation

- [Main README](README.md) - Project overview and setup
- [Databricks Lakebase Documentation](https://docs.databricks.com/en/lakehouse/lakebase.html)
- [PostgreSQL pgbench Documentation](https://www.postgresql.org/docs/current/pgbench.html)

## 🛠️ Troubleshooting

### Common Issues:

1. **Connection Refused**: Check Lakebase instance status and network connectivity
2. **Authentication Failed**: Verify token validity and user permissions
3. **SSL Errors**: Ensure SSL mode is configured correctly
4. **Performance Inconsistency**: Consider network latency and token refresh timing

### Getting Help:

For Lakebase-specific issues, consult your Databricks representative or the Databricks documentation.