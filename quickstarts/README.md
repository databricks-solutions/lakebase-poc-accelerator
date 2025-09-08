# Lakebase Postgres Testing Quickstarts

A comprehensive quickstart solution for testing Lakebase (managed Postgres database) on Databricks. This toolkit provides sizing guidance, query conversion, configuration generation, and concurrency testing capabilities specifically designed for Postgres workloads.

## 🎯 Overview

This quickstart accelerator helps you:

- **Size Postgres instances** appropriately based on workload requirements
- **Convert queries** from Databricks SQL to Postgres SQL automatically
- **Generate configurations** for Databricks integration with Postgres
- **Test concurrency** performance under various workload patterns
- **Monitor and optimize** Postgres performance on Databricks

## 📋 Prerequisites

### System Requirements
- Python 3.8 or higher
- Access to Databricks workspace
- Postgres database knowledge
- Valid Databricks API token

### Required Python Packages
```bash
pip install -r requirements.txt
```

**Core Dependencies:**
```
pyyaml>=6.0
psycopg2-binary>=2.9.0
openai>=1.0.0
pandas>=1.5.0
matplotlib>=3.6.0
sqlparse>=0.4.0
```

**Optional Dependencies:**
```
asyncpg>=0.28.0  # For async connection testing
plotly>=5.0.0    # For interactive charts
```

### Databricks Setup
1. **API Token**: Set `DATABRICKS_TOKEN` environment variable
2. **LLM Endpoint**: Ensure access to Databricks LLM serving endpoint
3. **Workspace Access**: Required permissions for job creation and execution

### Postgres Setup
1. **Connection Details**: Host, port, database, credentials
2. **Extensions**: Install `pg_stat_statements` for performance monitoring
3. **Permissions**: Appropriate read/write access for your use case

## 🚀 Quick Start

### 1. Clone and Setup
```bash
cd quickstarts/
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Environment
```bash
export DATABRICKS_TOKEN="your_databricks_token"
export POSTGRES_HOST="your_postgres_host"
export POSTGRES_DATABASE="your_database"
export POSTGRES_USERNAME="your_username"
export POSTGRES_PASSWORD="your_password"
```

### 3. Size Your Instance
```bash
# Review sizing guide for capacity planning
cat sizing_guide.md

# Determine appropriate CU allocation based on:
# - Data size
# - Query complexity  
# - Concurrent connections
# - Performance requirements
```

### 4. Convert Queries
```bash
# Convert all queries from Databricks SQL to Postgres SQL
python convert_queries.py \
    --source-dir queries/source \
    --target-dir queries/target \
    --databricks-token $DATABRICKS_TOKEN

# Convert a single query
python convert_queries.py \
    --file queries/source/oltp_q01_customer_lookup.sql \
    --output queries/target/oltp_q01_customer_lookup.sql
```

### 5. Generate Configuration
```bash
# Customize the configuration template
cp config_template.yaml my_config.yaml
# Edit my_config.yaml with your specific settings

# Generate databricks.yml
python generate_databricks_yml.py \
    --config my_config.yaml \
    --output databricks.yml \
    --validate-connection
```

### 6. Run Concurrency Tests
```bash
# List available test scenarios
python run_concurrency_test.py \
    --config concurrency_config.yaml \
    --list-scenarios

# Run a specific scenario
python run_concurrency_test.py \
    --config concurrency_config.yaml \
    --scenario oltp_light

# Run all scenarios
python run_concurrency_test.py \
    --config concurrency_config.yaml \
    --all-scenarios \
    --output-dir ./test_results
```

## 📁 Project Structure

```
quickstarts/
├── README.md                    # This file
├── sizing_guide.md              # Postgres sizing recommendations
├── config_template.yaml         # Configuration template
├── generate_databricks_yml.py   # Configuration generator
├── convert_queries.py           # Query conversion script
├── concurrency_config.yaml      # Concurrency test configuration
├── run_concurrency_test.py      # Concurrency testing framework
└── queries/
    ├── source/                  # Original Databricks SQL queries
    │   ├── oltp_q01_customer_lookup.sql
    │   ├── oltp_q02_order_details.sql
    │   └── ...
    └── target/                  # Converted Postgres queries
        └── (generated files)
```

## 🔧 Configuration Guide

### Postgres Configuration (`config_template.yaml`)

The configuration template covers all aspects of Postgres integration:

#### Database Instance Settings
```yaml
database_instance:
  name: "lakebase-postgres-prod"
  capacity_units: 4              # 1-8 CU (16-128GB memory)
  region: "us-west-2"
```

#### Connection Settings
```yaml
postgres_settings:
  connection:
    host: "${POSTGRES_HOST}"
    port: 5432
    database: "${POSTGRES_DATABASE}"
    username: "${secrets/scope/postgres_user}"
    password: "${secrets/scope/postgres_password}"
    ssl_mode: "require"
    pool_size: 20
    max_overflow: 30
```

#### Performance Tuning
```yaml
postgres_settings:
  performance:
    shared_buffers: "auto"       # Calculated based on CU
    work_mem: "auto"
    maintenance_work_mem: "auto"
    effective_cache_size: "auto"
    max_connections: "auto"
```

#### Table Synchronization
```yaml
synced_tables:
  - source_table: "public.customers"
    target_table: "customers"
    sync_mode: "incremental"     # full, incremental, cdc
    sync_frequency: "hourly"
    incremental_column: "updated_at"
    batch_size: 10000
```

### Concurrency Testing (`concurrency_config.yaml`)

Define test scenarios for different workload patterns:

#### OLTP Scenario
```yaml
test_scenarios:
  - name: "oltp_light"
    description: "Light OLTP workload simulation"
    duration: 300
    concurrent_connections: 25
    query_patterns:
      - query_name: "customer_lookup"
        weight: 30
        think_time: 0.1
        max_execution_time: 5
```

#### Performance Targets
```yaml
performance_targets:
  avg_response_time_ms: 500
  p95_response_time_ms: 2000
  min_throughput_qps: 50
  max_error_rate_percent: 1
```

## 📊 Understanding Results

### Sizing Recommendations

The sizing guide provides CU recommendations based on:

| Data Size | QPS | Query Complexity | Concurrent Users | Recommended CU |
|-----------|-----|------------------|------------------|----------------|
| < 10GB    | < 100 | Simple SELECT | < 50 | 1 CU |
| 10-50GB   | 100-500 | Mixed | 50-100 | 2 CU |
| 50-200GB  | 500-1000 | Complex JOINs | 100-200 | 4 CU |
| > 200GB   | > 1000 | Heavy Analytics | > 200 | 8 CU |

### Query Conversion Results

Converted queries include:
- **Original SQL** preserved in comments
- **Postgres-optimized syntax** with proper data types
- **Conversion warnings** for manual review
- **Performance annotations** for optimization

### Concurrency Test Metrics

Key performance indicators:
- **Throughput**: Queries per second (QPS)
- **Response Time**: P50, P95, P99 percentiles
- **Error Rate**: Percentage of failed queries
- **Connection Pool Utilization**: Resource efficiency

## 🔍 Troubleshooting

### Common Issues

#### Connection Problems
```bash
# Test connection manually
python -c "
import psycopg2
conn = psycopg2.connect(
    host='$POSTGRES_HOST',
    database='$POSTGRES_DATABASE',
    user='$POSTGRES_USERNAME', 
    password='$POSTGRES_PASSWORD'
)
print('Connection successful')
conn.close()
"
```

#### Query Conversion Errors
```bash
# Run conversion with verbose logging
python convert_queries.py \
    --file problematic_query.sql \
    --output converted_query.sql \
    --verbose

# Check conversion report
cat conversion_report.json
```

#### Performance Issues
```bash
# Run stress test to identify bottlenecks
python run_concurrency_test.py \
    --config concurrency_config.yaml \
    --scenario stress_test \
    --verbose
```

### Postgres-Specific Considerations

#### Memory Settings
- **shared_buffers**: 25% of total memory for OLTP, up to 40% for OLAP
- **work_mem**: Start with 32MB, increase for complex queries
- **effective_cache_size**: 75% of total memory

#### Connection Pooling
- Use **transaction-level pooling** for OLTP workloads
- Configure **statement timeout** to prevent runaway queries
- Monitor **connection pool utilization** regularly

#### Query Optimization
- Ensure **proper indexing** on frequently queried columns
- Use **EXPLAIN ANALYZE** to understand query plans
- Consider **materialized views** for complex analytical queries

## 📈 Best Practices

### Capacity Planning
1. **Start Conservative**: Begin with lower CU and scale up based on metrics
2. **Monitor Continuously**: Track CPU, memory, and I/O utilization
3. **Plan for Growth**: Consider future data size and user growth
4. **Test Thoroughly**: Validate performance under realistic workloads

### Query Development
1. **Convert Incrementally**: Start with simple queries, progress to complex
2. **Validate Results**: Compare output between Databricks and Postgres
3. **Optimize for Postgres**: Leverage Postgres-specific features
4. **Document Changes**: Track conversion decisions and optimizations

### Performance Testing
1. **Use Realistic Data**: Test with production-like data volumes
2. **Simulate Real Workloads**: Mix OLTP and OLAP patterns appropriately
3. **Test Edge Cases**: Include error conditions and resource limits
4. **Automate Testing**: Integrate performance tests into CI/CD pipeline

### Monitoring and Alerting
1. **Set Up Dashboards**: Monitor key metrics continuously
2. **Configure Alerts**: Alert on performance degradation
3. **Regular Reviews**: Analyze trends and optimize accordingly
4. **Capacity Planning**: Use metrics for future sizing decisions

## 🔗 Advanced Usage

### Custom Query Patterns
```python
# Add custom parameter generation
def generate_custom_parameter(param_config):
    if param_config['type'] == 'custom_date':
        # Custom date logic
        return generate_business_date()
    return default_generator(param_config)
```

### Integration with CI/CD
```yaml
# .github/workflows/postgres-testing.yml
name: Postgres Performance Testing
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Run Concurrency Tests
        run: |
          python run_concurrency_test.py \
            --config concurrency_config.yaml \
            --scenario oltp_light \
            --output-dir results/
```

### Custom Metrics Collection
```python
# Extend MetricsCollector for custom metrics
class CustomMetricsCollector(MetricsCollector):
    def collect_postgres_metrics(self):
        # Collect pg_stat_statements data
        # Monitor connection pool stats
        # Track custom business metrics
        pass
```

## 📚 Additional Resources

### Postgres Documentation
- [PostgreSQL Performance Tuning](https://www.postgresql.org/docs/current/performance-tips.html)
- [Connection Pooling](https://www.postgresql.org/docs/current/runtime-config-connection.html)
- [Monitoring and Statistics](https://www.postgresql.org/docs/current/monitoring-stats.html)

### Databricks Resources
- [Databricks SQL Reference](https://docs.databricks.com/sql/language-manual/index.html)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Delta Live Tables](https://docs.databricks.com/workflows/delta-live-tables/index.html)

### Performance Testing
- [Database Load Testing Best Practices](https://docs.databricks.com/optimizations/index.html)
- [SQL Performance Optimization](https://docs.databricks.com/optimizations/sql-performance.html)

## 🤝 Contributing

We welcome contributions to improve this quickstart accelerator:

1. **Report Issues**: Use GitHub issues for bugs and feature requests
2. **Submit PRs**: Follow standard GitHub workflow for contributions
3. **Share Feedback**: Let us know about your experience and suggestions
4. **Extend Functionality**: Add new query patterns, test scenarios, or metrics

## 📄 License

This project is licensed under the Apache License 2.0. See LICENSE file for details.

## 🆘 Support

For support and questions:

1. **Documentation**: Check this README and inline documentation
2. **Issues**: Create GitHub issues for bugs and feature requests  
3. **Discussions**: Use GitHub Discussions for questions and community support
4. **Databricks Support**: Contact Databricks support for platform-specific issues

---

**Happy Testing! 🚀**

This quickstart accelerator provides a solid foundation for testing Postgres workloads on Databricks. Customize the configurations and test scenarios to match your specific requirements, and use the insights to optimize your Lakebase deployment for production use.
