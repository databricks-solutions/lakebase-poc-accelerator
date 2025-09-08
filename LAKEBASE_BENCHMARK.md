# Lakebase Performance Benchmarks and Qualifications

This document summarizes Lakebase Postgres performance benchmarks for sizing and capacity planning decisions.

## Benchmarks (Loading via Sync)

### Initial Ingestion Performance
- **Snapshot sync, first run of triggered/continuous, full refreshes**
- **Performance**: ~15,000 1KB rows/sec per Capacity Unit (CU)
- **Use Case**: Bulk data loading, initial table setup, full data refreshes
- **Last Updated**: 2024-12-13

### Incremental Ingestion Performance  
- **Triggered/continuous updates**
- **Performance**: ~1,200 1KB rows/sec per Capacity Unit (CU)
- **Use Case**: Real-time data updates, incremental sync operations
- **Last Updated**: 2024-12-13

## Benchmarks (Read/Write from Application)

### YCSB Point Lookups
YCSB (Yahoo! Cloud Serving Benchmark) point get operations for key-value lookups:

- **Data fits in RAM**: ~30,000 rows @ 1KB / CU
- **Data does not fit in RAM**: ~1,700 rows @ 1KB / CU
- **Use Case**: Primary key lookups, cache hits, frequent single-row queries

### PostgreSQL Sequential Scan
Full table scans for analytical workloads:

- **No data cached in RAM**: ~18 MB/s per CU
- **Data in RAM**: ~2 GB/s per CU
- **Use Case**: Analytical queries, reporting, data exploration
- **Last Updated**: June 23, 2025

## PageServer Performance

The PageServer is the service that:
- Stores all table/index pages (durable storage for Postgres data)
- Serves pages on demand to Postgres compute nodes via GetPage protocol
- Reconstructs historical versions of pages using WAL from safekeepers (time travel)
- Handles garbage collection and compaction of data

### PageServer Benchmarks
- **GetPage QPS**: 176,000 QPS on 16-cores
- **GetPage throttling**: 5,500 QPS per CU (timeline_get_throttle)
- **Last Updated**: 2024-12-13

### Backpressure Limits
- **max_replication_write_lag**: 500 MB
- **max_replication_flush_lag**: 10 GB

## Safekeeper Performance

Safekeeper manages Write-Ahead Log (WAL) operations and replication:

### WAL Rate Limits
- **1 CU**: 25 MB/s
- **2 CU**: 50 MB/s  
- **4 CU**: 75 MB/s
- **8 CU**: 100 MB/s

**Note**: WAL rate is the limiting factor for workloads that write large rows (>100KB) to Postgres.

## Capacity Unit (CU) Specifications

| CU | Memory | CPU Cores | Base Cost/Month |
|----|--------|-----------|-----------------|
| 1  | 16 GB  | 2 cores   | $291.67         |
| 2  | 32 GB  | 4 cores   | $583.33         |
| 4  | 64 GB  | 8 cores   | $1,166.67       |
| 8  | 128 GB | 16 cores  | $2,333.33       |

## Sizing Guidelines

### For OLTP Workloads
- **Point lookups**: Size based on QPS requirements
- **Small transactions**: Consider 1-2 CU for light workloads
- **High concurrency**: Scale up to 4-8 CU with readable secondaries


## Cost Considerations

- **Storage**: $0.35 per GB per month
- **Continuous Sync**: $548 per pipeline per month(Continuous pipleines are fixed cost up until a certain load, then they will auto scale up. This assumes minimum sizing.)
- **Triggered Sync**: $0.75 per hour of sync time
- **Readable Secondaries**: Same CU pricing as main instance

## Benchmark Methodology

These benchmarks are based on:
- Standardized test environments
- Representative workload patterns
- Production-like data distributions
- Consistent measurement methodologies

For the most current performance data, refer to the official Lakebase documentation and your Databricks representative.
