# Benchmark Tables Setup Guide

This guide provides the SQL to create and populate the `customer` table for pgbench and concurrency tests. **The same schema and queries are used for both Provisioned and Autoscaling Lakebase**—connect with your PostgreSQL credentials (PGUSER/PGPASSWORD) in either case.

## Table of Contents

- [Customer Table Setup (Unified)](#customer-table-setup-unified)
- [Verification Queries](#verification-queries)
- [Predefined Query Mappings](#predefined-query-mappings)
- [Troubleshooting](#troubleshooting)

---

## Customer Table Setup (Unified)

Use this **single** table definition for both Provisioned and Autoscaling instances.

### 1. Create Table

```sql
-- Drop existing table if needed
DROP TABLE IF EXISTS public.customer CASCADE;

-- Create the customer table (full schema for both Provisioned and Autoscaling)
CREATE TABLE public.customer (
    c_customer_sk INT PRIMARY KEY,
    c_customer_id VARCHAR(50) NOT NULL,
    c_preferred_cust_flag VARCHAR(1) DEFAULT 'N',
    c_current_hdemo_sk INT,
    c_first_name VARCHAR(50),
    c_last_name VARCHAR(50),
    c_email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for point, range, and aggregation queries
CREATE INDEX idx_customer_hdemo ON public.customer(c_current_hdemo_sk);
CREATE INDEX idx_customer_flag ON public.customer(c_preferred_cust_flag);
```

### 2. Insert Sample Data

```sql
-- Insert 1000 sample records for benchmarking
INSERT INTO public.customer (c_customer_sk, c_customer_id, c_preferred_cust_flag, c_current_hdemo_sk, c_first_name, c_last_name, c_email)
SELECT 
    i AS c_customer_sk,
    'AAAAAAA' || LPAD(i::text, 9, 'A') AS c_customer_id,
    CASE WHEN i % 2 = 0 THEN 'Y' ELSE 'N' END AS c_preferred_cust_flag,
    (i % 20) + 1 AS c_current_hdemo_sk,
    'Customer' AS c_first_name,
    'Name_' || i AS c_last_name,
    'customer' || i || '@example.com' AS c_email
FROM generate_series(1, 1000) AS i;
```

### 3. Verify Setup

```sql
-- Row count
SELECT COUNT(*) FROM public.customer;
-- Expected: 1000

-- Schema
\d public.customer

-- Sample rows
SELECT * FROM public.customer LIMIT 10;

-- Range for range queries (c_current_hdemo_sk 1–20)
SELECT MIN(c_current_hdemo_sk), MAX(c_current_hdemo_sk) FROM public.customer;

-- Flag distribution
SELECT c_preferred_cust_flag, COUNT(*) FROM public.customer GROUP BY c_preferred_cust_flag;
-- Expected: Y: 500, N: 500
```

---

## Verification Queries

Use these to confirm the table works with the predefined benchmark queries:

```sql
-- Point query (pgbench)
SELECT * FROM public.customer WHERE c_customer_sk = 42;

-- Range query (pgbench) – uses c_current_hdemo_sk
SELECT count(*) FROM public.customer 
WHERE c_current_hdemo_sk BETWEEN 5 AND 15;

-- Aggregation query (pgbench and psycopg)
SELECT c_preferred_cust_flag, count(*) 
FROM public.customer 
GROUP BY c_preferred_cust_flag;

-- Parameterized lookup (psycopg)
SELECT * FROM customer 
WHERE c_customer_sk = 1 
  AND c_customer_id = 'AAAAAAAABAAAAAAA' 
LIMIT 100;
```

---

## Predefined Query Mappings

### Pgbench (Web UI and deploy_pgbench_job.py)

Same queries for both instance types:

**Point (60% weight):**
```sql
\set c_customer_sk random(0, 999)
SELECT * FROM public.customer WHERE c_customer_sk = :c_customer_sk;
```

**Range (30% weight):**
```sql
\set c_start random(1, 11)
\set c_end :c_start + 10
SELECT count(*) FROM public.customer 
WHERE c_current_hdemo_sk BETWEEN :c_start AND :c_end;
```

**Aggregation (10% weight):**
```sql
SELECT c_preferred_cust_flag, count(*) 
FROM public.customer 
GROUP BY c_preferred_cust_flag;
```

### Psycopg (Web UI)

**customer_lookup_example:**
```sql
-- PARAMETERS: [[1, "AAAAAAAABAAAAAAA", 100], [2, "AAAAAAAACAAAAAAA", 50], ...]
-- EXEC_COUNT: 100
SELECT * FROM customer 
WHERE c_customer_sk = %s 
  AND c_customer_id = %s 
LIMIT %s;
```

**simple_query_example:**
```sql
-- EXEC_COUNT: 100
SELECT COUNT(*) as total_customers FROM customer;
```

**customer_flag_example:**
```sql
-- PARAMETERS: [["N"], ["Y"]]
-- EXEC_COUNT: 100
SELECT c_preferred_cust_flag, count(*) 
FROM customer 
GROUP BY c_preferred_cust_flag;
```

---

## Troubleshooting

### "column c_current_hdemo_sk does not exist"

**Cause:** The table was created with an older simplified schema (no `c_current_hdemo_sk`).

**Solution:** Recreate the table using the [unified schema](#customer-table-setup-unified) above (includes `c_current_hdemo_sk` and the two indexes).

### "permission denied for table customer"

**Cause:** The database user lacks privileges.

**Solution:**
```sql
-- Run as admin
GRANT ALL PRIVILEGES ON TABLE public.customer TO <username>;
```

### No data returned

**Cause:** Table is empty or INSERT failed.

**Solution:**
```sql
SELECT COUNT(*) FROM public.customer;
-- Re-run the INSERT from the setup section if 0
```

---

## Related Documentation

- [PGBENCH_README.md](./PGBENCH_README.md) – pgbench testing
- [PSYCOPG_README.md](./PSYCOPG_README.md) – psycopg concurrency testing
- [README.md](./README.md) – Main application documentation
