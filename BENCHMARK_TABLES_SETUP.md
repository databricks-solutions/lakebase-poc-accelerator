# Benchmark Tables Setup Guide

This guide provides the SQL statements needed to create and populate the `customer` table for running the predefined benchmark queries in both Provisioned and Autoscaling Lakebase instances.

## Table of Contents

- [Schema Differences](#schema-differences)
- [Provisioned Instance Setup](#provisioned-instance-setup)
- [Autoscaling Instance Setup](#autoscaling-instance-setup)
- [Verification Queries](#verification-queries)


---

## Schema Differences

The `customer` table schemas differ slightly between Provisioned and Autoscaling instances:

| Column | Provisioned | Autoscaling | Purpose |
|--------|-------------|-------------|---------|
| `c_customer_sk` | ✅ | ✅ | Primary key for point lookups |
| `c_customer_id` | ✅ | ✅ | Customer identifier (TPC-DS format) |
| `c_preferred_cust_flag` | ✅ | ✅ | Used for aggregation queries |
| `c_first_name` | ✅ | ✅ | Customer first name |
| `c_last_name` | ✅ | ✅ | Customer last name |
| `c_email` | ✅ | ✅ | Customer email |
| `created_at` | ✅ | ✅ | Record creation timestamp |
| `c_current_hdemo_sk` | ✅ | ❌ | **Only in provisioned** - Used for range queries |

**Why the difference?**
- Provisioned instances use the full TPC-DS customer schema for comprehensive testing
- Autoscaling instances use a simplified schema focused on common OLTP patterns

---

## Provisioned Instance Setup

### 1. Create Table

```sql
-- Drop existing table if needed
DROP TABLE IF EXISTS public.customer CASCADE;

-- Create the customer table (Provisioned Instance - Full Schema)
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

-- Create indexes for better query performance
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
-- Check row count
SELECT COUNT(*) FROM public.customer;
-- Expected: 1000

-- Verify schema
\d public.customer

-- Check sample data
SELECT * FROM public.customer LIMIT 10;

-- Verify c_current_hdemo_sk range
SELECT MIN(c_current_hdemo_sk), MAX(c_current_hdemo_sk) FROM public.customer;
-- Expected: 1, 20
```

---

## Autoscaling Instance Setup

### 1. Create Table

```sql
-- Drop existing table if needed
DROP TABLE IF EXISTS public.customer CASCADE;

-- Create the customer table (Autoscaling Instance - Simplified Schema)
CREATE TABLE public.customer (
    c_customer_sk INT PRIMARY KEY,
    c_customer_id VARCHAR(50) NOT NULL,
    c_preferred_cust_flag VARCHAR(1) DEFAULT 'N',
    c_first_name VARCHAR(50),
    c_last_name VARCHAR(50),
    c_email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for aggregation queries
CREATE INDEX idx_customer_flag ON public.customer(c_preferred_cust_flag);
```

### 2. Insert Sample Data

```sql
-- Insert 1000 sample records for benchmarking
INSERT INTO public.customer (c_customer_sk, c_customer_id, c_preferred_cust_flag, c_first_name, c_last_name, c_email)
SELECT 
    i AS c_customer_sk,
    'AAAAAAA' || LPAD(i::text, 9, 'A') AS c_customer_id,
    CASE WHEN i % 2 = 0 THEN 'Y' ELSE 'N' END AS c_preferred_cust_flag,
    'Customer' AS c_first_name,
    'Name_' || i AS c_last_name,
    'customer' || i || '@example.com' AS c_email
FROM generate_series(1, 1000) AS i;
```

### 3. Verify Setup

```sql
-- Check row count
SELECT COUNT(*) FROM public.customer;
-- Expected: 1000

-- Verify schema
\d public.customer

-- Check sample data
SELECT * FROM public.customer LIMIT 10;

-- Verify flag distribution
SELECT c_preferred_cust_flag, COUNT(*) FROM public.customer GROUP BY c_preferred_cust_flag;
-- Expected: Y: 500, N: 500
```

---

## Verification Queries

After setup, test with the predefined benchmark queries:

### Provisioned Instance - Test Queries

```sql
-- Point query (from pgbench)
SELECT * FROM public.customer WHERE c_customer_sk = 42;

-- Range query using c_current_hdemo_sk (from pgbench)
SELECT count(*) FROM public.customer 
WHERE c_current_hdemo_sk BETWEEN 5 AND 15;

-- Aggregation query (from pgbench and psycopg)
SELECT c_preferred_cust_flag, count(*) 
FROM public.customer 
GROUP BY c_preferred_cust_flag;

-- Parameterized lookup (from psycopg)
SELECT * FROM customer 
WHERE c_customer_sk = 1 
  AND c_customer_id = 'AAAAAAAABAAAAAAA' 
LIMIT 100;
```

### Autoscaling Instance - Test Queries

```sql
-- Point query (from pgbench)
SELECT * FROM public.customer WHERE c_customer_sk = 42;

-- Range query using c_customer_sk (from pgbench)
-- Note: Uses c_customer_sk instead of c_current_hdemo_sk
SELECT count(*) FROM public.customer 
WHERE c_customer_sk BETWEEN 100 AND 200;

-- Aggregation query (from pgbench and psycopg)
SELECT c_preferred_cust_flag, count(*) 
FROM public.customer 
GROUP BY c_preferred_cust_flag;

-- Parameterized lookup (from psycopg)
SELECT * FROM customer 
WHERE c_customer_sk = 1 
  AND c_customer_id = 'AAAAAAAABAAAAAAA' 
LIMIT 100;
```

---

## Predefined Query Mappings

### Pgbench Queries (Web UI)

The web application uses these queries for pgbench testing:

**Point Query (60% weight):**
```sql
\set c_customer_sk random(0, 999)
SELECT * FROM public.customer WHERE c_customer_sk = :c_customer_sk;
```

**Range Query (30% weight):**
- **Provisioned**: Uses `c_current_hdemo_sk BETWEEN :c_start AND :c_end`
- **Autoscaling**: Uses `c_customer_sk BETWEEN :c_start AND :c_end`

**Aggregation Query (10% weight):**
```sql
SELECT c_preferred_cust_flag, count(*) 
FROM public.customer 
GROUP BY c_preferred_cust_flag;
```

### Psycopg Queries (Web UI)

The web application includes these predefined queries for concurrency testing:

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

### Issue: "column c_current_hdemo_sk does not exist"

**Cause:** Running provisioned queries against an autoscaling instance, or vice versa.

**Solution:** 
- For autoscaling instances, use the simplified schema without `c_current_hdemo_sk`
- Range queries in autoscaling should use `c_customer_sk` instead

### Issue: "permission denied for table customer"

**Cause:** User lacks necessary permissions.

**Solution:**
```sql
-- Grant permissions (run as admin)
GRANT ALL PRIVILEGES ON TABLE public.customer TO <username>;
```

### Issue: No data returned from queries

**Cause:** Table is empty or data wasn't inserted correctly.

**Solution:**
```sql
-- Check row count
SELECT COUNT(*) FROM public.customer;

-- Re-run the INSERT statements from the setup section
```

---

## Related Documentation

- [PGBENCH_README.md](./PGBENCH_README.md) - pgbench testing guide
- [PSYCOPG_README.md](./PSYCOPG_README.md) - psycopg concurrency testing guide
- [README.md](./README.md) - Main application documentation

