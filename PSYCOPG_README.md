# psycopg2 Concurrent Query Execution

This document explains how psycopg2 executes concurrent queries based on concurrency level in the Lakebase accelerator project.

## Overview

The concurrency implementation uses a sophisticated **asyncio-based approach** with **connection pooling** and **semaphore-based concurrency control** to simulate realistic database load testing scenarios.

## Architecture Components

### 1. Concurrency Control Mechanism

The system uses Python's `asyncio.Semaphore` to control the number of concurrent operations:

```python
sem = asyncio.Semaphore(concurrency_level)
```

This semaphore acts as a **gatekeeper** that only allows a maximum number of queries to execute simultaneously, equal to the `concurrency_level` parameter.

### 2. Connection Pool Management

The system uses **SQLAlchemy's async engine** with connection pooling:

```python
self._engine = create_async_engine(
    connection_url,
    pool_size=pool_config.get("base_pool_size", 5),
    max_overflow=pool_config.get("max_overflow", 10),
    pool_timeout=pool_config.get("pool_timeout", 30),
    pool_recycle=pool_config.get("pool_recycle", 3600),
    echo=False
)
```

**Key pool parameters:**
- `base_pool_size`: Base number of connections (default: 5)
- `max_overflow`: Additional connections when pool is exhausted (default: 10)
- `pool_timeout`: How long to wait for a connection (default: 30s)
- `pool_recycle`: How often to refresh connections (default: 3600s)

## Concurrent Execution Flow

### Step 1: Task Creation
```python
tasks: List[asyncio.Task] = []
for qc in queries:
    for sc in qc.get('test_scenarios', []):
        tasks.append(asyncio.create_task(_runner(qc, sc)))
```

Each query scenario becomes an **asyncio Task** that can run concurrently.

### Step 2: Semaphore-Controlled Execution
```python
async def _runner(query_config: Dict[str, Any], scenario: Dict[str, Any]) -> List[QueryExecutionResult]:
    results: List[QueryExecutionResult] = []
    exec_count = int(scenario.get('execution_count', 1))
    for _ in range(exec_count):
        async with sem:  # This is the key concurrency control
            res = await self._execute_single_query_async(...)
            results.append(res)
    return results
```

The `async with sem:` statement ensures that:
- Only `concurrency_level` number of queries can execute simultaneously
- Other queries wait in a queue until a slot becomes available
- Each query gets a fresh connection from the pool

### Step 3: Parallel Execution
```python
nested_results = await asyncio.gather(*tasks, return_exceptions=True)
```

All tasks run **in parallel**, but the semaphore ensures the concurrency limit is respected.

## Connection Management Per Query

Each query execution:
```python
async with self._engine.connect() as conn:
    result = await conn.execute(text(converted_query), params_map)
    row_count = result.rowcount or 0
```

- Gets a **fresh connection** from the pool
- Executes the query using **asyncpg** (PostgreSQL async driver)
- **Automatically returns** the connection to the pool when done

## Concurrency Level Impact

| Concurrency Level | Behavior |
|------------------|----------|
| **1** | Sequential execution (no parallelism) |
| **5** | Up to 5 queries run simultaneously |
| **10** | Up to 10 queries run simultaneously |
| **50** | Up to 50 queries run simultaneously |

## Pool Sizing Strategy

The system dynamically adjusts pool size based on concurrency:
```python
pool_config = {
    "base_pool_size": max(1, concurrency_level // 4),  # 25% of concurrency
    "max_overflow": concurrency_level,                  # 100% of concurrency
    "pool_timeout": 30,
    "pool_recycle": 3600,
    "command_timeout": 30,
    "ssl_mode": "require"
}
```

**Example:** For `concurrency_level=20`:
- Base pool: 5 connections
- Max overflow: 20 connections  
- Total possible: 25 connections

## Performance Benefits

This approach provides:
- **True parallelism** within concurrency limits
- **Connection reuse** through pooling
- **Resource management** to prevent database overload
- **Error isolation** (one failed query doesn't affect others)
- **Scalable performance** testing

## Implementation Details

### Frontend Configuration
The React frontend allows users to configure:
- Concurrency level (1-1000)
- Connection pool parameters
- Query execution scenarios
- Test duration and iterations

### Backend Processing
The FastAPI backend:
- Validates concurrency parameters
- Initializes connection pools
- Executes queries with proper error handling
- Collects performance metrics
- Returns comprehensive test reports

### Database Integration
- Uses **asyncpg** for PostgreSQL async operations
- Supports parameterized queries to prevent SQL injection
- Handles connection timeouts and retries
- Provides detailed execution metrics

## Query Parameter and Execution Count Processing

### Query File Format

Queries are defined in SQL files with special comment headers that specify parameters and execution counts:

```sql
-- PARAMETERS: [[1, "AAAAAAAABAAAAAAA", 100], [2, "AAAAAAAACAAAAAAA", 50], [3, "AAAAAAAADAAAAAAA", 200], [4, "AAAAAAAAEAAAAAAA", 1000]]
-- EXEC_COUNT: 40

SELECT * FROM customer 
WHERE c_customer_sk = %s 
  AND c_customer_id = %s 
LIMIT %s;
```

### Parameter Processing

The system processes these special comments to create test scenarios:

1. **PARAMETERS**: JSON array of parameter sets
   - Each inner array represents one parameter set
   - Multiple parameter sets allow testing different query variations
   - Example: `[[1, "AAAAAAAABAAAAAAA", 100], [2, "AAAAAAAACAAAAAAA", 50]]`

2. **EXEC_COUNT**: Number of times each parameter set should be executed
   - Defaults to 5 if not specified
   - Multiplies the total number of query executions

### Execution Flow for Parameterized Queries

#### Step 1: Query Parsing
```python
def validate_query_format(query_content: str) -> Dict[str, Any]:
    lines = query_content.split('\n')
    parameter_sets = []
    exec_count = 5  # Default
    
    for line in lines:
        line = line.strip()
        if line.startswith('-- PARAMETERS:'):
            json_str = line.replace('-- PARAMETERS:', '').strip()
            parameter_sets = json.loads(json_str)
        elif line.startswith('-- EXEC_COUNT:'):
            exec_count = int(line.replace('-- EXEC_COUNT:', '').strip())
    
    return {
        "parameter_sets": parameter_sets,
        "exec_count": exec_count
    }
```

#### Step 2: Test Scenario Generation
For the customer_lookup_example.sql:
- **4 parameter sets** × **40 executions each** = **160 total query executions**
- Each parameter set becomes a separate test scenario

```python
test_scenarios = []
for i, params in enumerate(parameter_sets):
    test_scenarios.append({
        "name": f"scenario_{i+1}",
        "parameters": params,
        "execution_count": exec_count  # 40
    })
```

#### Step 3: Concurrent Execution with Semaphore Control
```python
async def _runner(query_config: Dict[str, Any], scenario: Dict[str, Any]) -> List[QueryExecutionResult]:
    results: List[QueryExecutionResult] = []
    exec_count = int(scenario.get('execution_count', 1))  # 40
    
    for _ in range(exec_count):  # Execute 40 times
        async with sem:  # Wait for available concurrency slot
            res = await self._execute_single_query_async(
                query_config['query_content'],
                scenario['parameters'],  # [1, "AAAAAAAABAAAAAAA", 100]
                query_config['query_identifier'],
                scenario['name']
            )
            results.append(res)
    return results
```

### Parameter Binding Process

#### Step 1: Placeholder Conversion
```python
def _convert_ps_placeholders(self, query: str, parameters: List[Any]) -> Tuple[str, Dict[str, Any]]:
    converted_query = query
    params_map = {}
    
    for i, param in enumerate(parameters, 1):
        placeholder = f"%s"
        param_name = f"p{i}"
        converted_query = converted_query.replace(placeholder, f":{param_name}", 1)
        params_map[param_name] = param
    
    return converted_query, params_map
```

**Example transformation:**
- **Original**: `SELECT * FROM customer WHERE c_customer_sk = %s AND c_customer_id = %s LIMIT %s;`
- **Parameters**: `[1, "AAAAAAAABAAAAAAA", 100]`
- **Converted**: `SELECT * FROM customer WHERE c_customer_sk = :p1 AND c_customer_id = :p2 LIMIT :p3;`
- **Params Map**: `{"p1": 1, "p2": "AAAAAAAABAAAAAAA", "p3": 100}`

#### Step 2: Database Execution
```python
async with self._engine.connect() as conn:
    result = await conn.execute(text(converted_query), params_map)
    row_count = result.rowcount or 0
```

### Execution Timeline Example

For `concurrency_level=10` and the customer_lookup_example.sql:

```
Time 0s: 10 queries start (scenario_1, exec 1-10)
Time 0.1s: 10 queries start (scenario_1, exec 11-20)
Time 0.2s: 10 queries start (scenario_1, exec 21-30)
Time 0.3s: 10 queries start (scenario_1, exec 31-40)
Time 0.4s: 10 queries start (scenario_2, exec 1-10)
... and so on for all 4 scenarios
```

**Total Executions**: 4 scenarios × 40 executions = **160 query executions**
**Concurrent Limit**: Maximum 10 queries running simultaneously
**Estimated Duration**: ~16 batches of 10 queries each

### Performance Impact

| Parameter Sets | Exec Count | Total Queries | Concurrency Level | Estimated Batches |
|---------------|------------|---------------|-------------------|-------------------|
| 4 | 40 | 160 | 10 | 16 |
| 2 | 20 | 40 | 5 | 8 |
| 1 | 100 | 100 | 20 | 5 |


## Error Handling

The system includes comprehensive error handling:
- Connection pool exhaustion
- Query timeout handling
- Database connection errors
- Parameter validation errors
- Graceful degradation under load

## Monitoring and Metrics

The system tracks:
- Query execution times (average, P95, P99)
- Success/failure rates
- Throughput (queries per second)
- Connection pool utilization
- Error types and frequencies

This implementation effectively simulates real-world concurrent database usage while maintaining control over resource consumption and preventing database connection exhaustion.
