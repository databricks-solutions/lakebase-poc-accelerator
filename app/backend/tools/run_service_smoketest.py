#!/usr/bin/env python3
"""
Async smoketest for LakebaseConnectionService.

- Initializes the async SQLAlchemy engine with background token refresh
- Reads a SQL file from app/queries/test.sql
- Executes the query concurrently using provided concurrency and execution count
- Prints a concise report

Environment variables used:
  - LAKEBASE_INSTANCE_NAME (required)
  - LAKEBASE_DATABASE_NAME (required)
  - DATABRICKS_DATABASE_PORT (optional, default 5432)
  - DB_POOL_SIZE, DB_MAX_OVERFLOW, DB_POOL_TIMEOUT, DB_POOL_RECYCLE_INTERVAL, DB_COMMAND_TIMEOUT, DB_SSL_MODE (optional)

Reference: https://apps-cookbook.dev/docs/fastapi/getting_started/lakebase_connection
"""

import asyncio
import os
import sys
import argparse
import json
from pathlib import Path
from dotenv import load_dotenv


# Calculate paths correctly
SCRIPT_DIR = Path(__file__).resolve().parent  # app/backend/tools/
BACKEND_ROOT = SCRIPT_DIR.parent  # app/backend/
PROJECT_ROOT = BACKEND_ROOT.parent.parent  # project root
APP_ROOT = PROJECT_ROOT / "app"

# Load environment variables from .env file in project root
load_dotenv(PROJECT_ROOT / ".env")

# Ensure backend services are importable
sys.path.insert(0, str(BACKEND_ROOT))

print(f"   Current working directory: {os.getcwd()}")
print(f"   Project root: {PROJECT_ROOT}")
print(f"   Python path: {sys.path}")
print(f"   Backend root: {BACKEND_ROOT}")
print(f"   Backend exists: {BACKEND_ROOT.exists()}")
print(f"   Env file: {PROJECT_ROOT / ".env"}")

try:
    from services.lakebase_connection_service import LakebaseConnectionService  # noqa: E402
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)


def _read_sql_file(default_path: Path) -> tuple[str, list[list], int]:
    """Read SQL file and extract parameters from comments.
    
    Expected format in SQL file:
    -- PARAMETERS: [[1, "customer_001", 100], [2, "customer_002", 50]]
    -- EXEC_COUNT: 5
    
    Note: CONCURRENCY is no longer supported in SQL files - use --concurrency flag instead.
    
    Returns:
        tuple: (sql_content, parameter_sets, exec_count)
    """
    if not default_path.exists():
        raise FileNotFoundError(f"SQL file not found: {default_path}")
    
    content = default_path.read_text(encoding="utf-8")
    lines = content.split('\n')
    
    sql_lines = []
    parameter_sets = []
    exec_count = 5
    
    for line in lines:
        line = line.strip()
        if line.startswith('-- PARAMETERS:'):
            # Extract JSON from comment
            json_str = line.replace('-- PARAMETERS:', '').strip()
            try:
                parameter_sets = json.loads(json_str)
            except json.JSONDecodeError as e:
                print(f"Warning: Invalid JSON in PARAMETERS comment: {e}")
                parameter_sets = []
        elif line.startswith('-- CONCURRENCY:'):
            print("Warning: CONCURRENCY in SQL file is no longer supported. Use --concurrency flag instead.")
        elif line.startswith('-- EXEC_COUNT:'):
            exec_count = int(line.replace('-- EXEC_COUNT:', '').strip())
        elif not line.startswith('--'):
            # Regular SQL line (not a comment)
            sql_lines.append(line)
    
    sql_content = '\n'.join(sql_lines).strip()
    
    return sql_content, parameter_sets, exec_count


async def main():
    # Enhanced argument parsing - SQL file path and optional concurrency
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description='Run concurrency test on Lakebase PostgreSQL')
    parser.add_argument('sql_file', nargs='?', default=str(APP_ROOT / "queries" / "test.sql"),
                       help='Path to SQL file to execute')
    parser.add_argument('--concurrency', '-c', type=int, required=True,
                       help='Number of concurrent queries to run (required)')
    parser.add_argument('--exec-count', '-e', type=int, default=None,
                       help='Number of executions per scenario (overrides SQL file setting)')
    
    args = parser.parse_args()
    sql_file = args.sql_file
    
    instance_name = os.getenv("LAKEBASE_INSTANCE_NAME")
    database_name = os.getenv("LAKEBASE_DATABASE_NAME")
    access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")
    workspace_url = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    
    print(f"🔍 Environment check:")
    print(f"   LAKEBASE_INSTANCE_NAME:", instance_name)
    print(f"   LAKEBASE_DATABASE_NAME:", database_name)
    print(f"   DATABRICKS_SERVER_HOSTNAME:", workspace_url)

    sql_path = Path(sql_file)
    sql_text, parameter_sets, file_exec_count = _read_sql_file(sql_path)
    
    # Override with command-line arguments if provided
    concurrency = args.concurrency  # Always from command line (required)
    exec_count = args.exec_count if args.exec_count is not None else file_exec_count
    
    print(f"\n📊 Test Configuration:")
    print(f"   SQL File: {sql_path}")
    print(f"   Concurrency: {concurrency} (from command line)")
    print(f"   Exec Count: {exec_count} {'(from command line)' if args.exec_count is not None else '(from SQL file)'}")

    # Build pool config based on concurrency and env overrides
    pool_config = {
        "DB_POOL_SIZE": int(os.getenv("DB_POOL_SIZE", str(max(1, concurrency)))),
        "DB_MAX_OVERFLOW": int(os.getenv("DB_MAX_OVERFLOW", str(max(0, concurrency)))),
        "DB_POOL_TIMEOUT": int(os.getenv("DB_POOL_TIMEOUT", "10")),
        "DB_POOL_RECYCLE_INTERVAL": int(os.getenv("DB_POOL_RECYCLE_INTERVAL", "3600")),
        "DB_COMMAND_TIMEOUT": int(os.getenv("DB_COMMAND_TIMEOUT", "30")),
        "DB_SSL_MODE": os.getenv("DB_SSL_MODE", "require")
    }

    service = LakebaseConnectionService()

    ok = await service.initialize_connection_pool(
        workspace_url=workspace_url,
        instance_name=instance_name,
        database=database_name,
        pool_config=pool_config,
    )
    if not ok:
        print("\n❌ Failed to initialize Lakebase async engine!")
        print("   This could be due to:")
        print("   - Invalid Lakebase instance name")
        print("   - Invalid database name")
        print("   - Invalid Databricks access token")
        print("   - Network connectivity issues")
        print("   - Lakebase instance doesn't exist")
        return

    # Prepare execution payload
    has_parameters = "%s" in sql_text
    
    if has_parameters:
        if parameter_sets:
            print(f"Using parameter sets from SQL file: {len(parameter_sets)} scenarios")
            for i, params in enumerate(parameter_sets, 1):
                print(f"  Scenario {i}: {params}")
        else:
            # No default parameter generation - require explicit parameters
            print("❌ SQL contains parameters but no PARAMETERS comment found!")
            print("   Please add a PARAMETERS comment to your SQL file with the format:")
            print("   -- PARAMETERS: [[param1, param2, param3], [param4, param5, param6]]")
            return sql_text, [], concurrency, exec_count
    else:
        parameter_sets = [[]]  # Single empty parameter set
        print("SQL has no parameters")
    
    # Create test scenarios for each parameter set
    test_scenarios = []
    for i, params in enumerate(parameter_sets, 1):
        test_scenarios.append({
            "name": f"scenario_{i}",
            "parameters": params,
            "execution_count": exec_count,
        })
    
    print(f"\n🔍 Query debugging:")
    print(f"   SQL Query: {sql_text.strip()}")
    if parameter_sets and parameter_sets != [[]]:
        print(f"   Parameter sets: {parameter_sets}")
        # Show what the actual queries will look like
        for i, params in enumerate(parameter_sets):
            if params:
                # Convert %s placeholders to actual values for debugging
                debug_query = sql_text
                for param in params:
                    debug_query = debug_query.replace('%s', str(param), 1)
                print(f"   Example query {i+1}: {debug_query.strip()}")
            else:
                print(f"   Example query {i+1}: {sql_text.strip()}")
    else:
        print(f"   No parameters - query will be executed as-is")
    
    queries = [
        {
            "query_identifier": sql_path.stem,
            "query_content": sql_text,
            "test_scenarios": test_scenarios,
        }
    ]
    print(f"   Queries: {queries}")

    report = await service.execute_concurrent_queries(queries=queries, concurrency_level=concurrency)

    # Print concise summary
    print(
        {
            "test_id": report.test_id,
            "concurrency": report.concurrency_level,
            "total": report.total_queries_executed,
            "success": report.successful_executions,
            "failed": report.failed_executions,
            "avg_ms": report.average_execution_time_ms,
            "p95_ms": report.p95_execution_time_ms,
            "p99_ms": report.p99_execution_time_ms,
            "throughput_qps": report.throughput_queries_per_second,
        }
    )


if __name__ == "__main__":
    asyncio.run(main())


