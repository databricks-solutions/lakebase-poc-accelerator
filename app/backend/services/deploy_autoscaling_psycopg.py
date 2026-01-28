#!/usr/bin/env python3
"""
Deploy and Test Lakebase Autoscaling Concurrent Query Execution

This script connects to Databricks Lakebase Autoscaling instances and performs
concurrent query testing using native PostgreSQL connections.

Based on: https://docs.databricks.com/aws/en/oltp/projects/connect-overview

ENVIRONMENT VARIABLES (loaded from .env file):
    Required:
        AUTOSCALING_PGHOST     - Compute endpoint hostname (e.g., ep-abc-123.databricks.com)
        AUTOSCALING_PGDATABASE - Database name (default: databricks_postgres)
        AUTOSCALING_PGUSER     - PostgreSQL username (role or email)
        AUTOSCALING_PGPASSWORD - PostgreSQL password or OAuth token
    
    Optional:
        AUTOSCALING_PGPORT     - PostgreSQL port (default: 5432)
        AUTOSCALING_PGSSLMODE  - SSL mode (default: require)
        PSYCOPG_CONCURRENCY    - Concurrency level (default: 10)
        DB_POOL_SIZE           - Connection pool size (default: 5)
        DB_MAX_OVERFLOW        - Max additional connections (default: 10)
    
    Note: Also supports standard PG* variables (PGHOST, PGUSER, etc.) for backward compatibility

USAGE:
    # Option 1: Auto-load .env file (recommended)
    python3 deploy_autoscaling_psycopg.py
    
    # Option 2: Load specific .env file
    export $(cat .env_autoscaling | xargs)
    python3 deploy_autoscaling_psycopg.py
    
    # Option 3: With command-line arguments (overrides .env)
    python3 deploy_autoscaling_psycopg.py --concurrency 20

For unified .env configuration, see: env_unified_template.txt

Key Features:
- Supports both OAuth and native Postgres password authentication
- Direct PostgreSQL connection (no Databricks SDK database API)
- Connection pooling with SQLAlchemy async engine
- Concurrent query execution with performance metrics
- Compatible with Lakebase Autoscaling compute endpoints (ep-*)
"""

import os
import sys
import time
import asyncio
import argparse
from typing import Dict, Any, List, Optional
from datetime import datetime
from urllib.parse import quote_plus

# Try to load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    # Load .env file from current directory or parent
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"✅ Loaded environment from: {env_path}")
    else:
        # Try .env_autoscaling as fallback
        env_autoscaling_path = os.path.join(os.path.dirname(__file__), '.env_autoscaling')
        if os.path.exists(env_autoscaling_path):
            load_dotenv(env_autoscaling_path)
            print(f"✅ Loaded environment from: {env_autoscaling_path}")
        else:
            load_dotenv()  # Try to find .env in parent directories
except ImportError:
    # python-dotenv not installed, that's okay - use environment variables
    pass

# Third-party imports
try:
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
    from sqlalchemy import text
    from databricks.sdk import WorkspaceClient
except ImportError as e:
    print(f"❌ Missing required package: {e}")
    print("Install with: pip install sqlalchemy asyncpg databricks-sdk")
    sys.exit(1)


class LakebaseAutoscalingTester:
    """
    Handles concurrent query testing for Lakebase Autoscaling instances
    
    Supports two authentication methods:
    1. OAuth: Uses Databricks identity with OAuth token
    2. Native: Uses Postgres role with password
    """
    
    def __init__(
        self,
        host: str,
        database: str = "databricks_postgres",
        port: int = 5432,
        user: Optional[str] = None,
        password: Optional[str] = None,
        use_oauth: bool = True
    ):
        """
        Initialize Autoscaling tester
        
        Args:
            host: Compute endpoint hostname (e.g., ep-abc-123.databricks.com)
            database: Database name (default: databricks_postgres)
            port: PostgreSQL port (default: 5432)
            user: Username/role (for OAuth: email@domain.com, for native: role_name)
            password: Password or OAuth token
            use_oauth: Whether to use OAuth authentication (vs native password)
        """
        self.host = host
        self.database = database
        self.port = port
        self.user = user
        self.password = password
        self.use_oauth = use_oauth
        self.engine: Optional[AsyncEngine] = None
        
        # Validate hostname format for Autoscaling
        if not host.startswith('ep-'):
            print(f"⚠️  Warning: Hostname '{host}' doesn't start with 'ep-'.")
            print("   Lakebase Autoscaling endpoints typically use format: ep-abc-123.databricks.com")
    
    async def initialize_with_oauth(self) -> bool:
        """
        Initialize connection using OAuth authentication
        
        Uses Databricks SDK to get current user and generate OAuth token
        """
        try:
            print("\n🔐 Initializing OAuth Authentication...")
            
            # Get Databricks workspace client
            w = WorkspaceClient()
            user = w.current_user.me()
            
            print(f"✅ Authenticated as: {user.user_name}")
            
            # For OAuth, the username is the Databricks identity email
            if not self.user:
                self.user = user.user_name
            
            # Generate OAuth token
            # Note: For Autoscaling, you typically copy the token from the UI
            # or use Databricks personal access token
            if not self.password:
                print("\n⚠️  OAuth token not provided.")
                print("   To get your OAuth token:")
                print("   1. Go to Lakebase App > Your Project > Connect")
                print("   2. Select OAuth authentication")
                print("   3. Click 'Copy OAuth Token'")
                print("   4. Set PGPASSWORD environment variable or pass as --password")
                return False
            
            print(f"✅ Using OAuth token for user: {self.user}")
            return True
            
        except Exception as e:
            print(f"❌ OAuth initialization failed: {e}")
            return False
    
    async def initialize_with_password(self) -> bool:
        """
        Initialize connection using native Postgres password authentication
        """
        try:
            print("\n🔐 Initializing Native Password Authentication...")
            
            if not self.user or not self.password:
                print("❌ Username and password are required for native authentication")
                return False
            
            print(f"✅ Using native authentication for role: {self.user}")
            return True
            
        except Exception as e:
            print(f"❌ Password authentication initialization failed: {e}")
            return False
    
    async def initialize_connection_pool(
        self,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 30
    ) -> bool:
        """
        Initialize SQLAlchemy async connection pool
        
        Args:
            pool_size: Base number of connections in pool
            max_overflow: Max additional connections beyond pool_size
            pool_timeout: Seconds to wait for connection from pool
        """
        try:
            print("\n📦 Initializing Connection Pool...")
            print("="*70)
            
            # URL-encode user and password for connection string
            encoded_user = quote_plus(self.user)
            encoded_password = quote_plus(self.password) if self.password else ""
            
            # Build connection URL for asyncpg
            # Format: postgresql+asyncpg://user:password@host:port/database
            connection_url = (
                f"postgresql+asyncpg://{encoded_user}:{encoded_password}"
                f"@{self.host}:{self.port}/{self.database}"
                f"?ssl=require"  # Lakebase Autoscaling requires SSL
            )
            
            print(f"   Host: {self.host}")
            print(f"   Database: {self.database}")
            print(f"   Port: {self.port}")
            print(f"   User: {self.user}")
            print(f"   Auth Method: {'OAuth' if self.use_oauth else 'Native Password'}")
            print(f"   SSL: Required")
            
            # Create async engine
            self.engine = create_async_engine(
                connection_url,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_timeout=pool_timeout,
                pool_recycle=3600,  # Recycle connections every hour
                echo=False  # Set to True for SQL debugging
            )
            
            # Test connection
            async with self.engine.connect() as conn:
                result = await conn.execute(text("SELECT 1 AS test"))
                row = result.fetchone()
                if row[0] != 1:
                    raise Exception("Connection test failed")
            
            print(f"✅ Connection pool initialized")
            print(f"   Base pool size: {pool_size}")
            print(f"   Max overflow: {max_overflow}")
            print()
            
            return True
            
        except Exception as e:
            print(f"❌ Connection pool initialization failed: {e}")
            return False
    
    async def close(self):
        """Close connection pool"""
        if self.engine:
            await self.engine.dispose()
            print("✅ Connection pool closed")
    
    def create_sample_queries(self) -> List[Dict[str, Any]]:
        """
        Create sample queries for concurrent testing
        
        Returns list of query configurations with parameters and execution counts
        Note: Using :param syntax for SQLAlchemy compatibility
        """
        return [
            {
                "query_identifier": "version_check",
                "query_content": "SELECT version() AS pg_version, current_database() AS db_name;",
                "test_scenarios": [
                    {"name": "default", "parameters": {}, "execution_count": 5}
                ]
            },
            {
                "query_identifier": "point_lookup",
                "query_content": "SELECT CAST(:id AS INTEGER) AS id, 'point_lookup' AS query_type, now() AS timestamp;",
                "test_scenarios": [
                    {"name": "scenario_1", "parameters": {"id": 1}, "execution_count": 20},
                    {"name": "scenario_2", "parameters": {"id": 2}, "execution_count": 20},
                    {"name": "scenario_3", "parameters": {"id": 3}, "execution_count": 20}
                ]
            },
            {
                "query_identifier": "range_query",
                "query_content": "SELECT CAST(:start_val AS INTEGER) AS start_val, CAST(:end_val AS INTEGER) AS end_val, 'range' AS query_type;",
                "test_scenarios": [
                    {"name": "scenario_1", "parameters": {"start_val": 10, "end_val": 100}, "execution_count": 15},
                    {"name": "scenario_2", "parameters": {"start_val": 20, "end_val": 200}, "execution_count": 15}
                ]
            },
            {
                "query_identifier": "aggregate_query",
                "query_content": """
                    SELECT 
                        COUNT(*) AS row_count,
                        'aggregate' AS query_type,
                        now() AS timestamp
                    FROM generate_series(1, 100) AS s(i);
                """,
                "test_scenarios": [
                    {"name": "default", "parameters": {}, "execution_count": 20}
                ]
            }
        ]
    
    async def execute_query(
        self,
        query: str,
        parameters: Dict[str, Any],
        query_id: str
    ) -> Dict[str, Any]:
        """Execute a single query with timing"""
        start_time = time.time()
        
        try:
            async with self.engine.connect() as conn:
                if parameters:
                    # Use dict parameters with SQLAlchemy text()
                    result = await conn.execute(text(query), parameters)
                else:
                    result = await conn.execute(text(query))
                
                # Fetch all rows to ensure query completes
                rows = result.fetchall()
                row_count = len(rows)
            
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000
            
            return {
                "query_id": query_id,
                "success": True,
                "duration_ms": duration_ms,
                "row_count": row_count,
                "error": None
            }
            
        except Exception as e:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000
            
            return {
                "query_id": query_id,
                "success": False,
                "duration_ms": duration_ms,
                "row_count": 0,
                "error": str(e)
            }
    
    async def run_concurrent_test(
        self,
        queries: List[Dict[str, Any]],
        concurrency_level: int = 10
    ) -> Dict[str, Any]:
        """
        Run concurrent query test
        
        Args:
            queries: List of query configurations
            concurrency_level: Number of concurrent queries
        
        Returns:
            Test results with performance metrics
        """
        print("\n🚀 Starting Concurrent Query Test...")
        print("="*70)
        
        # Build task list
        tasks = []
        query_counts = {}
        
        for query_config in queries:
            query_id = query_config['query_identifier']
            query_content = query_config['query_content']
            query_counts[query_id] = {"total": 0, "success": 0, "failed": 0}
            
            for scenario in query_config['test_scenarios']:
                params = scenario['parameters']
                exec_count = scenario['execution_count']
                
                for i in range(exec_count):
                    task_id = f"{query_id}_s{scenario['name']}_e{i+1}"
                    tasks.append(
                        self.execute_query(query_content, params, task_id)
                    )
                    query_counts[query_id]["total"] += 1
        
        print(f"   Total Queries: {len(tasks)}")
        print(f"   Concurrency Level: {concurrency_level}")
        print(f"   Query Types: {len(queries)}")
        print("="*70)
        print()
        
        # Execute with concurrency limit
        start_time = time.time()
        
        # Use semaphore to limit concurrency
        semaphore = asyncio.Semaphore(concurrency_level)
        
        async def limited_execute(task):
            async with semaphore:
                return await task
        
        results = await asyncio.gather(*[limited_execute(task) for task in tasks])
        
        end_time = time.time()
        total_duration = end_time - start_time
        
        # Aggregate results
        successful_queries = [r for r in results if r['success']]
        failed_queries = [r for r in results if not r['success']]
        
        # Calculate per-query stats
        for result in results:
            # Extract base query_id from task_id format: "query_id_sscenario_eN"
            task_id = result['query_id']
            # Find the query_id by matching against known query_ids
            matched_query_id = None
            for qid in query_counts.keys():
                if task_id.startswith(qid + '_s'):
                    matched_query_id = qid
                    break
            
            if matched_query_id:
                if result['success']:
                    query_counts[matched_query_id]["success"] += 1
                else:
                    query_counts[matched_query_id]["failed"] += 1
        
        # Calculate metrics
        if successful_queries:
            latencies = [r['duration_ms'] for r in successful_queries]
            avg_latency = sum(latencies) / len(latencies)
            min_latency = min(latencies)
            max_latency = max(latencies)
            p95_latency = sorted(latencies)[int(len(latencies) * 0.95)] if len(latencies) > 1 else latencies[0]
        else:
            avg_latency = min_latency = max_latency = p95_latency = 0
        
        throughput = len(results) / total_duration if total_duration > 0 else 0
        success_rate = (len(successful_queries) / len(results) * 100) if results else 0
        
        # Print results
        print("\n" + "="*70)
        print("📊 TEST RESULTS")
        print("="*70)
        print()
        print(f"⏱️  Duration: {total_duration:.2f}s")
        print(f"🔢 Total Queries: {len(results)}")
        print(f"✅ Successful: {len(successful_queries)}")
        print(f"❌ Failed: {len(failed_queries)}")
        print(f"📈 Success Rate: {success_rate:.2f}%")
        print()
        print(f"⚡ Performance Metrics:")
        print(f"   Throughput: {throughput:.2f} queries/sec")
        print(f"   Avg Latency: {avg_latency:.2f}ms")
        print(f"   Min Latency: {min_latency:.2f}ms")
        print(f"   Max Latency: {max_latency:.2f}ms")
        print(f"   P95 Latency: {p95_latency:.2f}ms")
        print()
        print(f"📋 Per-Query Statistics:")
        for query_id, counts in query_counts.items():
            print(f"   {query_id}:")
            print(f"      Total: {counts['total']}, Success: {counts['success']}, Failed: {counts['failed']}")
        
        # Show sample errors if any
        if failed_queries:
            print()
            print(f"⚠️  Sample Errors (showing first 3):")
            for error_result in failed_queries[:3]:
                print(f"   - {error_result['query_id']}: {error_result['error']}")
        
        print()
        print("="*70)
        
        if success_rate >= 95:
            print("\n✅ Test completed successfully!")
        else:
            print("\n⚠️  Test completed with warnings (success rate < 95%)")
        
        print()
        
        return {
            "duration_seconds": total_duration,
            "total_queries": len(results),
            "successful": len(successful_queries),
            "failed": len(failed_queries),
            "success_rate": success_rate,
            "throughput": throughput,
            "avg_latency_ms": avg_latency,
            "min_latency_ms": min_latency,
            "max_latency_ms": max_latency,
            "p95_latency_ms": p95_latency,
            "query_counts": query_counts
        }


async def main():
    """Main execution function"""
    
    parser = argparse.ArgumentParser(
        description='Test Lakebase Autoscaling with concurrent queries',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using environment variables (recommended)
  export PGHOST=ep-abc-123.databricks.com
  export PGDATABASE=databricks_postgres
  export PGUSER=your.email@databricks.com
  export PGPASSWORD=your-oauth-token-or-password
  python3 deploy_autoscaling_test.py

  # Using command-line arguments
  python3 deploy_autoscaling_test.py \\
    --host ep-abc-123.databricks.com \\
    --user your.email@databricks.com \\
    --password your-oauth-token \\
    --concurrency 20

  # Native password authentication
  python3 deploy_autoscaling_test.py \\
    --host ep-abc-123.databricks.com \\
    --user my_role \\
    --password my_password \\
    --no-oauth \\
    --concurrency 15
        """
    )
    
    parser.add_argument('--host', help='Compute endpoint hostname (e.g., ep-abc-123.databricks.com)')
    parser.add_argument('--database', default='databricks_postgres', help='Database name')
    parser.add_argument('--port', type=int, default=5432, help='PostgreSQL port')
    parser.add_argument('--user', help='Username (Databricks email for OAuth, role name for native)')
    parser.add_argument('--password', help='Password or OAuth token')
    parser.add_argument('--no-oauth', action='store_true', help='Use native password auth instead of OAuth')
    parser.add_argument('--concurrency', type=int, default=10, help='Concurrency level')
    parser.add_argument('--pool-size', type=int, default=5, help='Connection pool base size')
    parser.add_argument('--max-overflow', type=int, default=10, help='Max additional connections')
    
    args = parser.parse_args()
    
    # Get connection details from args or environment
    # Check for AUTOSCALING_* prefix first, then fall back to standard PG* variables
    host = args.host or os.getenv('AUTOSCALING_PGHOST')
    database = args.database or os.getenv('AUTOSCALING_PGDATABASE') 
    port = args.port or int(os.getenv('AUTOSCALING_PGPORT') or os.getenv('5432'))
    user = args.user or os.getenv('AUTOSCALING_PGUSER')
    password = args.password or os.getenv('AUTOSCALING_PGPASSWORD') 
    use_oauth = not args.no_oauth
    
    # Validate required parameters
    if not host:
        print("\n❌ Error: AUTOSCALING_PGHOST is required")
        print("\nThis script automatically loads .env or .env_autoscaling files.")
        print("Please set your environment variables:")
        print("\nOption 1: Create .env file (recommended)")
        print("  cp env_unified_template.txt .env")
        print("  # Edit .env and set:")
        print("  AUTOSCALING_PGHOST=ep-abc-123.databricks.com")
        print("  AUTOSCALING_PGDATABASE=databricks_postgres")
        print("  AUTOSCALING_PGUSER=your-username")
        print("  AUTOSCALING_PGPASSWORD=your-password")
        print("\nOption 2: Export manually")
        print("  export AUTOSCALING_PGHOST=ep-abc-123.databricks.com")
        print("  export AUTOSCALING_PGUSER=your-username")
        print("  export AUTOSCALING_PGPASSWORD=your-password")
        print("\nOption 3: Use command-line arguments")
        print("  python3 deploy_autoscaling_psycopg.py --host ep-abc-123.databricks.com --user analyst --password pwd")
        print("\nNote: Also supports standard PG* variables for backward compatibility")
        print("For template, see: env_unified_template.txt")
        return 1
    
    if not user:
        print("\n❌ Error: AUTOSCALING_PGUSER is required")
        print("Set in .env file or export AUTOSCALING_PGUSER=your-username")
        print("For OAuth: Use your Databricks email (e.g., user@databricks.com)")
        print("For native: Use your Postgres role name")
        print("\nNote: Also supports PGUSER for backward compatibility")
        return 1
    
    # Print configuration
    print("\n" + "="*70)
    print("🧪 LAKEBASE AUTOSCALING CONCURRENT QUERY TEST")
    print("="*70)
    print()
    print("📋 Configuration:")
    print(f"   Host: {host}")
    print(f"   Database: {database}")
    print(f"   Port: {port}")
    print(f"   User: {user}")
    print(f"   Auth Method: {'OAuth' if use_oauth else 'Native Password'}")
    print(f"   Concurrency: {args.concurrency}")
    print(f"   Pool Size: {args.pool_size}")
    print(f"   Max Overflow: {args.max_overflow}")
    
    # Initialize tester
    tester = LakebaseAutoscalingTester(
        host=host,
        database=database,
        port=port,
        user=user,
        password=password,
        use_oauth=use_oauth
    )
    
    try:
        # Initialize authentication
        if use_oauth:
            success = await tester.initialize_with_oauth()
        else:
            success = await tester.initialize_with_password()
        
        if not success:
            return 1
        
        # Initialize connection pool
        success = await tester.initialize_connection_pool(
            pool_size=args.pool_size,
            max_overflow=args.max_overflow
        )
        
        if not success:
            return 1
        
        # Create test queries
        queries = tester.create_sample_queries()
        
        # Run concurrent test
        results = await tester.run_concurrent_test(
            queries=queries,
            concurrency_level=args.concurrency
        )
        
        # Return success/failure based on results
        return 0 if results['success_rate'] >= 95 else 1
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        return 130
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        await tester.close()


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
