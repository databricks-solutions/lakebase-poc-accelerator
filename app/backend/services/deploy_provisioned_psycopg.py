#!/usr/bin/env python3
"""
Deploy and Test Psycopg2 Concurrent Query Execution
This script automates concurrent query testing against Lakebase using async psycopg2/asyncpg
"""

import os
import sys
import time
import json
import asyncio
from typing import Dict, Any, List, Optional
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add backend services to path
backend_path = os.path.join(os.path.dirname(__file__), 'app', 'backend')
if backend_path not in sys.path:
    sys.path.insert(0, backend_path)

# Import services
from services.lakebase_connection_service import LakebaseConnectionService
from utils.parameter_parser import SimpleParameterParser


class PsycopgTestDeployer:
    """Handles deployment and execution of psycopg2 concurrent tests"""
    
    def __init__(self, instance_name: str, database_name: str = "databricks_postgres"):
        self.instance_name = instance_name
        self.database_name = database_name
        self.connection_service = LakebaseConnectionService()
        
    async def initialize(self):
        """Initialize connection to Lakebase"""
        print("\n🔧 Initializing Lakebase Connection Service...")
        print("="*70)
        
        try:
            # Verify Databricks authentication
            w = WorkspaceClient()
            user = w.current_user.me()
            print(f"✅ Authenticated as: {user.user_name}")
            
            # Verify instance exists
            instance = w.database.get_database_instance(name=self.instance_name)
            print(f"✅ Found Lakebase instance: {self.instance_name}")
            print(f"   Host: {instance.read_write_dns}")
            print(f"   State: {instance.state.value if instance.state else 'Unknown'}")
            
            return True
            
        except Exception as e:
            print(f"❌ Initialization failed: {e}")
            return False
    
    def create_sample_queries(self) -> List[Dict[str, Any]]:
        """Create sample queries for concurrent testing
        
        Returns list of query configurations with parameters and execution counts
        """
        return [
            {
                "query_identifier": "point_lookup",
                "query_content": "SELECT 1 AS id, 'point_lookup' AS query_type, CAST(%s AS INTEGER) AS param_value;",
                "test_scenarios": [
                    {"name": "scenario_1", "parameters": [1], "execution_count": 20},
                    {"name": "scenario_2", "parameters": [2], "execution_count": 20},
                    {"name": "scenario_3", "parameters": [3], "execution_count": 20},
                    {"name": "scenario_4", "parameters": [4], "execution_count": 20},
                    {"name": "scenario_5", "parameters": [5], "execution_count": 20}
                ]
            },
            {
                "query_identifier": "range_query",
                "query_content": "SELECT CAST(%s AS INTEGER) AS start_val, CAST(%s AS INTEGER) AS end_val, 'range' AS query_type;",
                "test_scenarios": [
                    {"name": "scenario_1", "parameters": [10, 100], "execution_count": 15},
                    {"name": "scenario_2", "parameters": [20, 200], "execution_count": 15},
                    {"name": "scenario_3", "parameters": [30, 300], "execution_count": 15}
                ]
            },
            {
                "query_identifier": "no_params",
                "query_content": "SELECT COUNT(*) AS count_result, 'no_params' AS query_type FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3) t;",
                "test_scenarios": [
                    {"name": "default_scenario", "parameters": [], "execution_count": 25}
                ]
            }
        ]
    
    def parse_query_parameters(self, queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate and prepare query parameters"""
        
        parsed_queries = []
        
        for query in queries:
            # Validate query format
            validation = SimpleParameterParser.validate_query_format(
                query['query_content']
            )
            
            if not validation['is_valid']:
                print(f"⚠️  Query '{query['query_identifier']}' validation warning: {validation.get('message', 'Unknown')}")
            
            # Use provided test scenarios (already defined in create_sample_queries)
            test_scenarios = query.get('test_scenarios', [])
            
            parsed_queries.append({
                'query_identifier': query['query_identifier'],
                'query_content': query['query_content'],
                'test_scenarios': test_scenarios
            })
            
            total_execs = sum(s['execution_count'] for s in test_scenarios)
            print(f"  📝 {query['query_identifier']}: {len(test_scenarios)} scenarios, {total_execs} total executions")
        
        return parsed_queries
    
    async def execute_test(
        self,
        queries: List[Dict[str, Any]],
        concurrency_level: int,
        pool_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute concurrent query test"""
        
        print("\n🚀 Starting Concurrent Query Test...")
        print("="*70)
        print(f"Configuration:")
        print(f"  Instance: {self.instance_name}")
        print(f"  Database: {self.database_name}")
        print(f"  Concurrency Level: {concurrency_level}")
        print(f"  Queries: {len(queries)}")
        
        # Calculate total executions
        total_executions = sum(
            sum(s['execution_count'] for s in q['test_scenarios'])
            for q in queries
        )
        print(f"  Total Executions: {total_executions}")
        print("="*70)
        
        try:
            # Default pool configuration
            if not pool_config:
                pool_config = {
                    'concurrency': concurrency_level,
                    'DB_POOL_SIZE': max(5, concurrency_level // 4),
                    'DB_MAX_OVERFLOW': concurrency_level,
                    'DB_POOL_TIMEOUT': 30,
                    'DB_POOL_RECYCLE_INTERVAL': 3600,
                    'DB_COMMAND_TIMEOUT': 30,
                    'DB_SSL_MODE': 'require'
                }
            
            # Initialize connection pool
            print("\n📦 Initializing connection pool...")
            pool_initialized = await self.connection_service.initialize_connection_pool(
                workspace_url="",  # Uses default from profile
                instance_name=self.instance_name,
                database=self.database_name,
                pool_config=pool_config
            )
            
            if not pool_initialized:
                raise Exception("Failed to initialize connection pool")
            
            print(f"✅ Connection pool initialized")
            print(f"   Base pool size: {pool_config['DB_POOL_SIZE']}")
            print(f"   Max overflow: {pool_config['DB_MAX_OVERFLOW']}")
            
            # Execute concurrent queries
            print(f"\n⚡ Executing {total_executions} queries with concurrency level {concurrency_level}...")
            start_time = time.time()
            
            report = await self.connection_service.execute_concurrent_queries(
                queries=queries,
                concurrency_level=concurrency_level
            )
            
            end_time = time.time()
            
            print(f"\n✅ Test completed in {end_time - start_time:.2f}s")
            
            return report
            
        finally:
            # Cleanup
            try:
                self.connection_service.close_connection_pool()
                print("✅ Connection pool closed")
            except:
                pass
    
    def print_results(self, report: Any):
        """Print test results in a formatted way"""
        
        print("\n" + "="*70)
        print("📊 TEST RESULTS")
        print("="*70)
        
        # Overall metrics
        print(f"\n⏱️  Duration: {report.total_duration_seconds:.2f}s")
        print(f"🔢 Total Queries: {report.total_queries_executed}")
        print(f"✅ Successful: {report.successful_executions}")
        print(f"❌ Failed: {report.failed_executions}")
        print(f"📈 Success Rate: {report.success_rate * 100:.2f}%")
        
        # Performance metrics
        print(f"\n⚡ Performance Metrics:")
        print(f"   Throughput: {report.throughput_queries_per_second:.2f} queries/sec")
        print(f"   Avg Latency: {report.average_execution_time_ms:.2f}ms")
        print(f"   Min Latency: {report.min_execution_time_ms:.2f}ms")
        print(f"   Max Latency: {report.max_execution_time_ms:.2f}ms")
        print(f"   P95 Latency: {report.p95_execution_time_ms:.2f}ms")
        print(f"   P99 Latency: {report.p99_execution_time_ms:.2f}ms")
        
        # Connection pool metrics
        if report.connection_pool_metrics:
            print(f"\n🔌 Connection Pool:")
            print(f"   Pool Size: {report.connection_pool_metrics.get('pool_size', 'N/A')}")
            print(f"   Max Connections: {report.connection_pool_metrics.get('max_connections', 'N/A')}")
            print(f"   Concurrency Level: {report.connection_pool_metrics.get('concurrency_level', 'N/A')}")
        
        # Per-query breakdown
        query_stats = {}
        for result in report.query_results:
            query_id = result.query_identifier
            if query_id not in query_stats:
                query_stats[query_id] = {
                    'total': 0,
                    'success': 0,
                    'failed': 0,
                    'durations': []
                }
            
            query_stats[query_id]['total'] += 1
            if result.success:
                query_stats[query_id]['success'] += 1
                query_stats[query_id]['durations'].append(result.duration_ms)
            else:
                query_stats[query_id]['failed'] += 1
        
        print(f"\n📋 Per-Query Statistics:")
        for query_id, stats in query_stats.items():
            avg_duration = sum(stats['durations']) / len(stats['durations']) if stats['durations'] else 0
            print(f"\n   {query_id}:")
            print(f"      Total: {stats['total']}, Success: {stats['success']}, Failed: {stats['failed']}")
            print(f"      Avg Latency: {avg_duration:.2f}ms")
        
        # Recommendations
        if report.recommendations:
            print(f"\n💡 Recommendations:")
            for rec in report.recommendations:
                print(f"   • {rec}")
        
        print("\n" + "="*70)


async def main():
    """Main execution flow"""
    
    print("\n" + "="*70)
    print("🚀 Psycopg2 Concurrent Query Testing")
    print("="*70)
    
    # Configuration from environment
    # Support both PROVISIONED_ prefix and legacy non-prefixed variables
    INSTANCE_NAME = os.getenv('PROVISIONED_LAKEBASE_INSTANCE_NAME')
    DATABASE_NAME = os.getenv('PROVISIONED_LAKEBASE_DATABASE') 
    CONCURRENCY_LEVEL = int(os.getenv('CONCURRENCY_LEVEL', '10'))
    
    print(f"\n📋 Test Configuration:")
    print(f"   Instance: {INSTANCE_NAME}")
    print(f"   Database: {DATABASE_NAME}")
    print(f"   Concurrency: {CONCURRENCY_LEVEL}")
    
    # Initialize deployer
    deployer = PsycopgTestDeployer(INSTANCE_NAME, DATABASE_NAME)
    
    if not await deployer.initialize():
        print("\n❌ Initialization failed. Exiting.")
        sys.exit(1)
    
    # Create and parse queries
    print(f"\n📝 Parsing Query Configurations...")
    raw_queries = deployer.create_sample_queries()
    parsed_queries = deployer.parse_query_parameters(raw_queries)
    
    # Execute test
    try:
        report = await deployer.execute_test(
            queries=parsed_queries,
            concurrency_level=CONCURRENCY_LEVEL
        )
        
        # Print results
        deployer.print_results(report)
        
        # Exit with appropriate code
        if report.success_rate >= 0.95:
            print("\n✅ Test completed successfully!")
            sys.exit(0)
        else:
            print("\n⚠️  Test completed with warnings (success rate < 95%)")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n👋 Interrupted by user. Exiting.")
        sys.exit(130)
