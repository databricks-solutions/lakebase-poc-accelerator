#!/usr/bin/env python3
"""
Deploy and Test pgbench Job on Databricks (Provisioned Lakebase)

This script automates the deployment and execution of pgbench performance tests
against Lakebase Provisioned instances.

ENVIRONMENT VARIABLES (loaded from .env file):
    Required:
        LAKEBASE_INSTANCE_NAME - Name of your Lakebase Provisioned instance
        LAKEBASE_DATABASE      - Database name (default: databricks_postgres)
    
    Optional:
        DATABRICKS_PROFILE     - Databricks CLI profile (default: DEFAULT)
        DATABRICKS_CLUSTER_ID  - Existing cluster ID (default: auto job cluster)
        PGBENCH_CLIENTS        - Number of concurrent clients (default: 5)
        PGBENCH_JOBS           - Number of worker threads (default: 4)
        PGBENCH_DURATION       - Test duration in seconds (default: 30)

USAGE:
    # Option 1: Load .env file
    export $(cat .env | grep -v '^#' | grep -v '^$' | xargs)
    python3 deploy_provisioned_pgbench_job.py
    
    # Option 2: Set variables directly
    export LAKEBASE_INSTANCE_NAME=my-instance
    export LAKEBASE_DATABASE=databricks_postgres
    python3 deploy_provisioned_pgbench_job.py
    
    # Option 3: Inline environment variables
    LAKEBASE_INSTANCE_NAME=my-instance python3 deploy_provisioned_pgbench_job.py

For unified .env configuration, see: env_unified_template.txt
"""

import os
import sys
import time
import json
from typing import Dict, Any, Optional
from databricks.sdk import WorkspaceClient

# Try to load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    # Try multiple locations: same directory as script, then project root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env_paths = [
        os.path.join(script_dir, '.env'),  # Same dir as script
        os.path.abspath(os.path.join(script_dir, '..', '..', '..', '.env')),  # Project root
    ]
    for env_path in env_paths:
        if os.path.exists(env_path):
            load_dotenv(env_path)
            print(f"✅ Loaded environment from: {env_path}")
            break
except ImportError:
    # python-dotenv not installed, that's okay - use environment variables
    pass

# Add backend directory to path for imports
# Handle both running from services dir and from project root
current_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.abspath(os.path.join(current_dir, '..'))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

# Now import after path is set
from services.databricks_jobs_service import DatabricksJobsService


class PgbenchJobDeployer:
    """Handles deployment and monitoring of pgbench jobs"""
    
    def __init__(self, instance_name: str, database_name: str = "databricks_postgres"):
        self.instance_name = instance_name
        self.database_name = database_name
        self.jobs_service = None
        
    def initialize(self):
        """Initialize Databricks Jobs Service"""
        print("\n🔧 Initializing Databricks Jobs Service...")
        print("="*70)
        
        try:
            # Set custom job name for Provisioned
            # Note: DatabricksJobsService adds '_job' suffix, so we set the base name
            os.environ['DATABRICKS_APP_NAME'] = 'provisioned_pgbench_test'
            
            # Initialize without workspace URL (will use default profile)
            self.jobs_service = DatabricksJobsService()
            
            # Test connection
            w = self.jobs_service._get_client()
            user = w.current_user.me()
            print(f"✅ Connected to Databricks as: {user.user_name}")
            
            # Verify instance exists
            instance = w.database.get_database_instance(name=self.instance_name)
            print(f"✅ Found Lakebase instance: {self.instance_name}")
            print(f"   Host: {instance.read_write_dns}")
            print(f"   State: {instance.state.value if instance.state else 'Unknown'}")
            
            return True
            
        except Exception as e:
            print(f"❌ Initialization failed: {e}")
            return False
    
    def create_sample_queries(self) -> list:
        """Create sample pgbench queries for testing
        
        Note: The notebook expects 'name', 'content', and 'weight' keys.
        Queries must be valid SQL statements.
        """
        return [
            {
                "name": "point_lookup",
                "content": "SELECT 1 AS point_lookup;",
                "weight": 60
            },
            {
                "name": "range_scan",
                "content": "SELECT 1 + 1 AS range_result;",
                "weight": 30
            },
            {
                "name": "aggregation",
                "content": "SELECT COUNT(*) AS total FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3) t;",
                "weight": 10
            }
        ]
    
    def create_pgbench_config(
        self,
        clients: int = 5,
        jobs: int = 4,
        duration: int = 30,
        protocol: str = "prepared"
    ) -> Dict[str, Any]:
        """Create pgbench configuration"""
        return {
            "pgbench_clients": clients,
            "pgbench_jobs": jobs,
            "pgbench_duration": duration,
            "pgbench_progress_interval": 5,
            "pgbench_protocol": protocol,
            "pgbench_per_statement_latency": True,
            "pgbench_detailed_logging": True,
            "pgbench_connect_per_transaction": False
        }
    
    def submit_job(
        self,
        pgbench_config: Dict[str, Any],
        query_configs: list,
        cluster_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Submit pgbench job to Databricks"""
        
        print("\n📤 Submitting pgbench job...")
        print("="*70)
        print(f"Configuration:")
        print(f"  Instance: {self.instance_name}")
        print(f"  Database: {self.database_name}")
        print(f"  Clients: {pgbench_config['pgbench_clients']}")
        print(f"  Jobs (threads): {pgbench_config['pgbench_jobs']}")
        print(f"  Duration: {pgbench_config['pgbench_duration']}s")
        print(f"  Queries: {len(query_configs)}")
        print(f"  Cluster: {'Auto (job cluster)' if not cluster_id else cluster_id}")
        print("="*70)
        
        try:
            result = self.jobs_service.submit_pgbench_job(
                lakebase_instance_name=self.instance_name,
                database_name=self.database_name,
                cluster_id=cluster_id,
                pgbench_config=pgbench_config,
                query_configs=query_configs
            )
            
            print(f"\n✅ Job submitted successfully!")
            print(f"   Job ID: {result['job_id']}")
            print(f"   Run ID: {result['run_id']}")
            print(f"   Job Name: {result.get('job_name', 'N/A')}")
            
            if result.get('job_run_url'):
                print(f"\n🔗 Job Run URL:")
                print(f"   {result['job_run_url']}")
            
            if result.get('job_url'):
                print(f"\n🔗 Job URL:")
                print(f"   {result['job_url']}")
            
            return result
            
        except Exception as e:
            print(f"❌ Job submission failed: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def monitor_job(self, run_id: str, poll_interval: int = 10) -> Dict[str, Any]:
        """Monitor job execution until completion"""
        
        print(f"\n👀 Monitoring job run: {run_id}")
        print("="*70)
        
        start_time = time.time()
        last_status = None
        
        while True:
            try:
                status = self.jobs_service.get_run_status(run_id)
                
                # Print status update if changed
                if status['status'] != last_status:
                    elapsed = int(time.time() - start_time)
                    print(f"[{elapsed}s] Status: {status['status'].upper()} - {status['message']}")
                    last_status = status['status']
                
                # Check if job is done
                if status['status'] in ['completed', 'failed']:
                    print("="*70)
                    
                    if status['status'] == 'completed':
                        print("🎉 Job completed successfully!")
                        
                        # Print results if available
                        if status.get('results'):
                            print("\n📊 Test Results:")
                            results = status['results']
                            
                            if 'test_parameters' in results:
                                params = results['test_parameters']
                                print(f"   Clients: {params.get('clients', 'N/A')}")
                                print(f"   Duration: {params.get('duration_seconds', 'N/A')}s")
                            
                            if 'performance_metrics' in results:
                                metrics = results['performance_metrics']
                                print(f"\n   Performance Metrics:")
                                print(f"   - TPS: {metrics.get('tps', 'N/A')}")
                                print(f"   - Avg Latency: {metrics.get('latency_avg_ms', 'N/A')}ms")
                                print(f"   - P95 Latency: {metrics.get('latency_p95_ms', 'N/A')}ms")
                                print(f"   - P99 Latency: {metrics.get('latency_p99_ms', 'N/A')}ms")
                                print(f"   - Total Transactions: {metrics.get('total_transactions', 'N/A')}")
                        
                        # Print pgbench summary if available
                        if status.get('pgbench_results'):
                            pgb = status['pgbench_results']
                            print(f"\n   pgbench Summary:")
                            print(f"   - TPS: {pgb.get('tps', 'N/A')}")
                            print(f"   - Latency: {pgb.get('latency_avg_ms', 'N/A')}ms")
                            print(f"   - Transactions: {pgb.get('total_transactions', 'N/A')}")
                            print(f"   - Failed: {pgb.get('failed_transactions', 0)}")
                            
                            if pgb.get('per_query_stats'):
                                print(f"\n   Per-Query Stats:")
                                for stat in pgb['per_query_stats']:
                                    print(f"   - {stat['query_name']}: {stat['tps']} TPS, {stat['latency_avg_ms']}ms")
                    
                    else:
                        print("❌ Job failed!")
                        print(f"   Error: {status.get('message', 'Unknown error')}")
                    
                    return status
                
                # Wait before next poll
                time.sleep(poll_interval)
                
            except KeyboardInterrupt:
                print("\n\n⚠️  Monitoring interrupted by user")
                print("   Job is still running in Databricks")
                return status
            
            except Exception as e:
                print(f"❌ Error monitoring job: {e}")
                return status


def main():
    """Main execution flow"""
    
    print("\n" + "="*70)
    print("🚀 Databricks Pgbench Job Deployment & Testing (Provisioned Lakebase)")
    print("="*70)
    
    # Validate required environment variables
    # Check for PROVISIONED_ prefix first, then fall back to non-prefixed for backward compatibility
    INSTANCE_NAME = os.getenv('PROVISIONED_LAKEBASE_INSTANCE_NAME') or os.getenv('LAKEBASE_INSTANCE_NAME')
    if not INSTANCE_NAME:
        print("\n❌ Error: PROVISIONED_LAKEBASE_INSTANCE_NAME (or LAKEBASE_INSTANCE_NAME) environment variable is required")
        print("\nPlease set your environment variables:")
        print("  Option 1: Load from .env file")
        print("    export $(cat .env | grep -v '^#' | grep -v '^$' | xargs)")
        print("  Option 2: Set directly")
        print("    export PROVISIONED_LAKEBASE_INSTANCE_NAME=my-lakebase-instance")
        print("    # OR (legacy)")
        print("    export LAKEBASE_INSTANCE_NAME=my-lakebase-instance")
        print("\nFor template, see: env_unified_template.txt")
        sys.exit(1)
    
    # Configuration with defaults
    DATABASE_NAME = os.getenv('PROVISIONED_LAKEBASE_DATABASE') or os.getenv('LAKEBASE_DATABASE', 'databricks_postgres')
    CLUSTER_ID = os.getenv('DATABRICKS_CLUSTER_ID', None)  # None = auto job cluster
    PROFILE = os.getenv('DATABRICKS_PROFILE', 'DEFAULT')
    
    # Test parameters with defaults
    TEST_CLIENTS = int(os.getenv('PGBENCH_CLIENTS', '5'))
    TEST_JOBS = int(os.getenv('PGBENCH_JOBS', '4'))
    TEST_DURATION = int(os.getenv('PGBENCH_DURATION', '30'))
    
    print(f"\n📋 Test Configuration:")
    print(f"   Instance: {INSTANCE_NAME}")
    print(f"   Database: {DATABASE_NAME}")
    print(f"   Profile: {PROFILE}")
    print(f"   Clients: {TEST_CLIENTS}")
    print(f"   Jobs: {TEST_JOBS}")
    print(f"   Duration: {TEST_DURATION}s")
    print(f"   Cluster: {'Auto (ephemeral job cluster)' if not CLUSTER_ID else CLUSTER_ID}")
    
    # Initialize deployer
    deployer = PgbenchJobDeployer(INSTANCE_NAME, DATABASE_NAME)
    
    if not deployer.initialize():
        print("\n❌ Initialization failed. Exiting.")
        sys.exit(1)
    
    # Create test configuration
    pgbench_config = deployer.create_pgbench_config(
        clients=TEST_CLIENTS,
        jobs=TEST_JOBS,
        duration=TEST_DURATION
    )
    
    query_configs = deployer.create_sample_queries()
    
    # Submit job
    try:
        result = deployer.submit_job(
            pgbench_config=pgbench_config,
            query_configs=query_configs,
            cluster_id=CLUSTER_ID
        )
        
        # Monitor job execution
        final_status = deployer.monitor_job(result['run_id'])
        
        # Exit with appropriate code
        if final_status['status'] == 'completed':
            print("\n✅ Test completed successfully!")
            sys.exit(0)
        else:
            print("\n❌ Test failed")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Deployment failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Interrupted by user. Exiting.")
        sys.exit(130)
