#!/usr/bin/env python3
"""
Deploy and Test pgbench Job on Databricks (Autoscaling Lakebase)

This script automates the deployment and execution of pgbench performance tests
against Lakebase Autoscaling instances using Databricks Jobs.

ENVIRONMENT VARIABLES (loaded from .env file):
    Required:
        AUTOSCALING_PGHOST     - Compute endpoint hostname (e.g., ep-abc-123.databricks.com)
        AUTOSCALING_PGDATABASE - Database name (default: databricks_postgres)
        AUTOSCALING_PGUSER     - PostgreSQL username (role or email)
        AUTOSCALING_PGPASSWORD - PostgreSQL password or OAuth token
    
    Optional:
        AUTOSCALING_PGPORT     - PostgreSQL port (default: 5432)
        AUTOSCALING_PGSSLMODE  - SSL mode (default: require)
        DATABRICKS_PROFILE     - Databricks CLI profile (default: DEFAULT)
        DATABRICKS_CLUSTER_ID  - Existing cluster ID (default: auto job cluster)
        PGBENCH_CLIENTS        - Number of concurrent clients (default: 5)
        PGBENCH_JOBS           - Number of worker threads (default: 4)
        PGBENCH_DURATION       - Test duration in seconds (default: 30)
    
    Note: Also supports standard PG* variables (PGHOST, PGUSER, etc.) for backward compatibility

USAGE:
    # Option 1: Load .env file
    export $(cat .env | grep -v '^#' | grep -v '^$' | xargs)
    python3 deploy_autoscaling_pgbench_job.py
    
    # Option 2: Load specific .env file
    export $(cat .env_autoscaling | xargs)
    python3 deploy_autoscaling_pgbench_job.py
    
    # Option 3: Set variables directly
    export PGHOST=ep-abc-123.databricks.com
    export PGUSER=analyst
    export PGPASSWORD=my-password
    python3 deploy_autoscaling_pgbench_job.py
    
    # Option 4: Inline environment variables
    PGHOST=ep-abc-123.databricks.com PGUSER=analyst PGPASSWORD=pwd \\
      python3 deploy_autoscaling_pgbench_job.py

For unified .env configuration, see: env_unified_template.txt

NOTES:
    - This is for Lakebase AUTOSCALING (ep-* hostnames)
    - For Provisioned Lakebase, use: deploy_provisioned_pgbench_job.py
    - See DEPLOYMENT_COMPARISON.md for detailed differences
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

# Add backend services to path
backend_path = os.path.join(os.path.dirname(__file__), 'app', 'backend')
if backend_path not in sys.path:
    sys.path.insert(0, backend_path)

# Now import after path is set
from services.databricks_jobs_service import DatabricksJobsService


class AutoscalingPgbenchDeployer:
    """Handles deployment and monitoring of pgbench jobs for Autoscaling Lakebase"""
    
    def __init__(
        self,
        pghost: str,
        pgdatabase: str,
        pguser: str,
        pgpassword: str,
        pgport: int = 5432,
        pgsslmode: str = "require"
    ):
        """
        Initialize deployer for Autoscaling Lakebase
        
        Args:
            pghost: Compute endpoint hostname (e.g., ep-abc-123.databricks.com)
            pgdatabase: Database name
            pguser: PostgreSQL username/role
            pgpassword: PostgreSQL password or OAuth token
            pgport: PostgreSQL port (default: 5432)
            pgsslmode: SSL mode (default: require)
        """
        self.pghost = pghost
        self.pgdatabase = pgdatabase
        self.pguser = pguser
        self.pgpassword = pgpassword
        self.pgport = pgport
        self.pgsslmode = pgsslmode
        self.jobs_service = None
        
        # Validate hostname format
        if not pghost.startswith('ep-'):
            print(f"⚠️  Warning: Hostname '{pghost}' doesn't start with 'ep-'.")
            print("   Autoscaling endpoints typically use format: ep-abc-123.databricks.com")
            print("   For Provisioned Lakebase, use: deploy_provisioned_pgbench_job.py")
        
    def initialize(self):
        """Initialize Databricks Jobs Service"""
        print("\n🔧 Initializing Databricks Jobs Service (Autoscaling Mode)...")
        print("="*70)
        
        try:
            # Set custom job name for Autoscaling
            # Note: DatabricksJobsService adds '_job' suffix, so we set the base name
            os.environ['DATABRICKS_APP_NAME'] = 'autoscaling_pgbench_test'
            
            # Initialize without workspace URL (will use default profile)
            self.jobs_service = DatabricksJobsService()
            
            # Test connection
            w = self.jobs_service._get_client()
            user = w.current_user.me()
            print(f"✅ Connected to Databricks as: {user.user_name}")
            
            # Display connection info
            print(f"✅ Target Autoscaling endpoint:")
            print(f"   Host: {self.pghost}")
            print(f"   Database: {self.pgdatabase}")
            print(f"   User: {self.pguser}")
            print(f"   Port: {self.pgport}")
            print(f"   SSL Mode: {self.pgsslmode}")
            
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
        """Submit pgbench job to Databricks for Autoscaling endpoint"""
        
        print("\n📤 Submitting pgbench job...")
        print("="*70)
        print(f"Configuration:")
        print(f"  Endpoint: {self.pghost}")
        print(f"  Database: {self.pgdatabase}")
        print(f"  User: {self.pguser}")
        print(f"  Clients: {pgbench_config['pgbench_clients']}")
        print(f"  Jobs (threads): {pgbench_config['pgbench_jobs']}")
        print(f"  Duration: {pgbench_config['pgbench_duration']}s")
        print(f"  Queries: {len(query_configs)}")
        print(f"  Cluster: {'Auto (job cluster)' if not cluster_id else cluster_id}")
        print("="*70)
        
        try:
            # Create connection parameters for Autoscaling
            connection_params = {
                "connection_type": "autoscaling",
                "pghost": self.pghost,
                "pgdatabase": self.pgdatabase,
                "pguser": self.pguser,
                "pgpassword": self.pgpassword,
                "pgport": str(self.pgport),
                "pgsslmode": self.pgsslmode
            }
            
            # Submit job using the jobs service
            result = self._submit_autoscaling_job(
                pgbench_config=pgbench_config,
                query_configs=query_configs,
                connection_params=connection_params,
                cluster_id=cluster_id
            )
            
            print(f"\n✅ Job submitted successfully!")
            print(f"   Job ID: {result.get('job_id', 'N/A')}")
            print(f"   Run ID: {result.get('run_id', 'N/A')}")
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
    
    def _submit_autoscaling_job(
        self,
        pgbench_config: Dict[str, Any],
        query_configs: list,
        connection_params: Dict[str, Any],
        cluster_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Submit job with Autoscaling-specific configuration"""
        
        print("\n   📝 Creating Autoscaling pgbench job...")
        print(f"   Using direct PostgreSQL connection to: {connection_params['pghost']}")
        
        # For Autoscaling, we use a workaround:
        # Pass connection string as environment variables in the notebook
        # The notebook will use psql connection string format instead of Databricks SDK
        
        # Build PostgreSQL connection string
        connection_string = (
            f"postgresql://{connection_params['pguser']}:{connection_params['pgpassword']}"
            f"@{connection_params['pghost']}:{connection_params['pgport']}"
            f"/{connection_params['pgdatabase']}?sslmode={connection_params['pgsslmode']}"
        )
        
        # Convert pgbench_config to match expected format
        # For Autoscaling, include connection credentials in the config
        pgbench_config_formatted = {
            "clients": pgbench_config["pgbench_clients"],
            "jobs": pgbench_config["pgbench_jobs"],
            "duration_seconds": pgbench_config["pgbench_duration"],
            "protocol": pgbench_config.get("pgbench_protocol", "prepared"),
            "progress_interval": pgbench_config.get("pgbench_progress_interval", 5),
            "per_statement_latency": pgbench_config.get("pgbench_per_statement_latency", True),
            "detailed_logging": pgbench_config.get("pgbench_detailed_logging", True),
            # Add Autoscaling connection parameters
            "pguser": connection_params['pguser'],
            "pgpassword": connection_params['pgpassword'],
            "pgport": connection_params['pgport'],
            "pgsslmode": connection_params['pgsslmode'],
        }
        
        # Use a special instance name format to signal Autoscaling mode to the notebook
        # The notebook will detect this and use connection string instead of SDK
        autoscaling_instance_name = f"autoscaling:{connection_params['pghost']}"
        
        try:
            # Submit using the standard jobs service, but with modified parameters
            result = self.jobs_service.submit_pgbench_job(
                lakebase_instance_name=autoscaling_instance_name,
                database_name=connection_params['pgdatabase'],
                cluster_id=cluster_id,
                pgbench_config=pgbench_config_formatted,
                query_configs=query_configs
            )
            
            # Add connection info to result
            result['connection_type'] = 'autoscaling'
            result['connection_string'] = f"postgresql://{connection_params['pguser']}@{connection_params['pghost']}/{connection_params['pgdatabase']}"
            
            return result
            
        except Exception as e:
            print(f"\n   ⚠️  Standard job submission failed: {e}")
            print(f"   This may be because the notebook needs Autoscaling-specific updates")
            print(f"\n   📋 Connection details for manual setup:")
            print(f"      Host: {connection_params['pghost']}")
            print(f"      Database: {connection_params['pgdatabase']}")
            print(f"      User: {connection_params['pguser']}")
            print(f"      Connection String: {connection_string}")
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
    print("🚀 Databricks Pgbench Job Deployment (Autoscaling Lakebase)")
    print("="*70)
    
    # Validate required environment variables for Autoscaling
    # Check for AUTOSCALING_* prefix first, then fall back to standard PG* variables
    PGHOST = os.getenv('AUTOSCALING_PGHOST') or os.getenv('PGHOST')
    if not PGHOST:
        print("\n❌ Error: AUTOSCALING_PGHOST environment variable is required")
        print("\nFor Autoscaling Lakebase, please set:")
        print("  export AUTOSCALING_PGHOST=ep-abc-123.databricks.com")
        print("  export AUTOSCALING_PGDATABASE=databricks_postgres")
        print("  export AUTOSCALING_PGUSER=your-username")
        print("  export AUTOSCALING_PGPASSWORD=your-password")
        print("\nOr load from .env file:")
        print("  export $(cat .env | grep -v '^#' | xargs)")
        print("  python3 deploy_autoscaling_pgbench_job.py")
        print("\nFor template, see: env_unified_template.txt")
        print("\n💡 For Provisioned Lakebase, use: deploy_provisioned_pgbench_job.py")
        print("\nNote: Also supports PGHOST for backward compatibility")
        sys.exit(1)
    
    PGUSER = os.getenv('AUTOSCALING_PGUSER') or os.getenv('PGUSER')
    if not PGUSER:
        print("\n❌ Error: AUTOSCALING_PGUSER environment variable is required")
        print("  export AUTOSCALING_PGUSER=your-username")
        print("\nNote: Also supports PGUSER for backward compatibility")
        sys.exit(1)
    
    PGPASSWORD = os.getenv('AUTOSCALING_PGPASSWORD') or os.getenv('PGPASSWORD')
    if not PGPASSWORD:
        print("\n❌ Error: AUTOSCALING_PGPASSWORD environment variable is required")
        print("  export AUTOSCALING_PGPASSWORD=your-password-or-token")
        print("\nNote: Also supports PGPASSWORD for backward compatibility")
        sys.exit(1)
    
    # Configuration with defaults
    PGDATABASE = os.getenv('AUTOSCALING_PGDATABASE') or os.getenv('PGDATABASE', 'databricks_postgres')
    PGPORT = int(os.getenv('AUTOSCALING_PGPORT') or os.getenv('PGPORT', '5432'))
    PGSSLMODE = os.getenv('AUTOSCALING_PGSSLMODE') or os.getenv('PGSSLMODE', 'require')
    CLUSTER_ID = os.getenv('DATABRICKS_CLUSTER_ID', None)
    PROFILE = os.getenv('DATABRICKS_PROFILE', 'DEFAULT')
    
    # Test parameters with defaults
    TEST_CLIENTS = int(os.getenv('PGBENCH_CLIENTS', '5'))
    TEST_JOBS = int(os.getenv('PGBENCH_JOBS', '4'))
    TEST_DURATION = int(os.getenv('PGBENCH_DURATION', '30'))
    
    print(f"\n📋 Test Configuration:")
    print(f"   Endpoint: {PGHOST}")
    print(f"   Database: {PGDATABASE}")
    print(f"   User: {PGUSER}")
    print(f"   Port: {PGPORT}")
    print(f"   SSL Mode: {PGSSLMODE}")
    print(f"   Profile: {PROFILE}")
    print(f"   Clients: {TEST_CLIENTS}")
    print(f"   Jobs: {TEST_JOBS}")
    print(f"   Duration: {TEST_DURATION}s")
    print(f"   Cluster: {'Auto (ephemeral job cluster)' if not CLUSTER_ID else CLUSTER_ID}")
    
    # Initialize deployer
    deployer = AutoscalingPgbenchDeployer(
        pghost=PGHOST,
        pgdatabase=PGDATABASE,
        pguser=PGUSER,
        pgpassword=PGPASSWORD,
        pgport=PGPORT,
        pgsslmode=PGSSLMODE
    )
    
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
            print("\n❌ Test failed or was interrupted")
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
