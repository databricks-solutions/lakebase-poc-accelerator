#!/usr/bin/env python3
"""
Deploy and run pgbench Job on Databricks (single script for Provisioned and Autoscaling Lakebase)

Uses PostgreSQL credentials (PGHOST, PGUSER, PGPASSWORD) for both instance types.
Either provide PGHOST directly (autoscaling or any endpoint) or LAKEBASE_INSTANCE_NAME
to resolve host from the Provisioned instance.

ENVIRONMENT VARIABLES (loaded from .env):
    Required:
        PGUSER     - PostgreSQL username
        PGPASSWORD - PostgreSQL password
    
    Host (one of):
        PGHOST                 - PostgreSQL host (use for autoscaling ep-*.databricks.com or any direct host)
        LAKEBASE_INSTANCE_NAME - Provisioned instance name (script resolves host via Databricks API)
    
    Optional:
        PGDATABASE             - Database name (default: databricks_postgres)
        PGPORT                 - Port (default: 5432)
        PGSSLMODE              - SSL mode (default: require)
        DATABRICKS_CLUSTER_ID   - Existing cluster ID (default: auto job cluster)
        DATABRICKS_PROFILE      - Databricks CLI profile (default: DEFAULT)
        PGBENCH_CLIENTS         - Concurrent clients (default: 5)
        PGBENCH_JOBS            - Worker threads (default: 4)
        PGBENCH_DURATION        - Test duration seconds (default: 30)

USAGE:
    # With direct host (autoscaling or provisioned)
    export PGHOST=ep-xxx.databricks.com   # or provisioned instance host
    export PGUSER=analyst
    export PGPASSWORD=your-password
    python3 deploy_pgbench_job.py

    # With provisioned instance name (host resolved from API)
    export LAKEBASE_INSTANCE_NAME=my-instance
    export PGUSER=analyst
    export PGPASSWORD=your-password
    python3 deploy_pgbench_job.py

    # Load from .env
    export $(cat .env | grep -v '^#' | grep -v '^$' | xargs)
    python3 deploy_pgbench_job.py
"""

import os
import sys
import time
from typing import Dict, Any, Optional

try:
    from dotenv import load_dotenv
    script_dir = os.path.dirname(os.path.abspath(__file__))
    for env_path in [
        os.path.join(script_dir, '.env'),
        os.path.abspath(os.path.join(script_dir, '..', '..', '..', '.env')),
    ]:
        if os.path.exists(env_path):
            load_dotenv(env_path)
            print(f"✅ Loaded environment from: {env_path}")
            break
except ImportError:
    pass

current_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.abspath(os.path.join(current_dir, '..'))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from services.databricks_jobs_service import DatabricksJobsService


class PgbenchJobDeployer:
    """Single deployer for pgbench jobs (Provisioned and Autoscaling via PostgreSQL credentials)."""

    def __init__(
        self,
        pghost: Optional[str] = None,
        instance_name: Optional[str] = None,
        pgdatabase: str = "databricks_postgres",
        pguser: str = "",
        pgpassword: str = "",
        pgport: int = 5432,
        pgsslmode: str = "require",
    ):
        self.pghost = pghost
        self.instance_name = instance_name
        self.pgdatabase = pgdatabase
        self.pguser = pguser
        self.pgpassword = pgpassword
        self.pgport = pgport
        self.pgsslmode = pgsslmode
        self.jobs_service: Optional[DatabricksJobsService] = None

    def initialize(self) -> bool:
        """Initialize Databricks client and resolve host if using instance name."""
        print("\n🔧 Initializing Databricks Jobs Service...")
        print("=" * 70)

        try:
            os.environ['DATABRICKS_APP_NAME'] = 'lakebase_app'
            self.jobs_service = DatabricksJobsService()
            w = self.jobs_service._get_client()
            user = w.current_user.me()
            print(f"✅ Connected to Databricks as: {user.user_name}")

            if self.pghost:
                print(f"✅ Using PostgreSQL host: {self.pghost}")
                if self.pghost.startswith('ep-'):
                    print("   (Autoscaling endpoint)")
                return True

            if self.instance_name:
                instance = w.database.get_database_instance(name=self.instance_name)
                self.pghost = getattr(instance, 'read_write_dns', None) or getattr(instance, 'host', None)
                if not self.pghost:
                    raise ValueError("Instance has no read_write_dns/host")
                print(f"✅ Resolved host for instance '{self.instance_name}': {self.pghost}")
                return True

            raise ValueError("Either PGHOST or LAKEBASE_INSTANCE_NAME must be set")

        except Exception as e:
            print(f"❌ Initialization failed: {e}")
            return False

    def create_sample_queries(self) -> list:
        """Default pgbench query set (name, content, weight)."""
        return [
            {"name": "point_lookup", "content": "SELECT 1 AS point_lookup;", "weight": 60},
            {"name": "range_scan", "content": "SELECT 1 + 1 AS range_result;", "weight": 30},
            {"name": "aggregation", "content": "SELECT COUNT(*) AS total FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3) t;", "weight": 10},
        ]

    def create_pgbench_config(
        self,
        clients: int = 5,
        jobs: int = 4,
        duration: int = 30,
        protocol: str = "prepared",
    ) -> Dict[str, Any]:
        return {
            "pgbench_clients": clients,
            "pgbench_jobs": jobs,
            "pgbench_duration": duration,
            "pgbench_progress_interval": 5,
            "pgbench_protocol": protocol,
            "pgbench_per_statement_latency": True,
            "pgbench_detailed_logging": True,
            "pgbench_connect_per_transaction": False,
        }

    def submit_job(
        self,
        pgbench_config: Dict[str, Any],
        query_configs: list,
        cluster_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Submit pgbench job to Databricks."""
        print("\n📤 Submitting pgbench job...")
        print("=" * 70)
        print(f"  Host: {self.pghost}")
        print(f"  Database: {self.pgdatabase}")
        print(f"  User: {self.pguser}")
        print(f"  Clients: {pgbench_config['pgbench_clients']}")
        print(f"  Jobs: {pgbench_config['pgbench_jobs']}")
        print(f"  Duration: {pgbench_config['pgbench_duration']}s")
        print(f"  Queries: {len(query_configs)}")
        print(f"  Cluster: {'Auto (job cluster)' if not cluster_id else cluster_id}")
        print("=" * 70)

        result = self.jobs_service.submit_pgbench_job(
            pghost=self.pghost,
            pgport=self.pgport,
            pgdatabase=self.pgdatabase,
            pguser=self.pguser,
            pgpassword=self.pgpassword,
            pgsslmode=self.pgsslmode,
            cluster_id=cluster_id,
            pgbench_config=pgbench_config,
            query_configs=query_configs,
        )

        print(f"\n✅ Job submitted successfully!")
        print(f"   Job ID: {result['job_id']}")
        print(f"   Run ID: {result['run_id']}")
        print(f"   Job Name: {result.get('job_name', 'N/A')}")
        if result.get('job_run_url'):
            print(f"\n🔗 Job Run URL:\n   {result['job_run_url']}")
        if result.get('job_url'):
            print(f"🔗 Job URL:\n   {result['job_url']}")
        return result

    def monitor_job(self, run_id: str, poll_interval: int = 10) -> Dict[str, Any]:
        """Poll job until completed or failed."""
        print(f"\n👀 Monitoring job run: {run_id}")
        print("=" * 70)
        start_time = time.time()
        last_status = None

        while True:
            try:
                status = self.jobs_service.get_run_status(run_id)
                if status['status'] != last_status:
                    elapsed = int(time.time() - start_time)
                    print(f"[{elapsed}s] Status: {status['status'].upper()} - {status['message']}")
                    last_status = status['status']

                if status['status'] in ['completed', 'failed']:
                    print("=" * 70)
                    if status['status'] == 'completed':
                        print("🎉 Job completed successfully!")
                        if status.get('pgbench_results'):
                            pgb = status['pgbench_results']
                            print(f"\n   pgbench Summary: TPS={pgb.get('tps', 'N/A')} Latency={pgb.get('latency_avg_ms', 'N/A')}ms")
                            if pgb.get('per_query_stats'):
                                for stat in pgb['per_query_stats']:
                                    print(f"   - {stat['query_name']}: {stat['tps']} TPS, {stat['latency_avg_ms']}ms")
                    else:
                        print("❌ Job failed!")
                        print(f"   Error: {status.get('message', 'Unknown error')}")
                    return status

                time.sleep(poll_interval)

            except KeyboardInterrupt:
                print("\n\n⚠️  Monitoring interrupted. Job still running in Databricks.")
                return status
            except Exception as e:
                print(f"❌ Error monitoring job: {e}")
                return status


def main() -> None:
    print("\n" + "=" * 70)
    print("🚀 Databricks Pgbench Job – Deploy & Run (Provisioned or Autoscaling)")
    print("=" * 70)

    pguser = os.getenv('PGUSER') or os.getenv('AUTOSCALING_PGUSER') or os.getenv('PROVISIONED_PGUSER')
    pgpassword = os.getenv('PGPASSWORD') or os.getenv('AUTOSCALING_PGPASSWORD') or os.getenv('PROVISIONED_PGPASSWORD')
    if not pguser or not pgpassword:
        print("\n❌ Error: PGUSER and PGPASSWORD are required")
        print("  export PGUSER=your-user")
        print("  export PGPASSWORD=your-password")
        sys.exit(1)

    pghost = os.getenv('PGHOST') or os.getenv('AUTOSCALING_PGHOST')
    instance_name = os.getenv('LAKEBASE_INSTANCE_NAME') or os.getenv('PROVISIONED_LAKEBASE_INSTANCE_NAME')
    if not pghost and not instance_name:
        print("\n❌ Error: Set either PGHOST (direct host) or LAKEBASE_INSTANCE_NAME (provisioned lookup)")
        print("  Autoscaling:  export PGHOST=ep-xxx.databricks.com")
        print("  Provisioned:  export LAKEBASE_INSTANCE_NAME=my-instance")
        sys.exit(1)

    pgdatabase = os.getenv('PGDATABASE') or os.getenv('LAKEBASE_DATABASE', 'databricks_postgres')
    pgport = int(os.getenv('PGPORT', '5432'))
    pgsslmode = os.getenv('PGSSLMODE', 'require')
    cluster_id = os.getenv('DATABRICKS_CLUSTER_ID')
    test_clients = int(os.getenv('PGBENCH_CLIENTS', '5'))
    test_jobs = int(os.getenv('PGBENCH_JOBS', '4'))
    test_duration = int(os.getenv('PGBENCH_DURATION', '30'))

    print(f"\n📋 Configuration:")
    print(f"   Host: {pghost or f'(from instance {instance_name})'}")
    print(f"   Database: {pgdatabase}")
    print(f"   User: {pguser}")
    print(f"   Clients: {test_clients}  Jobs: {test_jobs}  Duration: {test_duration}s")

    deployer = PgbenchJobDeployer(
        pghost=pghost or None,
        instance_name=instance_name or None,
        pgdatabase=pgdatabase,
        pguser=pguser,
        pgpassword=pgpassword,
        pgport=pgport,
        pgsslmode=pgsslmode,
    )

    if not deployer.initialize():
        print("\n❌ Initialization failed. Exiting.")
        sys.exit(1)

    pgbench_config = deployer.create_pgbench_config(clients=test_clients, jobs=test_jobs, duration=test_duration)
    query_configs = deployer.create_sample_queries()

    try:
        result = deployer.submit_job(pgbench_config=pgbench_config, query_configs=query_configs, cluster_id=cluster_id)
        final_status = deployer.monitor_job(result['run_id'])
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
