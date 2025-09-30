import asyncio
import logging
import os
import re
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

import numpy as np
from databricks.sdk import WorkspaceClient

from models.query_models import PgbenchTestReport, PgbenchExecutionResult
from services.oauth_service import DatabricksOAuthService


logger = logging.getLogger(__name__)


class PgbenchService:
    """
    Service for running pgbench-based concurrency tests against Lakebase instances.
    Replaces SQLAlchemy-based testing with native PostgreSQL pgbench tool.
    """

    def __init__(self):
        self._oauth_service = DatabricksOAuthService()
        self._workspace_client: Optional[WorkspaceClient] = None
        self._database_instance = None
        self._postgres_password: Optional[str] = None
        self._last_password_refresh: float = 0.0

    async def initialize_connection(
        self,
        workspace_url: str,
        instance_name: str,
        database: str,
        profile: str = 'DEFAULT'
    ) -> bool:
        """
        Initialize connection to Databricks Lakebase instance.

        Args:
            workspace_url: Databricks workspace URL
            instance_name: Lakebase instance name
            database: Target database name
            profile: Databricks CLI profile name

        Returns:
            bool: True if initialization successful
        """
        try:
            # Initialize Databricks SDK client
            self._workspace_client = WorkspaceClient(profile=profile) if profile != 'DEFAULT' else WorkspaceClient()

            # Lookup database instance by name
            self._database_instance = self._workspace_client.database.get_database_instance(name=instance_name)

            # Generate OAuth credential (Postgres token)
            cred = self._workspace_client.database.generate_database_credential(
                request_id=str(uuid.uuid4()),
                instance_names=[self._database_instance.name],
            )
            self._postgres_password = cred.token
            self._last_password_refresh = time.time()

            self._database = database

            logger.info(f"Pgbench service initialized for instance: {instance_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize pgbench service: {e}")
            raise e

    async def execute_pgbench_test(
        self,
        queries: List[Dict[str, Any]],
        pgbench_config: Dict[str, Any]
    ) -> PgbenchTestReport:
        """
        Execute pgbench test with uploaded queries and configuration.

        Args:
            queries: List of query configurations with content and weights
            pgbench_config: pgbench command-line options

        Returns:
            PgbenchTestReport with execution results and metrics
        """
        if not self._workspace_client or not self._database_instance:
            raise Exception("Pgbench service not initialized")

        test_id = f"pgbench_test_{int(time.time())}"
        test_start_time = time.time()

        try:
            # Create temporary directory for pgbench scripts
            with tempfile.TemporaryDirectory(prefix="pgbench_") as workdir:
                workdir_path = Path(workdir)

                # Write query scripts to files
                script_files = await self._write_query_scripts(queries, workdir_path)

                # Build pgbench command
                cmd = self._build_pgbench_command(pgbench_config, script_files, workdir)

                # Set up environment variables
                env = self._build_environment()

                # Execute pgbench
                logger.info(f"Executing pgbench command: {' '.join(cmd)}")
                result = await self._execute_pgbench_subprocess(cmd, env, workdir)

                test_end_time = time.time()

                # Parse pgbench output and logs
                parsed_results = self._parse_pgbench_output(result, workdir_path)

                # Generate comprehensive report
                report = self._generate_pgbench_report(
                    test_id=test_id,
                    test_start_time=test_start_time,
                    test_end_time=test_end_time,
                    pgbench_config=pgbench_config,
                    raw_output=result,
                    parsed_results=parsed_results,
                    queries=queries
                )

                return report

        except Exception as e:
            logger.error(f"Pgbench test execution failed: {e}")
            raise Exception(f"Pgbench test failed: {e}")

    async def _write_query_scripts(
        self,
        queries: List[Dict[str, Any]],
        workdir: Path
    ) -> List[Tuple[str, int]]:
        """
        Write query scripts to files with proper pgbench format.

        Returns:
            List of (script_path, weight) tuples
        """
        script_files = []

        for i, query_config in enumerate(queries):
            script_name = f"query_{i}.sql"
            script_path = workdir / script_name

            # Get query content and weight
            query_content = query_config.get('query_content', '')
            weight = query_config.get('weight', 1)

            # Write script file
            with open(script_path, 'w') as f:
                f.write(query_content.strip() + '\n')

            script_files.append((str(script_path), weight))
            logger.info(f"Created pgbench script: {script_path} (weight: {weight})")

        return script_files

    def _build_pgbench_command(
        self,
        config: Dict[str, Any],
        script_files: List[Tuple[str, int]],
        workdir: str
    ) -> List[str]:
        """
        Build pgbench command with user-specified options and weighted scripts.
        """
        cmd = ["pgbench"]

        # Core pgbench options
        cmd.extend(["-n"])  # No vacuum

        # Client and job configuration
        clients = config.get('clients', 8)
        jobs = config.get('jobs', min(clients, 8))
        cmd.extend(["-c", str(clients)])
        cmd.extend(["-j", str(jobs)])

        # Time or transaction configuration
        if config.get('duration_seconds'):
            cmd.extend(["-T", str(config['duration_seconds'])])
        elif config.get('transactions_per_client'):
            cmd.extend(["-t", str(config['transactions_per_client'])])
        else:
            cmd.extend(["-T", "30"])  # Default 30 seconds

        # Progress reporting
        if config.get('progress_interval'):
            cmd.extend(["-P", str(config['progress_interval'])])

        # Protocol mode
        protocol = config.get('protocol', 'prepared')
        if protocol in ['simple', 'extended', 'prepared']:
            cmd.extend(["-M", protocol])

        # Rate limiting
        if config.get('target_tps'):
            cmd.extend(["-R", str(config['target_tps'])])

        # Reporting options
        if config.get('per_statement_latency', True):
            cmd.extend(["-r"])

        if config.get('detailed_logging', True):
            cmd.extend(["-l"])

        # Connection options
        if config.get('connect_per_transaction', False):
            cmd.extend(["-C"])

        # Add script files with weights
        for script_path, weight in script_files:
            for _ in range(weight):
                cmd.extend(["-f", script_path])

        return cmd

    def _build_environment(self) -> Dict[str, str]:
        """Build environment variables for pgbench execution."""
        if not self._workspace_client or not self._database_instance:
            raise Exception("Service not initialized")

        env = os.environ.copy()
        env.update({
            "PGHOST": self._database_instance.read_write_dns,
            "PGPORT": "5432",
            "PGDATABASE": self._database,
            "PGUSER": self._workspace_client.current_user.me().user_name,
            "PGPASSWORD": self._postgres_password,
            "PGSSLMODE": "require",
        })

        return env

    async def _execute_pgbench_subprocess(
        self,
        cmd: List[str],
        env: Dict[str, str],
        workdir: str
    ) -> subprocess.CompletedProcess:
        """Execute pgbench subprocess asynchronously."""
        try:
            # Run pgbench in subprocess
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
                cwd=workdir
            )

            stdout, stderr = await process.communicate()

            result = subprocess.CompletedProcess(
                args=cmd,
                returncode=process.returncode,
                stdout=stdout.decode('utf-8'),
                stderr=stderr.decode('utf-8')
            )

            if result.returncode != 0:
                logger.error(f"Pgbench failed with exit code {result.returncode}")
                logger.error(f"STDERR: {result.stderr}")
                raise Exception(f"Pgbench execution failed: {result.stderr}")

            return result

        except Exception as e:
            logger.error(f"Failed to execute pgbench subprocess: {e}")
            raise e

    def _parse_pgbench_output(
        self,
        result: subprocess.CompletedProcess,
        workdir: Path
    ) -> Dict[str, Any]:
        """Parse pgbench output and log files for metrics."""
        parsed = {
            'tps': None,
            'latencies': [],
            'percentiles': {},
            'progress_reports': [],
            'per_statement_stats': {},
        }

        # Parse TPS from stdout
        tps_match = re.search(r"tps\s*=\s*([\d\.]+)", result.stdout)
        if tps_match:
            parsed['tps'] = float(tps_match.group(1))

        # Parse latency distribution from stdout
        latency_match = re.search(r"latency average = ([\d\.]+) ms", result.stdout)
        if latency_match:
            parsed['average_latency'] = float(latency_match.group(1))

        # Parse percentiles from stdout
        percentile_pattern = r"latency stddev = ([\d\.]+) ms"
        stddev_match = re.search(percentile_pattern, result.stdout)
        if stddev_match:
            parsed['latency_stddev'] = float(stddev_match.group(1))

        # Try to parse percentiles directly from pgbench output
        # pgbench sometimes includes percentiles in stdout
        logger.info(f"Parsing percentiles from pgbench output")
        p95_match = re.search(r"95th percentile: ([\d\.]+) ms", result.stdout)
        p99_match = re.search(r"99th percentile: ([\d\.]+) ms", result.stdout)

        if p95_match or p99_match:
            parsed['percentiles'] = {}
            if p95_match:
                parsed['percentiles']['p95'] = float(p95_match.group(1))
            if p99_match:
                parsed['percentiles']['p99'] = float(p99_match.group(1))

        # Parse per-statement statistics if available
        if "-r" in result.args:
            parsed['per_statement_stats'] = self._parse_per_statement_stats(result.stdout)

        # Parse detailed transaction logs if available
        if "-l" in result.args:
            parsed['latencies'] = self._parse_transaction_logs(workdir)
            if parsed['latencies']:
                percentiles_from_logs = self._calculate_percentiles(parsed['latencies'])
                if percentiles_from_logs:
                    parsed['percentiles'] = percentiles_from_logs

        # If we don't have percentiles yet, try to estimate them from average and stddev
        if 'percentiles' not in parsed and parsed.get('average_latency') and parsed.get('latency_stddev'):
            logger.info(f"Calculating percentiles from avg={parsed['average_latency']}ms, stddev={parsed['latency_stddev']}ms")
            avg_lat = parsed['average_latency']
            stddev_lat = parsed['latency_stddev']
            # Rough estimation: assume normal distribution
            # P95 ≈ mean + 1.645 * stddev, P99 ≈ mean + 2.326 * stddev
            parsed['percentiles'] = {
                'p95': avg_lat + (1.645 * stddev_lat),
                'p99': avg_lat + (2.326 * stddev_lat)
            }
            logger.info(f"Estimated percentiles from avg={avg_lat}ms, stddev={stddev_lat}ms")

        # Parse progress reports
        parsed['progress_reports'] = self._parse_progress_reports(result.stdout)

        return parsed

    def _parse_per_statement_stats(self, stdout: str) -> Dict[str, Dict[str, float]]:
        """Parse per-statement latency statistics from pgbench output."""
        stats = {}

        # Look for statement statistics in output
        lines = stdout.split('\n')
        in_statement_section = False

        for line in lines:
            if "statement latencies in milliseconds:" in line:
                in_statement_section = True
                continue

            if in_statement_section and line.strip():
                # Parse lines like: "0.123    statement1"
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        latency = float(parts[0])
                        statement = ' '.join(parts[1:])
                        stats[statement] = {'average_latency_ms': latency}
                    except ValueError:
                        continue

        return stats

    def _parse_transaction_logs(self, workdir: Path) -> List[float]:
        """Parse transaction latencies from pgbench log files."""
        latencies = []

        # Find all pgbench log files
        log_files = list(workdir.glob("pgbench_log.*"))
        logger.info(f"Found {len(log_files)} pgbench log files in {workdir}")

        for log_file in log_files:
            try:
                with open(log_file, 'r') as f:
                    line_count = 0
                    for line in f:
                        line_count += 1
                        parts = line.strip().split()
                        if len(parts) >= 3:
                            try:
                                # pgbench log format: client_id transaction_no time script_no time_epoch time_us
                                # We want the time_us (latency in microseconds) which is the last column
                                latency_us = float(parts[-1])
                                latency_ms = latency_us / 1000.0
                                latencies.append(latency_ms)
                            except (ValueError, IndexError):
                                continue
                    logger.info(f"Parsed {len(latencies)} latencies from {log_file} ({line_count} lines)")
            except Exception as e:
                logger.warning(f"Failed to parse log file {log_file}: {e}")

        logger.info(f"Total latencies parsed: {len(latencies)}")
        return latencies

    def _calculate_percentiles(self, latencies: List[float]) -> Dict[str, float]:
        """Calculate latency percentiles."""
        if not latencies:
            logger.warning("No latencies available for percentile calculation")
            return {}

        percentiles = [50, 95, 99, 99.9]
        results = {}

        try:
            calculated = np.percentile(latencies, percentiles)
            for p, value in zip(percentiles, calculated):
                results[f'p{p}'] = float(value)
            logger.info(f"Calculated percentiles from {len(latencies)} latency samples: {results}")
        except Exception as e:
            logger.warning(f"Failed to calculate percentiles: {e}")

        return results

    def _parse_progress_reports(self, stdout: str) -> List[Dict[str, Any]]:
        """Parse progress reports from pgbench output."""
        reports = []

        lines = stdout.split('\n')
        for line in lines:
            # Parse lines like: "progress: 5.0 s, 1234.5 tps, lat 0.123 ms stddev 0.456"
            if line.startswith("progress:"):
                try:
                    parts = line.split(', ')
                    if len(parts) >= 3:
                        # Extract time
                        time_part = parts[0].replace("progress:", "").strip()
                        time_match = re.search(r"([\d\.]+) s", time_part)

                        # Extract TPS
                        tps_match = re.search(r"([\d\.]+) tps", parts[1])

                        # Extract latency
                        lat_match = re.search(r"lat ([\d\.]+) ms", parts[2])

                        if time_match and tps_match:
                            report = {
                                'time_seconds': float(time_match.group(1)),
                                'tps': float(tps_match.group(1)),
                            }

                            if lat_match:
                                report['latency_ms'] = float(lat_match.group(1))

                            reports.append(report)
                except Exception as e:
                    logger.warning(f"Failed to parse progress line: {line}, error: {e}")

        return reports

    def _generate_pgbench_report(
        self,
        test_id: str,
        test_start_time: float,
        test_end_time: float,
        pgbench_config: Dict[str, Any],
        raw_output: subprocess.CompletedProcess,
        parsed_results: Dict[str, Any],
        queries: List[Dict[str, Any]]
    ) -> PgbenchTestReport:
        """Generate comprehensive pgbench test report."""

        total_duration = test_end_time - test_start_time

        # Build execution result
        execution_result = PgbenchExecutionResult(
            return_code=raw_output.returncode,
            stdout=raw_output.stdout,
            stderr=raw_output.stderr,
            execution_time_seconds=total_duration
        )

        # Generate recommendations
        recommendations = self._generate_recommendations(parsed_results, pgbench_config)

        return PgbenchTestReport(
            test_id=test_id,
            test_start_time=test_start_time,
            test_end_time=test_end_time,
            total_duration_seconds=total_duration,
            pgbench_config=pgbench_config,
            queries_tested=len(queries),
            tps=parsed_results.get('tps'),
            average_latency_ms=parsed_results.get('average_latency'),
            latency_stddev_ms=parsed_results.get('latency_stddev'),
            latency_percentiles=parsed_results.get('percentiles', {}),
            per_statement_stats=parsed_results.get('per_statement_stats', {}),
            progress_reports=parsed_results.get('progress_reports', []),
            execution_result=execution_result,
            recommendations=recommendations
        )

    def _generate_recommendations(
        self,
        parsed_results: Dict[str, Any],
        config: Dict[str, Any]
    ) -> List[str]:
        """Generate performance recommendations based on pgbench results."""
        recommendations = []

        tps = parsed_results.get('tps', 0)
        avg_latency = parsed_results.get('average_latency', 0)
        clients = config.get('clients', 1)

        if tps and tps < 100:
            recommendations.append("Low TPS detected. Consider optimizing queries or increasing client connections.")

        if avg_latency and avg_latency > 100:
            recommendations.append("High average latency. Check for inefficient queries or resource constraints.")

        if clients > 50 and tps and tps / clients < 2:
            recommendations.append("High client count with low per-client TPS. Consider reducing client connections.")

        percentiles = parsed_results.get('percentiles', {})
        if percentiles.get('p95') and percentiles.get('p50'):
            if percentiles['p95'] > percentiles['p50'] * 5:
                recommendations.append("High latency variance detected. Check for inconsistent query performance.")

        if not recommendations:
            recommendations.append("Performance looks good! Consider running longer tests for more accurate metrics.")

        return recommendations

    def refresh_credentials(self):
        """Refresh OAuth credentials for continued access."""
        try:
            if not self._workspace_client or not self._database_instance:
                raise Exception("Service not initialized")

            cred = self._workspace_client.database.generate_database_credential(
                request_id=str(uuid.uuid4()),
                instance_names=[self._database_instance.name],
            )
            self._postgres_password = cred.token
            self._last_password_refresh = time.time()
            logger.info("OAuth credentials refreshed successfully")

        except Exception as e:
            logger.error(f"Failed to refresh credentials: {e}")
            raise e