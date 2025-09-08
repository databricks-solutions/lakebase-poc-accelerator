#!/usr/bin/env python3
"""
Lakebase Postgres Concurrency Testing Framework

This script executes concurrency tests against Postgres databases based on
configuration files. It simulates various workload patterns and collects
detailed performance metrics.

Usage:
    python run_concurrency_test.py --config concurrency_config.yaml --scenario oltp_light
    python run_concurrency_test.py --config concurrency_config.yaml --all-scenarios
    python run_concurrency_test.py --config concurrency_config.yaml --list-scenarios

Requirements:
    - psycopg2-binary or asyncpg
    - pyyaml
    - pandas (for report generation)
    - matplotlib/plotly (for charts)
"""

import argparse
import asyncio
import logging
import os
import sys
import time
import threading
import json
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import statistics
from collections import defaultdict, deque
import queue

# Required imports
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False
    print("Error: pyyaml required. Install with: pip install pyyaml")

try:
    import psycopg2
    from psycopg2 import pool, sql
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    print("Warning: psycopg2 not available.")

try:
    import asyncpg
    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False
    print("Warning: asyncpg not available.")

if not PSYCOPG2_AVAILABLE and not ASYNCPG_AVAILABLE:
    print("Error: Either psycopg2 or asyncpg required for Postgres connectivity")
    sys.exit(1)

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("Warning: pandas not available. Report generation limited.")

try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    print("Warning: matplotlib not available. Chart generation disabled.")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    """Single query execution result."""
    query_name: str
    start_time: float
    end_time: float
    duration_ms: float
    success: bool
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    row_count: Optional[int] = None
    connection_id: Optional[str] = None
    worker_id: Optional[str] = None


@dataclass
class TestMetrics:
    """Aggregated test metrics."""
    scenario_name: str
    start_time: datetime
    end_time: datetime
    total_duration: float
    total_queries: int
    successful_queries: int
    failed_queries: int
    avg_response_time: float
    p50_response_time: float
    p95_response_time: float
    p99_response_time: float
    min_response_time: float
    max_response_time: float
    throughput_qps: float
    error_rate: float
    concurrent_connections: int
    errors_by_type: Dict[str, int]


class ParameterGenerator:
    """Generates test parameters for queries."""
    
    def __init__(self):
        self.random = random.Random()
        self.random.seed(42)  # For reproducible tests
    
    def generate_parameter(self, param_config: Dict[str, Any]) -> Any:
        """Generate a single parameter value based on configuration."""
        generation_type = param_config.get('generation', 'static')
        param_type = param_config.get('type', 'string')
        
        if generation_type == 'static':
            return param_config.get('value')
        
        elif generation_type == 'random_range':
            min_val, max_val = param_config['range']
            if param_type == 'integer':
                return self.random.randint(min_val, max_val)
            elif param_type == 'decimal':
                return round(self.random.uniform(min_val, max_val), 2)
            
        elif generation_type == 'random_choice':
            return self.random.choice(param_config['choices'])
        
        elif generation_type == 'random_date_range':
            start_date = datetime.fromisoformat(param_config['start_date'])
            end_date = datetime.fromisoformat(param_config['end_date'])
            time_between = end_date - start_date
            days_between = time_between.days
            random_days = self.random.randrange(days_between)
            return (start_date + timedelta(days=random_days)).date()
        
        elif generation_type == 'random_array':
            array_size = self.random.randint(*param_config['array_size'])
            element_range = param_config['element_range']
            return [self.random.randint(*element_range) for _ in range(array_size)]
        
        else:
            raise ValueError(f"Unknown parameter generation type: {generation_type}")
    
    def generate_parameters(self, query_config: Dict[str, Any]) -> Dict[str, Any]:
        """Generate all parameters for a query."""
        parameters = {}
        param_configs = query_config.get('parameters', [])
        
        for param_config in param_configs:
            param_name = param_config['name']
            parameters[param_name] = self.generate_parameter(param_config)
        
        return parameters


class ConnectionManager:
    """Manages Postgres connections and connection pooling."""
    
    def __init__(self, connection_config: Dict[str, Any]):
        self.config = connection_config
        self.pool = None
        self.async_pool = None
        self.use_async = ASYNCPG_AVAILABLE
        
    def initialize_pool(self):
        """Initialize connection pool."""
        if self.use_async and ASYNCPG_AVAILABLE:
            # AsyncPG pool will be created per async context
            pass
        elif PSYCOPG2_AVAILABLE:
            try:
                self.pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=self.config.get('initial_pool_size', 5),
                    maxconn=self.config.get('max_pool_size', 50),
                    host=self.config['host'],
                    port=self.config['port'],
                    database=self.config['database'],
                    user=self.config['username'],
                    password=self.config['password'],
                    sslmode=self.config.get('ssl_mode', 'prefer'),
                    connect_timeout=self.config.get('connection_timeout', 30)
                )
                logger.info(f"Initialized psycopg2 connection pool (size: {self.config.get('max_pool_size', 50)})")
            except Exception as e:
                logger.error(f"Failed to initialize connection pool: {e}")
                raise
        else:
            raise RuntimeError("No Postgres adapter available")
    
    def get_connection(self):
        """Get a connection from the pool."""
        if self.pool:
            return self.pool.getconn()
        else:
            raise RuntimeError("Connection pool not initialized")
    
    def return_connection(self, conn):
        """Return a connection to the pool."""
        if self.pool:
            self.pool.putconn(conn)
    
    def close_pool(self):
        """Close the connection pool."""
        if self.pool:
            self.pool.closeall()
            logger.info("Connection pool closed")


class QueryExecutor:
    """Executes queries with timing and error handling."""
    
    def __init__(self, connection_manager: ConnectionManager, query_configs: Dict[str, Any]):
        self.connection_manager = connection_manager
        self.query_configs = query_configs
        self.parameter_generator = ParameterGenerator()
        
        # Load query SQL from files
        self.query_sql = {}
        self._load_query_files()
    
    def _load_query_files(self):
        """Load SQL query files."""
        for query_name, config in self.query_configs.items():
            file_path = config.get('file_path')
            if file_path and os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                    # Extract actual SQL (skip comments and metadata)
                    self.query_sql[query_name] = self._extract_sql(sql_content)
            else:
                logger.warning(f"Query file not found for {query_name}: {file_path}")
                self.query_sql[query_name] = f"SELECT 1 as placeholder_{query_name};"
    
    def _extract_sql(self, content: str) -> str:
        """Extract SQL from file content (skip conversion metadata)."""
        lines = content.split('\\n')
        sql_lines = []
        in_sql = False
        
        for line in lines:
            line = line.strip()
            # Start capturing from converted SQL section
            if '-- CONVERTED POSTGRESQL QUERY:' in line:
                in_sql = True
                continue
            elif in_sql and line and not line.startswith('/*'):
                sql_lines.append(line)
        
        if sql_lines:
            return '\\n'.join(sql_lines)
        else:
            # Fallback: use the entire content
            return content
    
    def execute_query(self, query_name: str, worker_id: str) -> QueryResult:
        """Execute a single query and return results."""
        start_time = time.time()
        
        try:
            # Generate parameters
            parameters = self.parameter_generator.generate_parameters(
                self.query_configs.get(query_name, {})
            )
            
            # Get query SQL
            query_sql = self.query_sql.get(query_name, "SELECT 1;")
            
            # Replace parameters in SQL (simple string replacement)
            final_sql = self._substitute_parameters(query_sql, parameters)
            
            # Execute query
            conn = self.connection_manager.get_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(final_sql)
                    row_count = cursor.rowcount
                    
                    # Fetch results to ensure query completion
                    if cursor.description:
                        cursor.fetchall()
                
                end_time = time.time()
                duration_ms = (end_time - start_time) * 1000
                
                return QueryResult(
                    query_name=query_name,
                    start_time=start_time,
                    end_time=end_time,
                    duration_ms=duration_ms,
                    success=True,
                    row_count=row_count,
                    worker_id=worker_id
                )
                
            finally:
                self.connection_manager.return_connection(conn)
                
        except Exception as e:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000
            
            error_type = type(e).__name__
            error_message = str(e)
            
            logger.debug(f"Query {query_name} failed: {error_message}")
            
            return QueryResult(
                query_name=query_name,
                start_time=start_time,
                end_time=end_time,
                duration_ms=duration_ms,
                success=False,
                error_type=error_type,
                error_message=error_message,
                worker_id=worker_id
            )
    
    def _substitute_parameters(self, sql: str, parameters: Dict[str, Any]) -> str:
        """Simple parameter substitution for demo purposes."""
        # In production, use proper parameterized queries
        for param_name, param_value in parameters.items():
            placeholder = f"${{{param_name}}}"
            if isinstance(param_value, str):
                sql = sql.replace(placeholder, f"'{param_value}'")
            elif isinstance(param_value, list):
                value_str = ','.join([str(v) for v in param_value])
                sql = sql.replace(placeholder, f"({value_str})")
            else:
                sql = sql.replace(placeholder, str(param_value))
        
        return sql


class WorkloadWorker:
    """Individual worker thread that executes queries according to pattern."""
    
    def __init__(self, worker_id: str, query_executor: QueryExecutor, 
                 scenario_config: Dict[str, Any], results_queue: queue.Queue):
        self.worker_id = worker_id
        self.query_executor = query_executor
        self.scenario_config = scenario_config
        self.results_queue = results_queue
        self.running = False
        self.query_patterns = scenario_config.get('query_patterns', [])
        
        # Calculate cumulative weights for query selection
        self.cumulative_weights = []
        total_weight = 0
        for pattern in self.query_patterns:
            total_weight += pattern.get('weight', 1)
            self.cumulative_weights.append(total_weight)
        self.total_weight = total_weight
    
    def select_query(self) -> Tuple[str, Dict[str, Any]]:
        """Select a query based on weighted distribution."""
        if not self.query_patterns:
            return "default", {}
        
        rand_value = random.random() * self.total_weight
        
        for i, cumulative_weight in enumerate(self.cumulative_weights):
            if rand_value <= cumulative_weight:
                pattern = self.query_patterns[i]
                return pattern['query_name'], pattern
        
        # Fallback to first pattern
        pattern = self.query_patterns[0]
        return pattern['query_name'], pattern
    
    def run(self, duration: float):
        """Run worker for specified duration."""
        self.running = True
        end_time = time.time() + duration
        
        logger.debug(f"Worker {self.worker_id} starting")
        
        while self.running and time.time() < end_time:
            try:
                # Select query to execute
                query_name, query_pattern = self.select_query()
                
                # Execute query
                result = self.query_executor.execute_query(query_name, self.worker_id)
                self.results_queue.put(result)
                
                # Apply think time
                think_time = query_pattern.get('think_time', 0.1)
                if think_time > 0:
                    time.sleep(think_time)
                
            except Exception as e:
                logger.error(f"Worker {self.worker_id} error: {e}")
                break
        
        self.running = False
        logger.debug(f"Worker {self.worker_id} completed")
    
    def stop(self):
        """Stop the worker."""
        self.running = False


class MetricsCollector:
    """Collects and aggregates performance metrics during testing."""
    
    def __init__(self, scenario_name: str):
        self.scenario_name = scenario_name
        self.results = []
        self.start_time = None
        self.end_time = None
        self.results_queue = queue.Queue()
        self.collecting = False
        
    def start_collection(self):
        """Start metrics collection."""
        self.start_time = datetime.now()
        self.collecting = True
        logger.info("Metrics collection started")
    
    def stop_collection(self):
        """Stop metrics collection."""
        self.end_time = datetime.now()
        self.collecting = False
        
        # Collect any remaining results
        while not self.results_queue.empty():
            try:
                result = self.results_queue.get_nowait()
                self.results.append(result)
            except queue.Empty:
                break
        
        logger.info(f"Metrics collection stopped. Collected {len(self.results)} results")
    
    def collect_result(self, result: QueryResult):
        """Collect a single query result."""
        if self.collecting:
            self.results.append(result)
    
    def get_results_queue(self) -> queue.Queue:
        """Get the results queue for workers."""
        return self.results_queue
    
    def calculate_metrics(self) -> TestMetrics:
        """Calculate aggregated metrics from collected results."""
        if not self.results:
            logger.warning("No results collected for metrics calculation")
            return self._empty_metrics()
        
        # Filter successful queries for performance metrics
        successful_results = [r for r in self.results if r.success]
        failed_results = [r for r in self.results if not r.success]
        
        if not successful_results:
            logger.warning("No successful queries for performance metrics")
            return self._empty_metrics()
        
        # Calculate response time metrics
        response_times = [r.duration_ms for r in successful_results]
        response_times.sort()
        
        # Calculate percentiles
        def percentile(data, p):
            if not data:
                return 0
            k = (len(data) - 1) * p
            f = int(k)
            c = k - f
            if f + 1 < len(data):
                return data[f] + c * (data[f + 1] - data[f])
            else:
                return data[f]
        
        # Calculate throughput
        total_duration = (self.end_time - self.start_time).total_seconds()
        throughput_qps = len(successful_results) / total_duration if total_duration > 0 else 0
        
        # Calculate error rates by type
        errors_by_type = defaultdict(int)
        for result in failed_results:
            error_type = result.error_type or 'unknown'
            errors_by_type[error_type] += 1
        
        return TestMetrics(
            scenario_name=self.scenario_name,
            start_time=self.start_time,
            end_time=self.end_time,
            total_duration=total_duration,
            total_queries=len(self.results),
            successful_queries=len(successful_results),
            failed_queries=len(failed_results),
            avg_response_time=statistics.mean(response_times),
            p50_response_time=percentile(response_times, 0.50),
            p95_response_time=percentile(response_times, 0.95),
            p99_response_time=percentile(response_times, 0.99),
            min_response_time=min(response_times),
            max_response_time=max(response_times),
            throughput_qps=throughput_qps,
            error_rate=(len(failed_results) / len(self.results)) * 100,
            concurrent_connections=0,  # Will be set by test runner
            errors_by_type=dict(errors_by_type)
        )
    
    def _empty_metrics(self) -> TestMetrics:
        """Return empty metrics structure."""
        return TestMetrics(
            scenario_name=self.scenario_name,
            start_time=self.start_time or datetime.now(),
            end_time=self.end_time or datetime.now(),
            total_duration=0,
            total_queries=0,
            successful_queries=0,
            failed_queries=0,
            avg_response_time=0,
            p50_response_time=0,
            p95_response_time=0,
            p99_response_time=0,
            min_response_time=0,
            max_response_time=0,
            throughput_qps=0,
            error_rate=0,
            concurrent_connections=0,
            errors_by_type={}
        )


class ConcurrencyTestRunner:
    """Main test runner that orchestrates concurrency tests."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection_manager = ConnectionManager(config['postgres_connection'])
        self.query_executor = QueryExecutor(
            self.connection_manager, 
            config.get('queries', {})
        )
        self.metrics_collector = None
        self.workers = []
        
    def run_scenario(self, scenario_name: str) -> TestMetrics:
        """Run a single test scenario."""
        scenario_config = self._get_scenario_config(scenario_name)
        if not scenario_config:
            raise ValueError(f"Scenario not found: {scenario_name}")
        
        logger.info(f"Starting scenario: {scenario_name}")
        logger.info(f"Description: {scenario_config.get('description', 'No description')}")
        
        # Initialize components
        self.connection_manager.initialize_pool()
        self.metrics_collector = MetricsCollector(scenario_name)
        
        try:
            # Execute the test
            metrics = self._execute_scenario(scenario_config)
            metrics.concurrent_connections = scenario_config.get('concurrent_connections', 1)
            
            # Check performance targets
            self._check_performance_targets(metrics, scenario_config)
            
            return metrics
            
        finally:
            self.connection_manager.close_pool()
    
    def _get_scenario_config(self, scenario_name: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific scenario."""
        scenarios = self.config.get('test_scenarios', [])
        for scenario in scenarios:
            if scenario.get('name') == scenario_name:
                return scenario
        return None
    
    def _execute_scenario(self, scenario_config: Dict[str, Any]) -> TestMetrics:
        """Execute a test scenario with proper ramp-up/down."""
        duration = scenario_config.get('duration', 300)
        concurrent_connections = scenario_config.get('concurrent_connections', 10)
        ramp_up_duration = scenario_config.get('ramp_up_duration', 30)
        ramp_down_duration = scenario_config.get('ramp_down_duration', 15)
        
        # Start metrics collection
        self.metrics_collector.start_collection()
        
        try:
            # Ramp up phase
            if ramp_up_duration > 0:
                logger.info(f"Ramp-up phase: {ramp_up_duration}s")
                self._ramp_up_workers(scenario_config, concurrent_connections, ramp_up_duration)
            else:
                self._start_all_workers(scenario_config, concurrent_connections)
            
            # Steady state phase
            steady_duration = duration - ramp_up_duration - ramp_down_duration
            if steady_duration > 0:
                logger.info(f"Steady state phase: {steady_duration}s")
                
                # Collect results from queue during execution
                self._monitor_execution(steady_duration)
            
            # Ramp down phase
            if ramp_down_duration > 0:
                logger.info(f"Ramp-down phase: {ramp_down_duration}s")
                self._ramp_down_workers(ramp_down_duration)
            else:
                self._stop_all_workers()
            
        finally:
            # Ensure all workers are stopped
            self._stop_all_workers()
            
            # Stop metrics collection
            self.metrics_collector.stop_collection()
        
        return self.metrics_collector.calculate_metrics()
    
    def _start_all_workers(self, scenario_config: Dict[str, Any], worker_count: int):
        """Start all workers immediately."""
        results_queue = self.metrics_collector.get_results_queue()
        
        for i in range(worker_count):
            worker = WorkloadWorker(
                worker_id=f"worker_{i}",
                query_executor=self.query_executor,
                scenario_config=scenario_config,
                results_queue=results_queue
            )
            self.workers.append(worker)
        
        # Start all workers in separate threads
        self.worker_threads = []
        duration = scenario_config.get('duration', 300)
        
        for worker in self.workers:
            thread = threading.Thread(target=worker.run, args=(duration,))
            thread.start()
            self.worker_threads.append(thread)
        
        logger.info(f"Started {len(self.workers)} workers")
    
    def _ramp_up_workers(self, scenario_config: Dict[str, Any], target_workers: int, ramp_duration: float):
        """Gradually start workers during ramp-up period."""
        results_queue = self.metrics_collector.get_results_queue()
        worker_start_interval = ramp_duration / target_workers
        
        self.worker_threads = []
        scenario_duration = scenario_config.get('duration', 300)
        
        for i in range(target_workers):
            # Create worker
            worker = WorkloadWorker(
                worker_id=f"worker_{i}",
                query_executor=self.query_executor,
                scenario_config=scenario_config,
                results_queue=results_queue
            )
            self.workers.append(worker)
            
            # Start worker
            thread = threading.Thread(target=worker.run, args=(scenario_duration,))
            thread.start()
            self.worker_threads.append(thread)
            
            logger.debug(f"Started worker {i+1}/{target_workers}")
            
            # Wait before starting next worker
            if i < target_workers - 1:
                time.sleep(worker_start_interval)
        
        logger.info(f"Ramp-up completed: {target_workers} workers started")
    
    def _monitor_execution(self, duration: float):
        """Monitor test execution and collect results."""
        start_time = time.time()
        last_report = start_time
        report_interval = 30  # Report every 30 seconds
        
        while time.time() - start_time < duration:
            # Collect results from queue
            collected_count = 0
            while not self.metrics_collector.results_queue.empty():
                try:
                    result = self.metrics_collector.results_queue.get_nowait()
                    self.metrics_collector.collect_result(result)
                    collected_count += 1
                except queue.Empty:
                    break
            
            # Periodic progress report
            current_time = time.time()
            if current_time - last_report >= report_interval:
                elapsed = current_time - start_time
                remaining = duration - elapsed
                total_results = len(self.metrics_collector.results)
                
                logger.info(f"Progress: {elapsed:.1f}s elapsed, {remaining:.1f}s remaining, "
                          f"{total_results} queries completed")
                last_report = current_time
            
            time.sleep(1)  # Check every second
    
    def _ramp_down_workers(self, ramp_duration: float):
        """Gradually stop workers during ramp-down period."""
        if not self.workers:
            return
        
        worker_stop_interval = ramp_duration / len(self.workers)
        
        for i, worker in enumerate(self.workers):
            worker.stop()
            logger.debug(f"Stopped worker {i+1}/{len(self.workers)}")
            
            if i < len(self.workers) - 1:
                time.sleep(worker_stop_interval)
        
        # Wait for all threads to complete
        for thread in self.worker_threads:
            thread.join(timeout=10)
        
        logger.info("Ramp-down completed")
    
    def _stop_all_workers(self):
        """Stop all workers immediately."""
        for worker in self.workers:
            worker.stop()
        
        # Wait for threads to complete
        if hasattr(self, 'worker_threads'):
            for thread in self.worker_threads:
                thread.join(timeout=5)
        
        self.workers.clear()
        logger.info("All workers stopped")
    
    def _check_performance_targets(self, metrics: TestMetrics, scenario_config: Dict[str, Any]):
        """Check if performance targets were met."""
        targets = scenario_config.get('performance_targets', {})
        if not targets:
            return
        
        logger.info("Checking performance targets...")
        
        checks = []
        
        # Response time targets
        if 'avg_response_time_ms' in targets:
            target = targets['avg_response_time_ms']
            actual = metrics.avg_response_time
            passed = actual <= target
            checks.append(('Avg Response Time', target, actual, passed, 'ms'))
        
        if 'p95_response_time_ms' in targets:
            target = targets['p95_response_time_ms']
            actual = metrics.p95_response_time
            passed = actual <= target
            checks.append(('P95 Response Time', target, actual, passed, 'ms'))
        
        if 'p99_response_time_ms' in targets:
            target = targets['p99_response_time_ms']
            actual = metrics.p99_response_time
            passed = actual <= target
            checks.append(('P99 Response Time', target, actual, passed, 'ms'))
        
        # Throughput targets
        if 'min_throughput_qps' in targets:
            target = targets['min_throughput_qps']
            actual = metrics.throughput_qps
            passed = actual >= target
            checks.append(('Throughput', target, actual, passed, 'QPS'))
        
        # Error rate targets
        if 'max_error_rate_percent' in targets:
            target = targets['max_error_rate_percent']
            actual = metrics.error_rate
            passed = actual <= target
            checks.append(('Error Rate', target, actual, passed, '%'))
        
        # Print results
        for check_name, target, actual, passed, unit in checks:
            status = "✓ PASS" if passed else "✗ FAIL"
            logger.info(f"  {check_name}: {status} (Target: {target}{unit}, Actual: {actual:.2f}{unit})")


class ReportGenerator:
    """Generates test reports in various formats."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.reporting_config = config.get('reporting', {})
    
    def generate_report(self, metrics: TestMetrics, output_dir: str):
        """Generate comprehensive test report."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Generate different format reports
        formats = self.reporting_config.get('formats', ['json', 'html'])
        
        if 'json' in formats:
            self._generate_json_report(metrics, output_path)
        
        if 'html' in formats and PANDAS_AVAILABLE:
            self._generate_html_report(metrics, output_path)
        
        if 'csv' in formats and PANDAS_AVAILABLE:
            self._generate_csv_report(metrics, output_path)
        
        if 'markdown' in formats:
            self._generate_markdown_report(metrics, output_path)
        
        logger.info(f"Reports generated in: {output_path}")
    
    def _generate_json_report(self, metrics: TestMetrics, output_path: Path):
        """Generate JSON report."""
        report_data = {
            'test_summary': asdict(metrics),
            'test_configuration': self.config,
            'generation_timestamp': datetime.now().isoformat()
        }
        
        json_file = output_path / f"{metrics.scenario_name}_report.json"
        with open(json_file, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        logger.info(f"JSON report: {json_file}")
    
    def _generate_html_report(self, metrics: TestMetrics, output_path: Path):
        """Generate HTML report with charts."""
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Concurrency Test Report - {metrics.scenario_name}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
                .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin: 20px 0; }}
                .metric-card {{ border: 1px solid #ddd; padding: 15px; border-radius: 5px; }}
                .metric-value {{ font-size: 24px; font-weight: bold; color: #2c3e50; }}
                .metric-label {{ font-size: 14px; color: #7f8c8d; }}
                .pass {{ color: #27ae60; }}
                .fail {{ color: #e74c3c; }}
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Concurrency Test Report</h1>
                <h2>{metrics.scenario_name}</h2>
                <p>Test Duration: {metrics.total_duration:.1f} seconds</p>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            
            <div class="metrics">
                <div class="metric-card">
                    <div class="metric-value">{metrics.total_queries}</div>
                    <div class="metric-label">Total Queries</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">{metrics.throughput_qps:.1f}</div>
                    <div class="metric-label">Queries per Second</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">{metrics.avg_response_time:.1f}ms</div>
                    <div class="metric-label">Avg Response Time</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">{metrics.p95_response_time:.1f}ms</div>
                    <div class="metric-label">P95 Response Time</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">{metrics.error_rate:.1f}%</div>
                    <div class="metric-label">Error Rate</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">{metrics.concurrent_connections}</div>
                    <div class="metric-label">Concurrent Connections</div>
                </div>
            </div>
            
            <h3>Performance Details</h3>
            <table>
                <tr><th>Metric</th><th>Value</th></tr>
                <tr><td>Successful Queries</td><td>{metrics.successful_queries}</td></tr>
                <tr><td>Failed Queries</td><td>{metrics.failed_queries}</td></tr>
                <tr><td>Min Response Time</td><td>{metrics.min_response_time:.1f}ms</td></tr>
                <tr><td>Max Response Time</td><td>{metrics.max_response_time:.1f}ms</td></tr>
                <tr><td>P50 Response Time</td><td>{metrics.p50_response_time:.1f}ms</td></tr>
                <tr><td>P99 Response Time</td><td>{metrics.p99_response_time:.1f}ms</td></tr>
            </table>
            
            <h3>Error Breakdown</h3>
            <table>
                <tr><th>Error Type</th><th>Count</th></tr>
        """
        
        for error_type, count in metrics.errors_by_type.items():
            html_content += f"<tr><td>{error_type}</td><td>{count}</td></tr>"
        
        html_content += """
            </table>
        </body>
        </html>
        """
        
        html_file = output_path / f"{metrics.scenario_name}_report.html"
        with open(html_file, 'w') as f:
            f.write(html_content)
        
        logger.info(f"HTML report: {html_file}")
    
    def _generate_markdown_report(self, metrics: TestMetrics, output_path: Path):
        """Generate Markdown report."""
        md_content = f"""# Concurrency Test Report: {metrics.scenario_name}

## Test Summary

- **Duration**: {metrics.total_duration:.1f} seconds
- **Total Queries**: {metrics.total_queries}
- **Successful Queries**: {metrics.successful_queries}
- **Failed Queries**: {metrics.failed_queries}
- **Concurrent Connections**: {metrics.concurrent_connections}
- **Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Performance Metrics

| Metric | Value |
|--------|-------|
| Throughput | {metrics.throughput_qps:.1f} QPS |
| Average Response Time | {metrics.avg_response_time:.1f}ms |
| P50 Response Time | {metrics.p50_response_time:.1f}ms |
| P95 Response Time | {metrics.p95_response_time:.1f}ms |
| P99 Response Time | {metrics.p99_response_time:.1f}ms |
| Min Response Time | {metrics.min_response_time:.1f}ms |
| Max Response Time | {metrics.max_response_time:.1f}ms |
| Error Rate | {metrics.error_rate:.1f}% |

## Error Breakdown

| Error Type | Count |
|------------|-------|
"""
        
        for error_type, count in metrics.errors_by_type.items():
            md_content += f"| {error_type} | {count} |\\n"
        
        md_file = output_path / f"{metrics.scenario_name}_report.md"
        with open(md_file, 'w') as f:
            f.write(md_content)
        
        logger.info(f"Markdown report: {md_file}")
    
    def _generate_csv_report(self, metrics: TestMetrics, output_path: Path):
        """Generate CSV report for data analysis."""
        # Create summary CSV
        summary_data = {
            'scenario_name': [metrics.scenario_name],
            'total_duration': [metrics.total_duration],
            'total_queries': [metrics.total_queries],
            'successful_queries': [metrics.successful_queries],
            'failed_queries': [metrics.failed_queries],
            'throughput_qps': [metrics.throughput_qps],
            'avg_response_time': [metrics.avg_response_time],
            'p50_response_time': [metrics.p50_response_time],
            'p95_response_time': [metrics.p95_response_time],
            'p99_response_time': [metrics.p99_response_time],
            'error_rate': [metrics.error_rate],
            'concurrent_connections': [metrics.concurrent_connections]
        }
        
        df = pd.DataFrame(summary_data)
        csv_file = output_path / f"{metrics.scenario_name}_summary.csv"
        df.to_csv(csv_file, index=False)
        
        logger.info(f"CSV report: {csv_file}")


def load_config(config_file: str) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    if not YAML_AVAILABLE:
        raise ImportError("PyYAML required for configuration loading")
    
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        # Expand environment variables
        config = expand_environment_variables(config)
        
        return config
        
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_file}")
        sys.exit(1)
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML configuration: {e}")
        sys.exit(1)


def expand_environment_variables(obj):
    """Recursively expand environment variables in configuration."""
    if isinstance(obj, dict):
        return {k: expand_environment_variables(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [expand_environment_variables(item) for item in obj]
    elif isinstance(obj, str) and obj.startswith('${') and obj.endswith('}'):
        env_var = obj[2:-1]
        return os.environ.get(env_var, obj)
    else:
        return obj


def main():
    parser = argparse.ArgumentParser(
        description='Run Postgres concurrency tests'
    )
    
    parser.add_argument(
        '--config',
        required=True,
        help='Path to concurrency test configuration file'
    )
    
    # Scenario selection
    scenario_group = parser.add_mutually_exclusive_group(required=True)
    scenario_group.add_argument(
        '--scenario',
        help='Name of specific scenario to run'
    )
    scenario_group.add_argument(
        '--all-scenarios',
        action='store_true',
        help='Run all configured scenarios'
    )
    scenario_group.add_argument(
        '--list-scenarios',
        action='store_true',
        help='List available scenarios and exit'
    )
    
    # Output options
    parser.add_argument(
        '--output-dir',
        default='./test_results',
        help='Output directory for test results'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate configuration without running tests'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Load configuration
    logger.info(f"Loading configuration from {args.config}")
    config = load_config(args.config)
    
    # List scenarios if requested
    if args.list_scenarios:
        scenarios = config.get('test_scenarios', [])
        print("\\nAvailable test scenarios:")
        for scenario in scenarios:
            name = scenario.get('name', 'unnamed')
            description = scenario.get('description', 'No description')
            duration = scenario.get('duration', 'Unknown')
            connections = scenario.get('concurrent_connections', 'Unknown')
            print(f"  {name}: {description}")
            print(f"    Duration: {duration}s, Connections: {connections}")
        return
    
    # Validate configuration
    if args.dry_run:
        logger.info("Dry run mode - validating configuration...")
        
        # Check connection parameters
        conn_config = config.get('postgres_connection', {})
        required_fields = ['host', 'port', 'database', 'username', 'password']
        missing_fields = [f for f in required_fields if not conn_config.get(f)]
        
        if missing_fields:
            logger.error(f"Missing connection parameters: {missing_fields}")
            sys.exit(1)
        
        # Check scenarios
        scenarios = config.get('test_scenarios', [])
        if not scenarios:
            logger.error("No test scenarios configured")
            sys.exit(1)
        
        logger.info(f"Configuration valid. Found {len(scenarios)} scenarios.")
        return
    
    # Initialize test runner
    runner = ConcurrencyTestRunner(config)
    report_generator = ReportGenerator(config)
    
    # Determine scenarios to run
    scenarios_to_run = []
    if args.all_scenarios:
        scenarios = config.get('test_scenarios', [])
        scenarios_to_run = [s.get('name') for s in scenarios if s.get('name')]
    else:
        scenarios_to_run = [args.scenario]
    
    # Run scenarios
    all_metrics = []
    for scenario_name in scenarios_to_run:
        try:
            logger.info(f"\\n{'='*60}")
            logger.info(f"Running scenario: {scenario_name}")
            logger.info(f"{'='*60}")
            
            metrics = runner.run_scenario(scenario_name)
            all_metrics.append(metrics)
            
            # Generate individual scenario report
            scenario_output_dir = os.path.join(args.output_dir, scenario_name)
            report_generator.generate_report(metrics, scenario_output_dir)
            
            logger.info(f"\\nScenario {scenario_name} completed successfully")
            logger.info(f"Results: {metrics.successful_queries}/{metrics.total_queries} queries successful")
            logger.info(f"Throughput: {metrics.throughput_qps:.1f} QPS")
            logger.info(f"Avg Response Time: {metrics.avg_response_time:.1f}ms")
            logger.info(f"Error Rate: {metrics.error_rate:.1f}%")
            
        except Exception as e:
            logger.error(f"Scenario {scenario_name} failed: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
    
    # Generate summary report if multiple scenarios
    if len(all_metrics) > 1:
        logger.info(f"\\n{'='*60}")
        logger.info("SUMMARY REPORT")
        logger.info(f"{'='*60}")
        
        for metrics in all_metrics:
            logger.info(f"{metrics.scenario_name}:")
            logger.info(f"  Throughput: {metrics.throughput_qps:.1f} QPS")
            logger.info(f"  Avg Response: {metrics.avg_response_time:.1f}ms")
            logger.info(f"  Error Rate: {metrics.error_rate:.1f}%")
    
    logger.info(f"\\nAll test results saved to: {args.output_dir}")


if __name__ == '__main__':
    main()
