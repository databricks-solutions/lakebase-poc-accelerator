"""
Autoscaling Lakebase Connection Service

Manages PostgreSQL connections to autoscaling Lakebase endpoints using direct credentials.
Unlike provisioned Lakebase, autoscaling uses standard PostgreSQL authentication.
"""

import asyncio
import time
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
from contextlib import contextmanager


class AutoscalingConnectionService:
    """
    Service for managing connections to autoscaling Lakebase endpoints.
    
    Uses direct PostgreSQL connection parameters instead of OAuth tokens.
    """

    def __init__(self):
        """Initialize the autoscaling connection service."""
        self._engine = None
        self._connection_string = None
        self._pghost = None
        self._pgdatabase = None
        self._pguser = None

    async def initialize_connection_pool(
        self,
        pghost: str,
        pgport: int,
        pgdatabase: str,
        pguser: str,
        pgpassword: str,
        pgsslmode: str = "require",
        pool_config: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Initialize SQLAlchemy connection pool for autoscaling endpoint.
        
        Args:
            pghost: PostgreSQL endpoint hostname (ep-*.databricks.com)
            pgport: PostgreSQL port (default 5432)
            pgdatabase: Database name
            pguser: PostgreSQL username
            pgpassword: PostgreSQL password
            pgsslmode: SSL mode (require, prefer, allow, disable)
            pool_config: Optional pool configuration
            
        Returns:
            True if initialization successful
        """
        try:
            self._pghost = pghost
            self._pgdatabase = pgdatabase
            self._pguser = pguser
            
            # Build connection string
            self._connection_string = (
                f"postgresql://{pguser}:{pgpassword}@{pghost}:{pgport}/{pgdatabase}"
                f"?sslmode={pgsslmode}"
            )
            
            # Default pool configuration
            default_pool_config = {
                "base_pool_size": 5,
                "max_overflow": 10,
                "pool_timeout": 30,
                "pool_recycle": 3600,
                "command_timeout": 30,
                "pool_pre_ping": True
            }
            
            if pool_config:
                default_pool_config.update(pool_config)
            
            # Create SQLAlchemy engine with connection pooling
            self._engine = create_engine(
                self._connection_string,
                poolclass=QueuePool,
                pool_size=default_pool_config["base_pool_size"],
                max_overflow=default_pool_config["max_overflow"],
                pool_timeout=default_pool_config["pool_timeout"],
                pool_recycle=default_pool_config["pool_recycle"],
                pool_pre_ping=default_pool_config["pool_pre_ping"],
                echo=False,
                connect_args={
                    "options": f"-c statement_timeout={default_pool_config['command_timeout'] * 1000}"
                }
            )
            
            # Test connection
            with self._engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            
            print(f"✅ Connection pool initialized")
            print(f"   Host: {pghost}")
            print(f"   Database: {pgdatabase}")
            print(f"   User: {pguser}")
            print(f"   Pool Size: {default_pool_config['base_pool_size']}")
            print(f"   Max Overflow: {default_pool_config['max_overflow']}")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to initialize connection pool: {e}")
            return False

    @contextmanager
    def get_connection(self):
        """
        Context manager for getting a connection from the pool.
        
        Yields:
            SQLAlchemy connection
        """
        if not self._engine:
            raise RuntimeError("Connection pool not initialized")
        
        connection = self._engine.connect()
        try:
            yield connection
        finally:
            connection.close()

    def _execute_query_sync(
        self,
        query: str,
        parameters: Optional[List[Any]] = None
    ) -> Dict[str, Any]:
        """
        Synchronous query execution (to be called from thread pool).
        
        Args:
            query: SQL query to execute
            parameters: Query parameters (if any)
            
        Returns:
            Dictionary with execution results
        """
        start_time = time.time()
        
        try:
            with self.get_connection() as conn:
                # Get raw psycopg2 connection for parameterized queries
                raw_conn = conn.connection
                cursor = raw_conn.cursor()
                
                try:
                    if parameters:
                        # Execute with parameters using psycopg2 directly
                        cursor.execute(query, tuple(parameters))
                    else:
                        cursor.execute(query)
                    
                    # Fetch results
                    try:
                        rows = cursor.fetchall()
                    except psycopg2.ProgrammingError:
                        # No results to fetch (e.g., INSERT, UPDATE)
                        rows = []
                    
                    duration_ms = (time.time() - start_time) * 1000
                    
                    return {
                        "success": True,
                        "duration_ms": duration_ms,
                        "rows_returned": len(rows),
                        "error_message": None,
                        "error_type": None
                    }
                finally:
                    cursor.close()
                
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            error_msg = str(e)
            error_type = type(e).__name__
            
            # Log first few errors for debugging
            print(f"❌ Query Error [{error_type}]: {error_msg[:200]}")
            
            return {
                "success": False,
                "duration_ms": duration_ms,
                "rows_returned": 0,
                "error_message": error_msg,
                "error_type": error_type
            }
    
    async def execute_query(
        self,
        query: str,
        parameters: Optional[List[Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute a single query with optional parameters (async wrapper).
        
        Args:
            query: SQL query to execute
            parameters: Query parameters (if any)
            
        Returns:
            Dictionary with execution results
        """
        # Run blocking database call in thread pool to avoid blocking event loop
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._execute_query_sync, query, parameters)

    async def execute_concurrent_queries(
        self,
        queries: List[Dict[str, Any]],
        concurrency_level: int
    ) -> Dict[str, Any]:
        """
        Execute multiple queries concurrently.
        
        Args:
            queries: List of query configurations with test scenarios
            concurrency_level: Number of concurrent connections
            
        Returns:
            Comprehensive test report
        """
        print(f"\n{'='*70}")
        print("🚀 Starting Concurrent Query Testing")
        print(f"{'='*70}\n")
        
        print(f"📋 Test Configuration:")
        print(f"   Host: {self._pghost}")
        print(f"   Database: {self._pgdatabase}")
        print(f"   User: {self._pguser}")
        print(f"   Concurrency: {concurrency_level}")
        print(f"   Total Queries: {len(queries)}\n")
        
        # Prepare all test executions
        all_tasks = []
        
        for query_config in queries:
            query_identifier = query_config["query_identifier"]
            query_content = query_config["query_content"]
            test_scenarios = query_config.get("test_scenarios", [])
            
            # Remove comments from SQL
            sql_lines = [line for line in query_content.split('\n') 
                        if not line.strip().startswith('--')]
            clean_sql = '\n'.join(sql_lines).strip()
            
            for scenario in test_scenarios:
                parameters = scenario.get("parameters", [])
                execution_count = scenario.get("execution_count", 1)
                
                for _ in range(execution_count):
                    task = {
                        "query_identifier": query_identifier,
                        "query": clean_sql,
                        "parameters": parameters if parameters else None
                    }
                    all_tasks.append(task)
        
        print(f"📊 Prepared {len(all_tasks)} total query executions\n")
        
        # Execute queries concurrently
        start_time = time.time()
        
        async def execute_task(task):
            result = await self.execute_query(task["query"], task["parameters"])
            result["query_identifier"] = task["query_identifier"]
            result["parameters"] = task["parameters"]
            return result
        
        # Use semaphore to limit concurrency
        semaphore = asyncio.Semaphore(concurrency_level)
        
        async def execute_with_semaphore(task):
            async with semaphore:
                return await execute_task(task)
        
        results = await asyncio.gather(
            *[execute_with_semaphore(task) for task in all_tasks],
            return_exceptions=True
        )
        
        total_duration = time.time() - start_time
        
        # Process results
        query_results = []
        successful_queries = 0
        failed_queries = 0
        total_execution_time = 0
        execution_times = []
        
        for result in results:
            if isinstance(result, Exception):
                failed_queries += 1
                query_results.append({
                    "success": False,
                    "error_message": str(result),
                    "error_type": type(result).__name__
                })
            else:
                query_results.append(result)
                if result["success"]:
                    successful_queries += 1
                    execution_times.append(result["duration_ms"])
                    total_execution_time += result["duration_ms"]
                else:
                    failed_queries += 1
        
        # Calculate statistics
        success_rate = successful_queries / len(results) if results else 0
        average_execution_time = (
            total_execution_time / successful_queries if successful_queries > 0 else 0
        )
        throughput = len(results) / total_duration if total_duration > 0 else 0
        
        # Calculate percentiles
        if execution_times:
            execution_times.sort()
            p50_index = int(len(execution_times) * 0.50)
            p95_index = int(len(execution_times) * 0.95)
            p99_index = int(len(execution_times) * 0.99)
            
            p50_latency = execution_times[p50_index] if p50_index < len(execution_times) else 0
            p95_latency = execution_times[p95_index] if p95_index < len(execution_times) else 0
            p99_latency = execution_times[p99_index] if p99_index < len(execution_times) else 0
        else:
            p50_latency = p95_latency = p99_latency = 0
        
        # Get pool status
        pool_status = self.get_pool_status()
        
        # Build report
        report = {
            "concurrency_level": concurrency_level,
            "total_queries_executed": len(results),
            "successful_queries": successful_queries,
            "failed_queries": failed_queries,
            "success_rate": success_rate,
            "average_execution_time_ms": average_execution_time,
            "p50_execution_time_ms": p50_latency,
            "p95_execution_time_ms": p95_latency,
            "p99_execution_time_ms": p99_latency,
            "throughput_queries_per_second": throughput,
            "total_duration_seconds": total_duration,
            "connection_pool_metrics": pool_status,
            "query_results": query_results
        }
        
        print(f"\n{'='*70}")
        print("✅ Concurrent Query Testing Complete")
        print(f"{'='*70}\n")
        print(f"📊 Results:")
        print(f"   Total Queries: {len(results)}")
        print(f"   Successful: {successful_queries}")
        print(f"   Failed: {failed_queries}")
        print(f"   Success Rate: {success_rate * 100:.1f}%")
        print(f"   Throughput: {throughput:.2f} qps")
        print(f"   Avg Latency: {average_execution_time:.2f} ms")
        print(f"   P95 Latency: {p95_latency:.2f} ms")
        print(f"   P99 Latency: {p99_latency:.2f} ms")
        print(f"   Duration: {total_duration:.2f} seconds\n")
        
        return report

    def get_pool_status(self) -> Dict[str, Any]:
        """
        Get current connection pool status.
        
        Returns:
            Dictionary with pool metrics
        """
        if not self._engine:
            return {"status": "not_initialized"}
        
        pool = self._engine.pool
        
        return {
            "pool_size": pool.size(),
            "checked_in_connections": pool.checkedin(),
            "checked_out_connections": pool.checkedout(),
            "overflow": pool.overflow(),
            "status": "active"
        }

    def close_connection_pool(self):
        """Close the connection pool and release all resources."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            print("🔒 Connection pool closed")
