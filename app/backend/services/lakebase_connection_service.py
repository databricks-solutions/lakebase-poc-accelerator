import asyncio
import logging
import os
import time
import uuid
from typing import Dict, List, Any, Optional, Tuple

from databricks.sdk import WorkspaceClient
from sqlalchemy import URL, event, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from services.oauth_service import DatabricksOAuthService
from models.query_models import QueryExecutionResult, ConcurrencyTestReport


logger = logging.getLogger(__name__)

class LakebaseConnectionService:
    """
    Singleton service for managing Lakebase Postgres connections
    following Databricks best practices for OAuth authentication
    and connection pooling.
    """
    
    _instance = None
    _engine: Optional[AsyncEngine] = None
    _instance_info = None
    _credentials = None
    _oauth_service = None
    _workspace_client: Optional[WorkspaceClient] = None
    _database_instance = None
    _postgres_password: Optional[str] = None
    _last_password_refresh: float = 0.0
    _token_refresh_task: Optional[asyncio.Task] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LakebaseConnectionService, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._engine = None
            self._instance_info = None
            self._credentials = None
            self._oauth_service = DatabricksOAuthService()
            self._workspace_client = None
            self._database_instance = None
            self._postgres_password = None
            self._last_password_refresh = 0.0
            self._token_refresh_task = None
            self._initialized = True
            self._profile = 'DEFAULT'
    
    async def initialize_connection_pool(
        self,
        workspace_url: str,
        instance_name: str,
        database: str,
        pool_config: Dict[str, Any],
        profile: str = 'DEFAULT'
    ) -> bool:
        """
        Initialize connection pool using Databricks OAuth authentication.
        
        Args:
            workspace_url: Databricks workspace URL (now primarily for context, profile is key)
            instance_name: Lakebase instance name
            database: Target database name
            pool_config: Connection pool configuration
            profile: Databricks CLI profile name to use for authentication
            
        Returns:
            bool: True if initialization successful
        """
        try:
            # Set the profile and initialize Databricks SDK client
            self._profile = profile
            self._workspace_client = WorkspaceClient(profile=self._profile) if self._profile else WorkspaceClient()

            # Lookup database instance by name
            self._database_instance = self._workspace_client.database.get_database_instance(name=instance_name)

            # Generate initial OAuth credential (Postgres token)
            cred = self._workspace_client.database.generate_database_credential(
                request_id=str(uuid.uuid4()),
                instance_names=[self._database_instance.name],
            )
            self._postgres_password = cred.token
            self._last_password_refresh = time.time()

            # Derive pool sizes. Align base pool to requested pool_size; allow overflow as configured
            base_pool_size = int(pool_config.get('DB_POOL_SIZE', max(1, int(pool_config.get('concurrency', 5)))))
            max_overflow = int(pool_config.get('DB_MAX_OVERFLOW', max(0, base_pool_size)))
            pool_timeout = int(pool_config.get('DB_POOL_TIMEOUT', 10))
            pool_recycle = int(pool_config.get('DB_POOL_RECYCLE_INTERVAL', 3600))
            command_timeout = int(pool_config.get('DB_COMMAND_TIMEOUT', 30))
            ssl_mode = pool_config.get('DB_SSL_MODE', 'require')

            # Build SQLAlchemy async URL for asyncpg
            username = self._workspace_client.current_user.me().user_name

            url = URL.create(
                drivername="postgresql+asyncpg",
                username=username,
                password="",  # password injected by do_connect event
                host=self._database_instance.read_write_dns,
                port=5432,
                database=database,
            )

            self._engine = create_async_engine(
                url,
                pool_pre_ping=False,
                echo=False,
                pool_size=base_pool_size,
                max_overflow=max_overflow,
                pool_timeout=pool_timeout,
                pool_recycle=pool_recycle,
                connect_args={
                    "command_timeout": command_timeout,
                    "server_settings": {
                        "application_name": "lakebase_concurrency_tester",
                    },
                    "ssl": ssl_mode,
                },
            )

            # Inject fresh tokens for every new DB connection
            @event.listens_for(self._engine.sync_engine, "do_connect")
            def _provide_token(dialect, conn_rec, cargs, cparams):
                cparams["password"] = self._postgres_password

            # Start background token refresh task
            await self._start_token_refresh()

            logger.info(
                f"Lakebase async engine initialized. pool_size={base_pool_size}, max_overflow={max_overflow}, timeout={pool_timeout}s"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to initialize async engine: {e}")
            raise e  # Re-raise the specific exception instead of returning False
    
    async def execute_concurrent_queries(
        self,
        queries: List[Dict[str, Any]],
        concurrency_level: int
    ) -> ConcurrencyTestReport:
        """
        Execute queries concurrently with proper connection management.
        
        Args:
            queries: List of query configurations
            concurrency_level: Number of concurrent connections
            
        Returns:
            ConcurrencyTestReport with execution results and metrics
        """
        if not self._engine:
            raise Exception("Engine not initialized")
        
        test_id = f"test_{int(time.time())}"
        test_start_time = time.time()
        all_results = []
        
        try:
            sem = asyncio.Semaphore(concurrency_level)

            async def _runner(query_config: Dict[str, Any], scenario: Dict[str, Any]) -> List[QueryExecutionResult]:
                results: List[QueryExecutionResult] = []
                exec_count = int(scenario.get('execution_count', 1))
                for _ in range(exec_count):
                    async with sem:
                        res = await self._execute_single_query_async(
                            query_config['query_content'],
                            scenario['parameters'],
                            query_config['query_identifier'],
                            scenario['name']
                        )
                        results.append(res)
                return results

            tasks: List[asyncio.Task] = []
            for qc in queries:
                for sc in qc.get('test_scenarios', []):
                    tasks.append(asyncio.create_task(_runner(qc, sc)))

            nested_results = await asyncio.gather(*tasks, return_exceptions=True)

            for item in nested_results:
                if isinstance(item, Exception):
                    logger.error(f"Query execution failed: {item}")
                    all_results.append(
                        QueryExecutionResult(
                            query_identifier="unknown",
                            parameter_set_name="error",
                            execution_start_time=time.time(),
                            execution_end_time=time.time(),
                            duration_ms=0,
                            success=False,
                            error_message=str(item),
                            error_type="execution_error",
                        )
                    )
                else:
                    all_results.extend(item)

            test_end_time = time.time()

            report = self._generate_test_report(
                test_id=test_id,
                test_start_time=test_start_time,
                test_end_time=test_end_time,
                concurrency_level=concurrency_level,
                results=all_results,
            )

            return report

        except Exception as e:
            logger.error(f"Concurrent query execution failed: {e}")
            raise Exception(f"Query execution failed: {e}")
    
    async def _execute_single_query_async(
        self,
        query: str,
        parameters: List[Any],
        query_identifier: str,
        scenario_name: str,
    ) -> QueryExecutionResult:
        """Execute a single query asynchronously using the async engine.

        The frontend provides queries with %s placeholders. SQLAlchemy/asyncpg
        expects bound parameters. We convert %s placeholders to :p1, :p2, ...
        and pass a mapping of {p1: val1, p2: val2, ...}.
        """
        if not self._engine:
            raise RuntimeError("Engine not initialized")

        start_time = time.time()
        try:
            converted_query, params_map = self._convert_ps_placeholders(query, parameters)
            async with self._engine.connect() as conn:
                result = await conn.execute(text(converted_query), params_map)
                # For SELECTs, fetch all to ensure consistent behavior
                row_count = result.rowcount or 0
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000

            return QueryExecutionResult(
                query_identifier=query_identifier,
                parameter_set_name=scenario_name,
                execution_start_time=start_time,
                execution_end_time=end_time,
                duration_ms=duration_ms,
                success=True,
                rows_returned=row_count,
            )
        except Exception as e:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000
            logger.error(f"Query execution error: {e}")
            return QueryExecutionResult(
                query_identifier=query_identifier,
                parameter_set_name=scenario_name,
                execution_start_time=start_time,
                execution_end_time=end_time,
                duration_ms=duration_ms,
                success=False,
                error_message=str(e),
                error_type=type(e).__name__,
            )
    
    def _generate_test_report(
        self,
        test_id: str,
        test_start_time: float,
        test_end_time: float,
        concurrency_level: int,
        results: List[QueryExecutionResult]
    ) -> ConcurrencyTestReport:
        """
        Generate comprehensive test report from execution results.
        
        Args:
            test_id: Unique test identifier
            test_start_time: Test start time
            test_end_time: Test end time
            concurrency_level: Number of concurrent connections
            results: List of query execution results
            
        Returns:
            ConcurrencyTestReport with metrics and analysis
        """
        total_duration = test_end_time - test_start_time
        successful_results = [r for r in results if r.success]
        failed_results = [r for r in results if not r.success]
        
        # Calculate metrics
        total_queries = len(results)
        successful_executions = len(successful_results)
        failed_executions = len(failed_results)
        success_rate = successful_executions / total_queries if total_queries > 0 else 0
        
        # Calculate timing metrics
        execution_times = [r.duration_ms for r in successful_results]
        if execution_times:
            avg_execution_time = sum(execution_times) / len(execution_times)
            min_execution_time = min(execution_times)
            max_execution_time = max(execution_times)
            
            # Calculate percentiles
            sorted_times = sorted(execution_times)
            logger.info(f"Sorted execution times: {sorted_times}")
            
            # Log the sorted data for debugging
            logger.info(f"Calculating percentiles for {len(sorted_times)} execution times")
            logger.info(f"Sorted execution times (first 10): {sorted_times[:10]}")
            if len(sorted_times) > 10:
                logger.info(f"Sorted execution times (last 10): {sorted_times[-10:]}")
            
            p95_index = int(len(sorted_times) * 0.95)
            p99_index = int(len(sorted_times) * 0.99)
            
            logger.info(f"P95 calculation: index = {p95_index} (len * 0.95 = {len(sorted_times) * 0.95:.4f})")
            logger.info(f"P99 calculation: index = {p99_index} (len * 0.99 = {len(sorted_times) * 0.99:.4f})")
            
            p95_execution_time = sorted_times[p95_index] if p95_index < len(sorted_times) else max_execution_time
            p99_execution_time = sorted_times[p99_index] if p99_index < len(sorted_times) else max_execution_time
            
            logger.info(f"P95 result: {p95_execution_time}ms at index {p95_index}")
            logger.info(f"P99 result: {p99_execution_time}ms at index {p99_index}")
        else:
            avg_execution_time = 0
            min_execution_time = 0
            max_execution_time = 0
            p95_execution_time = 0
            p99_execution_time = 0
        
        # Calculate throughput
        throughput = total_queries / total_duration if total_duration > 0 else 0
        
        # Generate recommendations
        recommendations = self._generate_recommendations(
            success_rate, avg_execution_time, throughput, concurrency_level
        )
        
        # Connection pool metrics (best-effort from config)
        pool_metrics = {
            "pool_size": getattr(self._engine, "pool", None).size() if self._engine else 0,
            "max_connections": getattr(self._engine, "pool", None).size() + getattr(self._engine, "pool", None).overflow() if self._engine else 0,
            "concurrency_level": concurrency_level,
        }
        
        return ConcurrencyTestReport(
            test_id=test_id,
            test_start_time=test_start_time,
            test_end_time=test_end_time,
            total_duration_seconds=total_duration,
            concurrency_level=concurrency_level,
            total_queries_executed=total_queries,
            successful_executions=successful_executions,
            failed_executions=failed_executions,
            success_rate=success_rate,
            average_execution_time_ms=avg_execution_time,
            min_execution_time_ms=min_execution_time,
            max_execution_time_ms=max_execution_time,
            p95_execution_time_ms=p95_execution_time,
            p99_execution_time_ms=p99_execution_time,
            throughput_queries_per_second=throughput,
            query_results=results,
            connection_pool_metrics=pool_metrics,
            recommendations=recommendations
        )
    
    def _generate_recommendations(
        self,
        success_rate: float,
        avg_execution_time: float,
        throughput: float,
        concurrency_level: int
    ) -> List[str]:
        """Generate performance recommendations based on test results."""
        recommendations = []
        
        if success_rate < 0.95:
            recommendations.append("Success rate is below 95%. Check for connection issues or query errors.")
        
        if avg_execution_time > 5000:  # 5 seconds
            recommendations.append("Average execution time is high. Consider optimizing queries or increasing connection pool size.")
        
        if throughput < 10:  # queries per second
            recommendations.append("Throughput is low. Consider increasing concurrency level or optimizing queries.")
        
        if concurrency_level > 50 and success_rate < 0.99:
            recommendations.append("High concurrency with low success rate. Consider reducing concurrency level.")
        
        if not recommendations:
            recommendations.append("Performance looks good! Consider running longer tests for more accurate metrics.")
        
        return recommendations
    
    def close_connection_pool(self):
        """Close the engine and cleanup resources."""
        if self._engine:
            # Engine dispose is sync method on sync engine; async engine provides .dispose() coroutine
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self._engine.dispose())
                else:
                    loop.run_until_complete(self._engine.dispose())
            except RuntimeError:
                # Fallback: create a new loop just to dispose
                asyncio.run(self._engine.dispose())
            self._engine = None
            logger.info("Async engine disposed")
    
    def get_pool_status(self) -> Dict[str, Any]:
        """Get current connection pool status."""
        if not self._engine:
            return {"status": "not_initialized"}
        
        return {
            "status": "active",
            "engine": "async_sqlalchemy_asyncpg",
        }

    async def _start_token_refresh(self):
        if self._token_refresh_task and not self._token_refresh_task.done():
            return

        async def _refresh_loop():
            while True:
                try:
                    await asyncio.sleep(50 * 60)  # refresh every 50 minutes
                    cred = self._workspace_client.database.generate_database_credential(
                        request_id=str(uuid.uuid4()),
                        instance_names=[self._database_instance.name],
                    )
                    self._postgres_password = cred.token
                    self._last_password_refresh = time.time()
                    logger.info("Background token refresh: Token updated")
                except Exception as e:
                    logger.error(f"Background token refresh failed: {e}")

        self._token_refresh_task = asyncio.create_task(_refresh_loop())

    @staticmethod
    def _convert_ps_placeholders(query: str, params: List[Any]) -> Tuple[str, Dict[str, Any]]:
        """Convert %s placeholders to :p1, :p2 ... and build parameter mapping.

        SQLAlchemy text binds use named parameters. This keeps frontend simple (%s)
        while allowing safe execution with asyncpg.
        """
        converted = []
        idx = 0
        i = 0
        while i < len(query):
            if i + 1 < len(query) and query[i] == '%' and query[i + 1] == 's':
                idx += 1
                converted.append(f":p{idx}")
                i += 2
            else:
                converted.append(query[i])
                i += 1
        mapping = {f"p{i+1}": params[i] for i in range(len(params))}
        return "".join(converted), mapping
