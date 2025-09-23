import statistics
from typing import List, Dict, Any
from models.query_models import QueryExecutionResult, ConcurrencyTestReport
import logging

logger = logging.getLogger(__name__)

class ConcurrencyMetricsService:
    """
    Collects and analyzes performance metrics for concurrency tests.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def collect_execution_metrics(
        self,
        results: List[QueryExecutionResult]
    ) -> ConcurrencyTestReport:
        """
        Analyze query execution results and generate performance report.
        
        Args:
            results: List of query execution results
            
        Returns:
            ConcurrencyTestReport with comprehensive metrics
        """
        if not results:
            raise ValueError("No execution results provided")
        
        # Basic metrics
        total_queries = len(results)
        successful_results = [r for r in results if r.success]
        failed_results = [r for r in results if not r.success]
        
        successful_executions = len(successful_results)
        failed_executions = len(failed_results)
        success_rate = successful_executions / total_queries if total_queries > 0 else 0
        
        # Timing metrics
        execution_times = [r.duration_ms for r in successful_results]
        
        if execution_times:
            avg_execution_time = statistics.mean(execution_times)
            min_execution_time = min(execution_times)
            max_execution_time = max(execution_times)
            median_execution_time = statistics.median(execution_times)
            
            # Calculate percentiles
            sorted_times = sorted(execution_times)
            p95_execution_time = self._calculate_percentile(sorted_times, 95)
            p99_execution_time = self._calculate_percentile(sorted_times, 99)
            p90_execution_time = self._calculate_percentile(sorted_times, 90)
        else:
            avg_execution_time = 0
            min_execution_time = 0
            max_execution_time = 0
            median_execution_time = 0
            p95_execution_time = 0
            p99_execution_time = 0
            p90_execution_time = 0
        
        # Calculate test duration
        start_times = [r.execution_start_time for r in results]
        end_times = [r.execution_end_time for r in results]
        test_start_time = min(start_times) if start_times else 0
        test_end_time = max(end_times) if end_times else 0
        total_duration = test_end_time - test_start_time if test_start_time and test_end_time else 0
        
        # Calculate throughput
        throughput = total_queries / total_duration if total_duration > 0 else 0
        
        # Error analysis
        error_analysis = self._analyze_errors(failed_results)
        
        # Query performance analysis
        query_performance = self._analyze_query_performance(results)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(
            success_rate, avg_execution_time, throughput, error_analysis
        )
        
        # Connection pool metrics (placeholder - would be populated by connection service)
        connection_pool_metrics = {
            "total_connections_used": len(set(r.connection_id for r in results if r.connection_id)),
            "average_connection_utilization": self._calculate_connection_utilization(results)
        }
        
        return ConcurrencyTestReport(
            test_id=f"test_{int(test_start_time)}",
            test_start_time=test_start_time,
            test_end_time=test_end_time,
            total_duration_seconds=total_duration,
            concurrency_level=0,  # Will be set by caller
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
            connection_pool_metrics=connection_pool_metrics,
            recommendations=recommendations
        )
    
    def _calculate_percentile(self, sorted_data: List[float], percentile: int) -> float:
        """Calculate the given percentile of the data."""
        if not sorted_data:
            return 0
        
        index = (percentile / 100) * (len(sorted_data) - 1)
        if index.is_integer():
            return sorted_data[int(index)]
        else:
            lower = sorted_data[int(index)]
            upper = sorted_data[int(index) + 1]
            return lower + (upper - lower) * (index - int(index))
    
    def _analyze_errors(self, failed_results: List[QueryExecutionResult]) -> Dict[str, Any]:
        """Analyze error patterns in failed executions."""
        if not failed_results:
            return {"total_errors": 0, "error_types": {}}
        
        error_types = {}
        for result in failed_results:
            error_type = result.error_type or "unknown"
            error_types[error_type] = error_types.get(error_type, 0) + 1
        
        return {
            "total_errors": len(failed_results),
            "error_types": error_types,
            "most_common_error": max(error_types.items(), key=lambda x: x[1])[0] if error_types else None
        }
    
    def _analyze_query_performance(self, results: List[QueryExecutionResult]) -> Dict[str, Any]:
        """Analyze performance by query type and identifier."""
        query_stats = {}
        
        for result in results:
            query_id = result.query_identifier
            if query_id not in query_stats:
                query_stats[query_id] = {
                    "total_executions": 0,
                    "successful_executions": 0,
                    "failed_executions": 0,
                    "execution_times": [],
                    "success_rate": 0
                }
            
            stats = query_stats[query_id]
            stats["total_executions"] += 1
            
            if result.success:
                stats["successful_executions"] += 1
                stats["execution_times"].append(result.duration_ms)
            else:
                stats["failed_executions"] += 1
            
            stats["success_rate"] = stats["successful_executions"] / stats["total_executions"]
        
        # Calculate average execution times for each query
        for query_id, stats in query_stats.items():
            if stats["execution_times"]:
                stats["avg_execution_time"] = statistics.mean(stats["execution_times"])
                stats["min_execution_time"] = min(stats["execution_times"])
                stats["max_execution_time"] = max(stats["execution_times"])
            else:
                stats["avg_execution_time"] = 0
                stats["min_execution_time"] = 0
                stats["max_execution_time"] = 0
        
        return query_stats
    
    def _calculate_connection_utilization(self, results: List[QueryExecutionResult]) -> float:
        """Calculate average connection utilization."""
        if not results:
            return 0
        
        # Group results by connection ID
        connection_usage = {}
        for result in results:
            conn_id = result.connection_id
            if conn_id:
                if conn_id not in connection_usage:
                    connection_usage[conn_id] = []
                connection_usage[conn_id].append(result.duration_ms)
        
        if not connection_usage:
            return 0
        
        # Calculate average usage per connection
        total_usage = sum(sum(times) for times in connection_usage.values())
        total_connections = len(connection_usage)
        
        return total_usage / total_connections if total_connections > 0 else 0
    
    def _generate_recommendations(
        self,
        success_rate: float,
        avg_execution_time: float,
        throughput: float,
        error_analysis: Dict[str, Any]
    ) -> List[str]:
        """Generate performance recommendations based on metrics."""
        recommendations = []
        
        # Success rate recommendations
        if success_rate < 0.95:
            recommendations.append("Success rate is below 95%. Check for connection issues, query errors, or resource constraints.")
        
        if success_rate < 0.99 and error_analysis.get("total_errors", 0) > 0:
            most_common_error = error_analysis.get("most_common_error")
            if most_common_error:
                recommendations.append(f"Most common error is '{most_common_error}'. Investigate and fix this issue.")
        
        # Performance recommendations
        if avg_execution_time > 5000:  # 5 seconds
            recommendations.append("Average execution time is high (>5s). Consider optimizing queries, adding indexes, or increasing connection pool size.")
        
        if avg_execution_time > 10000:  # 10 seconds
            recommendations.append("Average execution time is very high (>10s). This may indicate serious performance issues that need immediate attention.")
        
        # Throughput recommendations
        if throughput < 10:  # queries per second
            recommendations.append("Throughput is low (<10 qps). Consider increasing concurrency level, optimizing queries, or scaling resources.")
        
        if throughput > 100:  # queries per second
            recommendations.append("High throughput achieved! Consider running longer tests to validate stability under sustained load.")
        
        # General recommendations
        if success_rate >= 0.99 and avg_execution_time < 1000 and throughput > 50:
            recommendations.append("Excellent performance! Consider running extended tests to validate stability and identify any potential issues.")
        
        if not recommendations:
            recommendations.append("Performance looks good! Consider running longer tests for more comprehensive analysis.")
        
        return recommendations
    
    def generate_summary_report(self, report: ConcurrencyTestReport) -> Dict[str, Any]:
        """Generate a summary report for easy consumption."""
        return {
            "test_id": report.test_id,
            "duration_seconds": round(report.total_duration_seconds, 2),
            "total_queries": report.total_queries_executed,
            "success_rate": f"{report.success_rate:.2%}",
            "avg_execution_time_ms": round(report.average_execution_time_ms, 2),
            "throughput_qps": round(report.throughput_queries_per_second, 2),
            "p95_latency_ms": round(report.p95_execution_time_ms, 2),
            "p99_latency_ms": round(report.p99_execution_time_ms, 2),
            "recommendations_count": len(report.recommendations),
            "top_recommendations": report.recommendations[:3]  # Top 3 recommendations
        }
