from pydantic import BaseModel, Field, validator
from typing import List, Optional, Union, Any, Dict
from enum import Enum

class ParameterType(str, Enum):
    INTEGER = "integer"
    STRING = "string"
    BOOLEAN = "boolean"
    DECIMAL = "decimal"
    DATE = "date"
    TIMESTAMP = "timestamp"

class QueryParameter(BaseModel):
    """Individual parameter configuration for a query."""
    parameter_index: int = Field(..., ge=1, description="Position of parameter in query")
    parameter_name: str = Field(..., min_length=1, max_length=50)
    data_type: ParameterType
    sample_value: Any = Field(..., description="Example value for this parameter")
    required: bool = True
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    pattern: Optional[str] = Field(None, description="Regex pattern for string validation")
    
    @validator('sample_value')
    def validate_sample_value(cls, v, values):
        data_type = values.get('data_type')
        if data_type == ParameterType.INTEGER and not isinstance(v, int):
            raise ValueError('Sample value must be integer for integer parameter type')
        elif data_type == ParameterType.BOOLEAN and not isinstance(v, bool):
            raise ValueError('Sample value must be boolean for boolean parameter type')
        elif data_type == ParameterType.DECIMAL and not isinstance(v, (int, float)):
            raise ValueError('Sample value must be numeric for decimal parameter type')
        return v

class ParameterSet(BaseModel):
    """Set of parameters for a specific test scenario."""
    set_name: str = Field(..., min_length=1, max_length=100)
    parameters: List[Any] = Field(..., min_items=1)
    execution_count: int = Field(..., ge=1, le=1000)
    description: Optional[str] = Field(None, max_length=500)

class QueryConfiguration(BaseModel):
    """Complete configuration for a single query."""
    query_identifier: str = Field(..., min_length=1, max_length=100)
    query_content: str = Field(..., min_length=1)
    parameter_definitions: List[QueryParameter] = Field(default_factory=list)
    parameter_sets: List[ParameterSet] = Field(..., min_items=1)
    total_executions: int = Field(..., ge=1)
    
    @validator('total_executions')
    def validate_total_executions(cls, v, values):
        parameter_sets = values.get('parameter_sets', [])
        calculated_total = sum(ps.execution_count for ps in parameter_sets)
        if v != calculated_total:
            raise ValueError(f'Total executions ({v}) must equal sum of parameter set executions ({calculated_total})')
        return v
    
    @validator('parameter_sets')
    def validate_parameter_sets(cls, v, values):
        parameter_definitions = values.get('parameter_definitions', [])
        expected_param_count = len(parameter_definitions)
        
        for param_set in v:
            if len(param_set.parameters) != expected_param_count:
                raise ValueError(f'Parameter set "{param_set.set_name}" has {len(param_set.parameters)} parameters, expected {expected_param_count}')
        return v

class ConcurrencyTestConfig(BaseModel):
    """Complete configuration for concurrency test execution."""
    workspace_url: str = Field(..., pattern=r'^https://.*\.databricks\.com/?$')
    instance_name: str = Field(..., min_length=3, max_length=63, pattern=r'^[a-zA-Z0-9-]+$')
    database_name: str = Field(default="databricks_postgres", min_length=1, max_length=63)
    concurrency_level: int = Field(..., ge=1, le=1000)
    connection_pool_config: Dict[str, Any] = Field(..., description="Connection pool settings")
    query_configurations: List[QueryConfiguration] = Field(..., min_items=1, max_items=50)
    test_duration_seconds: Optional[int] = Field(None, ge=10, le=3600)
    
    @validator('query_configurations')
    def validate_query_identifiers_unique(cls, v):
        identifiers = [qc.query_identifier for qc in v]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError('Query identifiers must be unique')
        return v

class QueryExecutionResult(BaseModel):
    """Result of a single query execution."""
    query_identifier: str
    parameter_set_name: str
    execution_start_time: float
    execution_end_time: float
    duration_ms: float
    success: bool
    error_message: Optional[str] = None
    error_type: Optional[str] = None
    rows_returned: Optional[int] = None
    connection_id: Optional[str] = None
    
    @property
    def execution_duration_seconds(self) -> float:
        return self.duration_ms / 1000.0

class ConcurrencyTestReport(BaseModel):
    """Comprehensive report of concurrency test results."""
    test_id: str
    test_start_time: float
    test_end_time: float
    total_duration_seconds: float
    concurrency_level: int
    total_queries_executed: int
    successful_executions: int
    failed_executions: int
    success_rate: float
    average_execution_time_ms: float
    min_execution_time_ms: float
    max_execution_time_ms: float
    p95_execution_time_ms: float
    p99_execution_time_ms: float
    throughput_queries_per_second: float
    query_results: List[QueryExecutionResult]
    connection_pool_metrics: Dict[str, Any]
    recommendations: List[str]
    
    @validator('success_rate')
    def validate_success_rate(cls, v):
        if not 0 <= v <= 1:
            raise ValueError('Success rate must be between 0 and 1')
        return v

# Simple models for user input
class SimpleParameterConfig(BaseModel):
    """Simple parameter configuration from user input."""
    name: str
    type: str
    sample_value: Any

class SimpleTestScenario(BaseModel):
    """Simple test scenario from user input."""
    name: str
    parameters: List[Any]
    execution_count: int = 1
    description: Optional[str] = None

class SimpleQueryConfig(BaseModel):
    """Simple query configuration from user input."""
    query_identifier: str
    query_content: str
    parameters: List[SimpleParameterConfig]
    test_scenarios: List[SimpleTestScenario]

class ConcurrencyTestRequest(BaseModel):
    """Request model for concurrency test execution."""
    workspace_url: str
    instance_name: str
    database_name: str = "databricks_postgres"
    concurrency_level: int
    connection_pool_config: Dict[str, Any]
    queries: List[SimpleQueryConfig]

# Pgbench-specific models
class PgbenchConfig(BaseModel):
    """Configuration for pgbench test execution."""
    clients: int = Field(..., ge=1, le=1000, description="Number of concurrent database sessions")
    jobs: int = Field(..., ge=1, le=100, description="Number of worker threads")
    duration_seconds: Optional[int] = Field(None, ge=1, le=3600, description="Run benchmark for specified duration")
    transactions_per_client: Optional[int] = Field(None, ge=1, description="Number of transactions per client")
    progress_interval: Optional[int] = Field(None, ge=1, le=60, description="Show progress reports every N seconds")
    protocol: str = Field(default="prepared", description="Query protocol mode")
    target_tps: Optional[int] = Field(None, ge=1, description="Target transactions per second rate")
    per_statement_latency: bool = Field(default=True, description="Report per-statement latency statistics")
    detailed_logging: bool = Field(default=True, description="Enable detailed transaction logging")
    connect_per_transaction: bool = Field(default=False, description="Establish new connection for each transaction")

    @validator('protocol')
    def validate_protocol(cls, v):
        if v not in ['simple', 'extended', 'prepared']:
            raise ValueError('Protocol must be one of: simple, extended, prepared')
        return v

    @validator('jobs')
    def validate_jobs_vs_clients(cls, v, values):
        clients = values.get('clients', 1)
        if v > clients:
            raise ValueError('Number of jobs cannot exceed number of clients')
        return v

class PgbenchQueryConfig(BaseModel):
    """Query configuration for pgbench execution."""
    query_identifier: str = Field(..., min_length=1, max_length=100)
    query_content: str = Field(..., min_length=1, description="SQL query in pgbench format")
    weight: int = Field(default=1, ge=1, le=100, description="Relative weight for query execution")

class PgbenchTestRequest(BaseModel):
    """Request model for pgbench test execution."""
    workspace_url: str
    instance_name: str
    database_name: str = "databricks_postgres"
    pgbench_config: PgbenchConfig
    queries: List[PgbenchQueryConfig]

class PgbenchExecutionResult(BaseModel):
    """Raw execution result from pgbench subprocess."""
    return_code: int
    stdout: str
    stderr: str
    execution_time_seconds: float

class PgbenchTestReport(BaseModel):
    """Comprehensive report of pgbench test results."""
    test_id: str
    test_start_time: float
    test_end_time: float
    total_duration_seconds: float
    pgbench_config: PgbenchConfig
    queries_tested: int
    tps: Optional[float] = Field(None, description="Transactions per second")
    average_latency_ms: Optional[float] = Field(None, description="Average latency in milliseconds")
    latency_stddev_ms: Optional[float] = Field(None, description="Latency standard deviation")
    latency_percentiles: Dict[str, float] = Field(default_factory=dict, description="Latency percentiles (p50, p95, p99)")
    per_statement_stats: Dict[str, Dict[str, float]] = Field(default_factory=dict)
    progress_reports: List[Dict[str, Any]] = Field(default_factory=list)
    execution_result: PgbenchExecutionResult
    recommendations: List[str] = Field(default_factory=list)
