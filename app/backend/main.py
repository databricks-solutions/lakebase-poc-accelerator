#!/usr/bin/env python3
"""
FastAPI backend for Databricks Lakebase Accelerator
Provides API endpoints for cost estimation, table generation, and configuration management.
"""

import os
import sys
import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from pydantic import BaseModel, Field
import asyncio
from typing import AsyncGenerator
import json
import yaml

# Import the cost estimator and table generator functions from services
from services.lakebase_cost_estimator import estimate_cost_from_config
from services.generate_synced_tables import generate_synced_tables_from_config

# Import concurrency testing modules
from models.query_models import ConcurrencyTestRequest, ConcurrencyTestReport, SimpleQueryConfig, PgbenchTestReport
from services.lakebase_connection_service import LakebaseConnectionService
from services.pgbench_service import PgbenchService
from services.oauth_service import DatabricksOAuthService
from services.query_executor import QueryExecutorService
from services.metrics_service import ConcurrencyMetricsService
from services.databricks_deployment_service import DatabricksDeploymentService, DeploymentProgress
from utils.parameter_parser import SimpleParameterParser

app = FastAPI(
    title="Databricks Lakebase Accelerator API",
    description="API for cost estimation, table generation, and workload configuration",
    version="1.0.0"
)

# Global progress tracker for deployment status
deployment_progress_tracker = {}

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint for Databricks Apps."""
    return {"status": "healthy", "service": "lakebase-accelerator-api"}

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models for request/response
class DatabaseInstanceConfig(BaseModel):
    bulk_writes_per_second: int = Field(..., description="Bulk writes for initial data loading")
    continuous_writes_per_second: int = Field(..., description="Continuous writes for real-time processing")
    reads_per_second: int = Field(..., description="Read operations per second")
    number_of_readable_secondaries: int = Field(0, description="Number of read replicas")
    readable_secondary_size_cu: int = Field(1, description="Size of read replicas in CU")
    promotion_percentage: float = Field(0.0, description="Promotion discount percentage")

class DatabaseStorageConfig(BaseModel):
    data_stored_gb: float = Field(..., description="Total data size in GB")
    estimated_data_deleted_daily_gb: float = Field(0, description="Daily data cleanup in GB")
    restore_windows_days: int = Field(0, description="Restore windows for data recovery")

class TableToSync(BaseModel):
    name: str = Field(..., description="Fully qualified table name")
    primary_keys: List[str] = Field(..., description="Primary key columns")
    scheduling_policy: str = Field("SNAPSHOT", description="Sync mode: SNAPSHOT, TRIGGERED, or CONTINUOUS")

class DeltaSynchronizationConfig(BaseModel):
    number_of_continuous_pipelines: int = Field(0, description="Number of continuous sync pipelines")
    expected_data_per_sync_gb: float = Field(0, description="Data volume per sync operation")
    sync_mode: str = Field("SNAPSHOT", description="Primary sync mode")
    sync_frequency: str = Field("Per day", description="Sync frequency description")
    tables_to_sync: List[TableToSync] = Field([], description="Tables to synchronize")

class WorkloadConfigRequest(BaseModel):
    database_instance: DatabaseInstanceConfig
    database_storage: DatabaseStorageConfig
    delta_synchronization: DeltaSynchronizationConfig
    databricks_workspace_url: str = Field(..., description="Databricks workspace URL")
    warehouse_http_path: str = Field(..., description="SQL warehouse HTTP path for table size calculation")
    lakebase_instance_name: str = Field("lakebase-accelerator-instance", description="Name for the Lakebase instance")
    uc_catalog_name: str = Field("lakebase-accelerator-catalog", description="Name for the UC catalog")
    database_name: str = Field("databricks_postgres", description="Name for the database")
    recommended_cu: int = Field(1, description="Recommended CU from cost estimation")

class CostEstimationRequest(BaseModel):
    workload_config: WorkloadConfigRequest

class DeploymentRequest(BaseModel):
    workload_config: WorkloadConfigRequest
    databricks_profile_name: Optional[str] = Field(None, description="Databricks profile name for authentication")
    tables: List[Dict[str, Any]] = Field(default_factory=list, description="Tables to sync")

@app.get("/")
async def root():
    return {"message": "Databricks Lakebase Accelerator API"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "lakebase-accelerator-api"}

@app.post("/api/estimate-cost")
async def estimate_cost(request: CostEstimationRequest):
    """
    Estimate Lakebase costs based on workload configuration.
    Automatically calculates actual table sizes from Databricks when tables are configured.
    """
    try:
        # Prepare workload data for the cost estimator
        workload_data = {
            'database_instance': request.workload_config.database_instance.model_dump(),
            'database_storage': request.workload_config.database_storage.model_dump(),
            'delta_synchronization': request.workload_config.delta_synchronization.model_dump()
        }

        # Call the cost estimator directly
        cost_report = estimate_cost_from_config(
            workload_data,
            warehouse_http_path=request.workload_config.warehouse_http_path,
            workspace_url=request.workload_config.databricks_workspace_url,
            profile=None  # Could be extended to accept profile from request if needed
        )
        
        # Transform the response to match frontend expectations
        transformed_response = {
            "timestamp": cost_report.get("timestamp"),
            "cost_breakdown": cost_report.get("cost_breakdown"),
            "recommendations": []  # Add recommendations if needed
        }
        
        # Transform table sizes data to match frontend expectations
        if "table_sizes" in cost_report and "error" not in cost_report["table_sizes"]:
            table_sizes_data = cost_report["table_sizes"]
            transformed_response["table_sizes"] = {
                "total_uncompressed_size_mb": table_sizes_data.get("total_size_mb", 0),
                "table_details": []
            }
            
            # Transform table details
            if "table_details" in table_sizes_data:
                for table_detail in table_sizes_data["table_details"]:
                    transformed_table = {
                        "table_name": table_detail.get("table_name", ""),
                        "uncompressed_size_mb": table_detail.get("size_mb", 0),
                        "row_count": table_detail.get("row_count", 0)
                    }
                    transformed_response["table_sizes"]["table_details"].append(transformed_table)
        
        return JSONResponse(content=transformed_response)

    except Exception as e:
        print(f"Error during cost estimation: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error during cost estimation: {str(e)}")



@app.post("/api/generate-databricks-config")
async def generate_databricks_config(request: WorkloadConfigRequest):
    """
    Generate databricks.yml configuration file.
    """
    try:
        # Extract workspace host from URL
        workspace_url = request.databricks_workspace_url.rstrip('/')
        if not workspace_url.startswith('https://'):
            workspace_url = f"https://{workspace_url}"

        databricks_config = {
            'bundle': {
                'name': 'lakebase_accelerator',
                'uuid': '5fec423c-234a-4869-a92b-766f76f6f70f'
            },
            'include': [
                'resources/*.yml',
                'resources/*/*.yml'
            ],
            'targets': {
                'dev': {
                    'mode': 'development',
                    'default': True,
                    'workspace': {
                        'host': workspace_url
                    }
                }
            }
        }

        return JSONResponse(content=databricks_config)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating databricks config: {str(e)}")

@app.post("/api/generate-lakebase-instance")
async def generate_lakebase_instance(request: WorkloadConfigRequest):
    """
    Generate lakebase instance configuration file.
    """
    database_instance_name = request.lakebase_instance_name.replace('-', '_')
    try:
        # Generate YAML content that matches the existing lakebase_instance.yml structure
        yaml_content = f"""resources:
#  Provision Lakebase database instance
#  https://docs.databricks.com/aws/en/dev-tools/bundles/resources#database_instances
  database_instances:
    {database_instance_name}:
      name: {request.lakebase_instance_name}
      capacity: CU_{request.recommended_cu}

  # Register Lakebase database as a read-only UC catalog to enable federated queries + data governance
  database_catalogs:
    {request.uc_catalog_name}:
      database_instance_name: ${{resources.database_instances.{database_instance_name}.name}} 
      name: {request.uc_catalog_name}
      database_name: {request.database_name}
      create_database_if_not_exists: true"""

        return JSONResponse(content={
            'yaml_content': yaml_content,
            'filename': 'lakebase_instance.yml',
            'description': 'Lakebase instance definition'
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating lakebase instance config: {str(e)}")

@app.post("/api/generate-synced-tables")
async def generate_synced_tables(request: WorkloadConfigRequest):
    """
    Generate synced tables configuration from workload config.
    """
    try:
        # Prepare workload data for the table generator
        workload_data = {
            'database_instance': request.database_instance.model_dump(),
            'database_storage': request.database_storage.model_dump(),
            'delta_synchronization': request.delta_synchronization.model_dump(),
            'uc_catalog_name': request.uc_catalog_name,
            'lakebase_instance_name': request.lakebase_instance_name,
            'database_name': request.database_name
        }
        
        # Call the table generator directly
        synced_tables_config = generate_synced_tables_from_config(workload_data)
        
        return JSONResponse(content=synced_tables_config)

    except Exception as e:
        print(f"Error generating synced tables: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error generating synced tables: {str(e)}")

@app.post("/api/deploy")
async def deploy_to_databricks(request: DeploymentRequest):
    """
    Deploy Lakebase instance directly using Databricks SDK.
    Creates database instance, catalog, and synced tables.
    """
    try:
        import uuid

        # Generate unique deployment ID
        deployment_id = str(uuid.uuid4())

        # Initialize deployment service
        deployment_service = DatabricksDeploymentService()

        # Set up progress tracking
        def progress_callback(progress: DeploymentProgress):
            deployment_progress_tracker[deployment_id] = progress.__dict__

        deployment_service.set_progress_callback(progress_callback)

        # Convert request to config dictionary
        config = {
            'databricks_workspace_url': request.workload_config.databricks_workspace_url,
            'lakebase_instance_name': request.workload_config.lakebase_instance_name,
            'database_name': request.workload_config.database_name,
            'uc_catalog_name': request.workload_config.uc_catalog_name,
            'recommended_cu': request.workload_config.recommended_cu,
            'workload_description': f"OLTP workload with {request.workload_config.database_instance.bulk_writes_per_second} bulk writes/sec, {request.workload_config.database_instance.reads_per_second} reads/sec",
            'tables': request.tables
        }

        # Deploy the instance
        result = await deployment_service.deploy_lakebase_instance(
            config=config,
            profile=request.databricks_profile_name
        )

        # Add deployment ID to result
        result['deployment_id'] = deployment_id

        return result

    except Exception as e:
        logger.error(f"Deployment failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Deployment failed: {str(e)}")

@app.get("/api/deploy/progress/{deployment_id}")
async def get_deployment_progress(deployment_id: str):
    """
    Get current deployment progress for a specific deployment ID
    """
    if deployment_id not in deployment_progress_tracker:
        raise HTTPException(status_code=404, detail="Deployment not found")

    return deployment_progress_tracker[deployment_id]

@app.get("/api/deploy/progress/{deployment_id}/stream")
async def stream_deployment_progress(deployment_id: str):
    """
    Stream deployment progress updates using Server-Sent Events
    """
    async def event_generator():
        last_status = None
        while True:
            if deployment_id in deployment_progress_tracker:
                current_progress = deployment_progress_tracker[deployment_id]
                current_status = current_progress.get('status', 'pending')

                # Send update if status changed or if still in progress
                if current_status != last_status or current_status == 'in_progress':
                    yield f"data: {json.dumps(current_progress)}\n\n"
                    last_status = current_status

                # Stop streaming if deployment is completed or failed
                if current_status in ['completed', 'failed']:
                    break
            else:
                yield f"data: {json.dumps({'status': 'pending', 'message': 'Deployment not started'})}\n\n"

            await asyncio.sleep(1)  # Check every second

    return StreamingResponse(
        event_generator(),
        media_type="text/plain",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "text/event-stream"
        }
    )

# Concurrency Testing Endpoints

@app.post("/api/concurrency-test/validate-query")
async def validate_query(query: str):
    """
    Validate a SQL query for concurrency testing.
    
    Args:
        query: SQL query string to validate
        
    Returns:
        Validation result with parameter count and safety check
    """
    try:
        # Use parameter parser to validate query
        validation_result = SimpleParameterParser.validate_query_format(query)
        
        # Additional safety check
        query_executor = QueryExecutorService()
        is_safe, safety_message = query_executor.validate_query_safety(query)
        
        return {
            "is_valid": validation_result["is_valid"] and is_safe,
            "parameter_count": validation_result["parameter_count"],
            "error_message": validation_result["error_message"] or safety_message,
            "query_type": query_executor.get_query_type(query)
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Query validation failed: {str(e)}")

@app.post("/api/concurrency-test/validate-instance")
async def validate_lakebase_instance(workspace_url: str, instance_name: str):
    """
    Validate access to a Lakebase instance.
    
    Args:
        workspace_url: Databricks workspace URL
        instance_name: Lakebase instance name
        
    Returns:
        Validation result
    """
    try:
        oauth_service = DatabricksOAuthService()
        has_access = await oauth_service.validate_instance_access(workspace_url, instance_name)
        
        return {
            "has_access": has_access,
            "workspace_url": workspace_url,
            "instance_name": instance_name
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Instance validation failed: {str(e)}")

@app.post("/api/concurrency-test/execute")
async def execute_concurrency_test(test_request: ConcurrencyTestRequest):
    """
    Execute concurrency test against Lakebase instance.
    
    Args:
        test_request: Complete test configuration including queries and parameters
        
    Returns:
        ConcurrencyTestReport with test results and metrics
    """
    try:
        # Initialize connection service
        connection_service = LakebaseConnectionService()
        
        # Initialize connection pool
        pool_initialized = await connection_service.initialize_connection_pool(
            workspace_url=test_request.workspace_url,
            instance_name=test_request.instance_name,
            database=test_request.database_name,
            pool_config=test_request.connection_pool_config
        )
        
        if not pool_initialized:
            raise HTTPException(status_code=500, detail="Failed to initialize connection pool")
        
        # Convert simple query configs to execution format
        execution_queries = []
        for query_config in test_request.queries:
            execution_query = {
                "query_identifier": query_config.query_identifier,
                "query_content": query_config.query_content,
                "test_scenarios": [
                    {
                        "name": scenario.name,
                        "parameters": scenario.parameters,
                        "execution_count": scenario.execution_count
                    }
                    for scenario in query_config.test_scenarios
                ]
            }
            execution_queries.append(execution_query)
        
        # Execute concurrent queries
        report = await connection_service.execute_concurrent_queries(
            queries=execution_queries,
            concurrency_level=test_request.concurrency_level
        )
        
        # Clean up connection pool
        connection_service.close_connection_pool()
        
        return report
        
    except Exception as e:
        # Ensure connection pool is closed on error
        try:
            connection_service.close_connection_pool()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Concurrency test failed: {str(e)}")

@app.post("/api/concurrency-test/upload-query")
async def upload_query_file(file: UploadFile = File(...)):
    """
    Upload and parse a SQL query file, saving it to app/queries/ folder.
    
    Args:
        file: SQL file to upload
        
    Returns:
        Parsed query information
    """
    try:
        # Validate file type
        if not file.filename.endswith('.sql'):
            raise HTTPException(status_code=400, detail="File must be a .sql file")
        
        # Read file content
        content = await file.read()
        query_content = content.decode('utf-8')
        
        # Parse query
        query_identifier = file.filename.replace('.sql', '')
        validation_result = SimpleParameterParser.validate_query_format(query_content)
        
        # Save file to app/queries/ folder
        queries_dir = Path(__file__).parent.parent / "queries"
        queries_dir.mkdir(exist_ok=True)
        
        file_path = queries_dir / file.filename
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(query_content)
        
        return {
            "query_identifier": query_identifier,
            "query_content": query_content,
            "parameter_count": validation_result["parameter_count"],
            "is_valid": validation_result["is_valid"],
            "error_message": validation_result["error_message"],
            "saved_path": str(file_path)
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"File upload failed: {str(e)}")

@app.post("/api/concurrency-test/run-uploaded-tests")
async def run_uploaded_tests(test_request: dict):
    """
    Run concurrency tests using uploaded SQL files from app/queries/ folder.
    
    Args:
        test_request: Test configuration with databricks_profile, instance_name, database_name, concurrency_level
        
    Returns:
        ConcurrencyTestReport with test results and metrics
    """
    try:
        # Debug: Log the incoming request
        print(f"🔍 Incoming test_request: {test_request}")
        
        # Extract test configuration
        databricks_profile = test_request.get("databricks_profile", "DEFAULT")
        workspace_url = test_request.get("workspace_url")
        instance_name = test_request.get("instance_name")
        database_name = test_request.get("database_name", "databricks_postgres")
        concurrency_level = test_request.get("concurrency_level", 10)

        print(f"🔍 Extracted values:")
        print(f"   databricks_profile: {databricks_profile}")
        print(f"   workspace_url: {workspace_url}")
        print(f"   instance_name: {instance_name}")
        print(f"   database_name: {database_name}")
        print(f"   concurrency_level: {concurrency_level}")

        if not instance_name:
            raise HTTPException(status_code=400, detail="instance_name is required")

        if not workspace_url:
            raise HTTPException(status_code=400, detail="workspace_url is required. Please provide your Databricks workspace URL.")
        
        # Get all SQL files from app/queries/ folder
        queries_dir = Path(__file__).parent.parent / "queries"
        if not queries_dir.exists():
            raise HTTPException(status_code=404, detail="No queries folder found")
        
        sql_files = list(queries_dir.glob("*.sql"))
        if not sql_files:
            raise HTTPException(status_code=404, detail="No SQL files found in queries folder")
        
        # Parse each SQL file and prepare queries
        execution_queries = []
        for sql_file in sql_files:
            try:
                # Read and parse SQL file
                content = sql_file.read_text(encoding='utf-8')
                lines = content.split('\n')
                
                sql_lines = []
                parameter_sets = []
                exec_count = 5  # Default
                
                for line in lines:
                    line = line.strip()
                    if line.startswith('-- PARAMETERS:'):
                        json_str = line.replace('-- PARAMETERS:', '').strip()
                        try:
                            parameter_sets = json.loads(json_str)
                        except json.JSONDecodeError:
                            parameter_sets = []
                    elif line.startswith('-- EXEC_COUNT:'):
                        exec_count = int(line.replace('-- EXEC_COUNT:', '').strip())
                    elif not line.startswith('--'):
                        sql_lines.append(line)
                
                sql_content = '\n'.join(sql_lines).strip()
                
                # Create test scenarios for each parameter set
                test_scenarios = []
                if parameter_sets:
                    for i, params in enumerate(parameter_sets, 1):
                        test_scenarios.append({
                            "name": f"scenario_{i}",
                            "parameters": params,
                            "execution_count": exec_count,
                        })
                else:
                    # No parameters - single scenario
                    test_scenarios.append({
                        "name": "scenario_1", 
                        "parameters": [],
                        "execution_count": exec_count,
                    })
                
                execution_queries.append({
                    "query_identifier": sql_file.stem,
                    "query_content": sql_content,
                    "test_scenarios": test_scenarios
                })
                
            except Exception as e:
                print(f"Error parsing {sql_file.name}: {e}")
                continue
        
        if not execution_queries:
            raise HTTPException(status_code=400, detail="No valid queries found")
        
        # Initialize connection service
        connection_service = LakebaseConnectionService()
        
        # Initialize connection pool
        pool_config = {
            "base_pool_size": max(1, concurrency_level // 4),
            "max_overflow": concurrency_level,
            "pool_timeout": 30,
            "pool_recycle": 3600,
            "command_timeout": 30,
            "ssl_mode": "require"
        }
        
        try:
            pool_initialized = await connection_service.initialize_connection_pool(
                workspace_url=workspace_url,
                instance_name=instance_name,
                database=database_name,
                pool_config=pool_config,
                profile=databricks_profile
            )
            
            if not pool_initialized:
                raise HTTPException(status_code=500, detail="Failed to initialize connection pool")
        except Exception as e:
            # Provide more specific error details
            error_message = str(e)
            if "Resource not found" in error_message:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Lakebase instance '{instance_name}' not found or not accessible with profile '{databricks_profile}'. Please verify the instance name and your Databricks profile configuration."
                )
            elif "profile" in error_message.lower():
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid Databricks profile '{databricks_profile}'. Please check your Databricks CLI configuration."
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail=f"Connection initialization failed: {error_message}"
                )
        
        # Execute concurrent queries
        report = await connection_service.execute_concurrent_queries(
            queries=execution_queries,
            concurrency_level=concurrency_level
        )
        
        # Clean up connection pool
        connection_service.close_connection_pool()
        
        return report
        
    except Exception as e:
        # Ensure connection pool is closed on error
        try:
            connection_service.close_connection_pool()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Test execution failed: {str(e)}")

@app.get("/api/concurrency-test/status")
async def get_concurrency_test_status():
    """
    Get the current status of the concurrency testing service.

    Returns:
        Service status information
    """
    try:
        connection_service = LakebaseConnectionService()
        pool_status = connection_service.get_pool_status()

        return {
            "service_status": "running",
            "connection_pool": pool_status,
            "available_endpoints": [
                "/api/concurrency-test/validate-query",
                "/api/concurrency-test/validate-instance",
                "/api/concurrency-test/execute",
                "/api/concurrency-test/upload-query",
                "/api/concurrency-test/run-uploaded-tests"
            ]
        }

    except Exception as e:
        return {
            "service_status": "error",
            "error": str(e)
        }

# pgbench Testing Endpoints

@app.delete("/api/pgbench-test/delete-query")
async def delete_pgbench_query_file(request: dict):
    """
    Delete a pgbench query file from the app/queries/ folder.

    Args:
        request: Dictionary containing file_path

    Returns:
        Success message
    """
    try:
        file_path = request.get('file_path')
        if not file_path:
            raise HTTPException(status_code=400, detail="file_path is required")

        # Ensure the file is in the queries directory for security
        queries_dir = Path(__file__).parent.parent / "queries"
        file_to_delete = Path(file_path)

        # Security check: ensure the file is within the queries directory
        if not str(file_to_delete).startswith(str(queries_dir)):
            raise HTTPException(status_code=400, detail="Invalid file path")

        if file_to_delete.exists():
            file_to_delete.unlink()
            return {"message": f"File {file_to_delete.name} deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="File not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File deletion failed: {str(e)}")

@app.post("/api/pgbench-test/upload-query")
async def upload_pgbench_query_file(file: UploadFile = File(...)):
    """
    Upload and save a pgbench-format SQL query file to app/queries/ folder.

    Args:
        file: SQL file in pgbench format to upload

    Returns:
        Parsed query information
    """
    try:
        # Validate file type
        if not file.filename.endswith('.sql'):
            raise HTTPException(status_code=400, detail="File must be a .sql file")

        # Read file content
        content = await file.read()
        query_content = content.decode('utf-8')

        # Parse pgbench query (simple validation)
        query_identifier = file.filename.replace('.sql', '')

        # Count pgbench variables (lines starting with \set)
        variable_count = len([line for line in query_content.split('\n') if line.strip().startswith('\\set')])

        # Save file to app/queries/ folder
        queries_dir = Path(__file__).parent.parent / "queries"
        queries_dir.mkdir(exist_ok=True)

        file_path = queries_dir / file.filename
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(query_content)

        return {
            "query_identifier": query_identifier,
            "query_content": query_content,
            "variable_count": variable_count,
            "is_valid": True,
            "saved_path": str(file_path),
            "format": "pgbench"
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"File upload failed: {str(e)}")

@app.post("/api/pgbench-test/run-uploaded-tests")
async def run_pgbench_uploaded_tests(test_request: dict):
    """
    Run pgbench tests using uploaded SQL files from app/queries/ folder.

    Args:
        test_request: pgbench test configuration with connection and benchmark settings

    Returns:
        PgbenchTestReport with test results and metrics
    """
    try:
        # Extract test configuration
        databricks_profile = test_request.get("databricks_profile", "DEFAULT")
        workspace_url = test_request.get("workspace_url")
        instance_name = test_request.get("instance_name")
        database_name = test_request.get("database_name", "databricks_postgres")

        # Extract pgbench configuration
        pgbench_config = {
            "clients": test_request.get("pgbench_clients", 8),
            "jobs": test_request.get("pgbench_jobs", 8),
            "duration_seconds": test_request.get("pgbench_duration", 30),
            "progress_interval": test_request.get("pgbench_progress_interval", 5),
            "protocol": test_request.get("pgbench_protocol", "prepared"),
            "per_statement_latency": test_request.get("pgbench_per_statement_latency", True),
            "detailed_logging": test_request.get("pgbench_detailed_logging", True),
            "connect_per_transaction": test_request.get("pgbench_connect_per_transaction", False)
        }

        if not instance_name:
            raise HTTPException(status_code=400, detail="instance_name is required")

        if not workspace_url:
            raise HTTPException(status_code=400, detail="workspace_url is required")

        # Get all SQL files from app/queries/ folder
        queries_dir = Path(__file__).parent.parent / "queries"
        if not queries_dir.exists():
            raise HTTPException(status_code=404, detail="No queries folder found")

        sql_files = list(queries_dir.glob("*.sql"))
        if not sql_files:
            raise HTTPException(status_code=404, detail="No SQL files found in queries folder")

        # Prepare queries for pgbench
        queries_with_weights = []
        for sql_file in sql_files:
            try:
                content = sql_file.read_text(encoding='utf-8')
                queries_with_weights.append({
                    "query_identifier": sql_file.stem,
                    "query_content": content,
                    "weight": 1  # Equal weight for all queries
                })
            except Exception as e:
                print(f"Error reading {sql_file.name}: {e}")
                continue

        if not queries_with_weights:
            raise HTTPException(status_code=400, detail="No valid queries found")

        # Initialize pgbench service
        pgbench_service = PgbenchService()

        try:
            # Initialize connection
            connection_initialized = await pgbench_service.initialize_connection(
                workspace_url=workspace_url,
                instance_name=instance_name,
                database=database_name,
                profile=databricks_profile
            )

            if not connection_initialized:
                raise HTTPException(status_code=500, detail="Failed to initialize pgbench connection")
        except Exception as e:
            error_message = str(e)
            if "Resource not found" in error_message:
                raise HTTPException(
                    status_code=400,
                    detail=f"Lakebase instance '{instance_name}' not found or not accessible with profile '{databricks_profile}'"
                )
            elif "profile" in error_message.lower():
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid Databricks profile '{databricks_profile}'"
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail=f"Connection initialization failed: {error_message}"
                )

        # Execute pgbench test
        report = await pgbench_service.execute_pgbench_test(
            queries=queries_with_weights,
            pgbench_config=pgbench_config
        )

        return report

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"pgbench test execution failed: {str(e)}")

@app.get("/api/pgbench-test/status")
async def get_pgbench_test_status():
    """
    Get the current status of the pgbench testing service.

    Returns:
        Service status information
    """
    try:
        return {
            "service_status": "running",
            "pgbench_available": True,
            "available_endpoints": [
                "/api/pgbench-test/upload-query",
                "/api/pgbench-test/run-uploaded-tests"
            ]
        }

    except Exception as e:
        return {
            "service_status": "error",
            "error": str(e)
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)