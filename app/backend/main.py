#!/usr/bin/env python3
"""
FastAPI backend for Databricks Lakebase Accelerator
Provides API endpoints for cost estimation, table generation, and configuration management.
"""

import os
import sys
import json
import logging
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

# Initialize logger
logger = logging.getLogger(__name__)

# Import the cost estimator and table generator functions from services
from services.lakebase_cost_estimator import estimate_cost_from_config
from services.generate_synced_tables import generate_synced_tables_from_config, generate_synced_tables_yaml_from_config

# Import concurrency testing modules
from models.query_models import ConcurrencyTestRequest, ConcurrencyTestReport, SimpleQueryConfig, PgbenchTestReport
from services.lakebase_connection_service import LakebaseConnectionService
from services.pgbench_service import PgbenchService
from services.oauth_service import DatabricksOAuthService
from services.query_executor import QueryExecutorService
from services.metrics_service import ConcurrencyMetricsService
from services.databricks_deployment_service import DatabricksDeploymentService, DeploymentProgress
from services.databricks_jobs_service import DatabricksJobsService
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
    storage_catalog: str = Field("main", description="Unity Catalog for storing synced table data during processing")
    storage_schema: str = Field("default", description="Schema within storage catalog for synced table data")
    recommended_cu: int = Field(1, description="Recommended CU from cost estimation")
    databricks_profile_name: Optional[str] = Field(None, description="Databricks profile name for authentication (localhost only)")

class CostEstimationRequest(BaseModel):
    workload_config: WorkloadConfigRequest

class DeploymentRequest(BaseModel):
    workload_config: WorkloadConfigRequest
    databricks_profile_name: Optional[str] = Field(None, description="Databricks profile name for authentication")
    tables: List[Dict[str, Any]] = Field(default_factory=list, description="Tables to sync")

@app.get("/api")
async def api_root():
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

        # Convert to YAML string
        yaml_content = yaml.dump(databricks_config, default_flow_style=False, sort_keys=False, indent=2)

        return JSONResponse(content={
            'yaml_content': yaml_content,
            'filename': 'databricks.yml',
            'description': 'Main bundle configuration'
        })

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
            'database_name': request.database_name,
            'storage_catalog': request.storage_catalog,
            'storage_schema': request.storage_schema
        }
        
        # Call the table generator to get YAML string
        yaml_content = generate_synced_tables_yaml_from_config(workload_data)
        
        # Also get the dictionary version for the deploy flow
        synced_tables_config = generate_synced_tables_from_config(workload_data)
        
        return JSONResponse(content={
            'yaml_content': yaml_content,
            'filename': 'synced_delta_tables.yml',
            'description': 'Table sync configurations',
            'config_data': synced_tables_config  # Keep for deploy flow
        })

    except Exception as e:
        print(f"Error generating synced tables: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error generating synced tables: {str(e)}")

@app.post("/api/deploy")
async def deploy_to_databricks(request: DeploymentRequest):
    """
    Deploy Lakebase instance directly using Databricks SDK.
    Returns immediately with deployment_id, deployment runs in background.
    """
    try:
        import uuid
        
        # Log deployment request details
        logger.info("=" * 80)
        logger.info("DEPLOYMENT API - REQUEST RECEIVED")
        logger.info("=" * 80)
        logger.info(f"Deployment request received for:")
        logger.info(f"  - Lakebase instance: {request.workload_config.lakebase_instance_name}")
        logger.info(f"  - UC catalog: {request.workload_config.uc_catalog_name}")
        logger.info(f"  - Database: {request.workload_config.database_name}")
        logger.info(f"  - Recommended CU: {request.workload_config.recommended_cu}")
        logger.info(f"  - Workspace URL: {request.workload_config.databricks_workspace_url}")
        logger.info(f"  - Profile: {request.databricks_profile_name}")
        logger.info(f"  - Tables to sync: {len(request.workload_config.delta_synchronization.tables_to_sync)}")
        logger.info("=" * 80)

        # Generate unique deployment ID
        deployment_id = str(uuid.uuid4())

        # Initialize progress tracker with pending status
        deployment_progress_tracker[deployment_id] = {
            'status': 'pending',
            'message': 'Deployment starting...',
            'current_step': 0,
            'total_steps': 5,
            'steps': [
                {'name': 'Initialize', 'status': 'pending'},
                {'name': 'Database Instance', 'status': 'pending'},
                {'name': 'Database Catalog', 'status': 'pending'},
                {'name': 'Synced Tables', 'status': 'pending'},
                {'name': 'Finalize', 'status': 'pending'}
            ]
        }

        # Start deployment in background task
        async def run_deployment():
            try:
                # Initialize deployment service
                deployment_service = DatabricksDeploymentService()

                # Set up progress tracking
                def progress_callback(progress: DeploymentProgress):
                    # Create JSON-serializable progress dict (avoid circular references)
                    deployment_progress_tracker[deployment_id] = {
                        'status': progress.status,
                        'message': progress.steps[progress.current_step]['details'] if progress.current_step < len(progress.steps) else 'Processing...',
                        'current_step': progress.current_step,
                        'total_steps': progress.total_steps,
                        'error_message': progress.error_message,
                        'steps': [
                            {
                                'name': step.get('description', ''),
                                'status': step.get('status', 'pending'),
                                'details': step.get('details', ''),
                                'error': step.get('error', None)
                            }
                            for step in progress.steps
                        ]
                    }
            
                deployment_service.set_progress_callback(progress_callback)

                # Convert request to config dictionary
                config = {
                    'databricks_workspace_url': request.workload_config.databricks_workspace_url,
                    'lakebase_instance_name': request.workload_config.lakebase_instance_name,
                    'database_name': request.workload_config.database_name,
                    'uc_catalog_name': request.workload_config.uc_catalog_name,
                    'storage_catalog': request.workload_config.storage_catalog,
                    'storage_schema': request.workload_config.storage_schema,
                    'recommended_cu': request.workload_config.recommended_cu,
                    'workload_description': f"OLTP workload with {request.workload_config.database_instance.bulk_writes_per_second} bulk writes/sec, {request.workload_config.database_instance.reads_per_second} reads/sec",
                    'tables': request.tables
                }
                
                logger.info(f"DEBUG - Config being passed to deployment service:")
                logger.info(f"  - recommended_cu: {config['recommended_cu']}")
                logger.info(f"  - Full config keys: {list(config.keys())}")

                # Deploy the instance
                result = await deployment_service.deploy_lakebase_instance(
                    config=config,
                    profile=request.databricks_profile_name
                )

                # Update progress tracker with final result (JSON-serializable only)
                if deployment_id in deployment_progress_tracker:
                    # Check if deployment was successful
                    is_successful = result.get('success', False)
                    deployment_progress_tracker[deployment_id].update({
                        'status': 'completed' if is_successful else 'failed',
                        'message': result.get('message', 'Deployment completed' if is_successful else 'Deployment failed'),
                        'error_message': None if is_successful else result.get('message'),
                        'result': {
                            'success': result.get('success'),
                            'message': result.get('message'),
                            'instance': result.get('instance'),
                            'catalog': result.get('catalog'),
                            'tables': result.get('tables')
                        }
                    })

            except Exception as e:
                logger.error(f"Background deployment failed: {str(e)}", exc_info=True)
                # Update progress tracker with error
                if deployment_id in deployment_progress_tracker:
                    deployment_progress_tracker[deployment_id].update({
                        'status': 'failed',
                        'error_message': str(e)
                    })

        # Create background task (non-blocking)
        asyncio.create_task(run_deployment())

        # Return immediately with deployment ID
        return {
            'success': True,
            'deployment_id': deployment_id,
            'message': 'Deployment started. Poll /api/deploy/progress/{deployment_id} for status.'
        }

    except Exception as e:
        logger.error(f"Failed to start deployment: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to start deployment: {str(e)}")

@app.get("/api/deploy/progress/{deployment_id}")
async def get_deployment_progress(deployment_id: str):
    """
    Get current deployment progress for a specific deployment ID
    """
    try:
        if deployment_id not in deployment_progress_tracker:
            # Return a minimal pending status instead of 404 for better UX
            return {
                'status': 'pending',
                'message': 'Deployment initializing...',
                'current_step': 0,
                'total_steps': 5,
                'steps': []
            }

        # Return current progress
        progress = deployment_progress_tracker[deployment_id]
        
        # Ensure response is JSON-serializable
        return {
            'status': progress.get('status', 'pending'),
            'message': progress.get('message', 'Processing...'),
            'current_step': progress.get('current_step', 0),
            'total_steps': progress.get('total_steps', 5),
            'error_message': progress.get('error_message'),
            'steps': progress.get('steps', []),
            'result': progress.get('result')
        }
    except Exception as e:
        logger.error(f"Error getting deployment progress: {str(e)}")
        # Return error status instead of crashing
        return {
            'status': 'error',
            'message': f'Error retrieving progress: {str(e)}',
            'current_step': 0,
            'total_steps': 5,
            'steps': []
        }

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
    Upload and parse a SQL query file, saving it to app/queries_psycopg/ folder.
    
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
        
        # Extract parameter sets from comments
        parameter_sets = []
        lines = query_content.split('\n')
        for line in lines:
            line = line.strip()
            if line.startswith('-- PARAMETERS:'):
                json_str = line.replace('-- PARAMETERS:', '').strip()
                try:
                    parameter_sets = json.loads(json_str)
                except json.JSONDecodeError:
                    parameter_sets = []
                break
        
        # Save file to temp directory for security (separate from pgbench)
        import tempfile
        temp_dir = Path(tempfile.gettempdir()) / "lakebase_psycopg_queries"
        temp_dir.mkdir(exist_ok=True)
        
        file_path = temp_dir / file.filename
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(query_content)
        
        return {
            "query_identifier": query_identifier,
            "query_content": query_content,
            "parameter_count": len(parameter_sets),  # Show parameter set count instead of parameter count
            "is_valid": validation_result["is_valid"],
            "error_message": validation_result["error_message"],
            "saved_path": str(file_path)
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"File upload failed: {str(e)}")

@app.post("/api/concurrency-test/run-uploaded-tests")
async def run_uploaded_tests(test_request: dict):
    """
    Run concurrency tests using uploaded SQL files from app/queries_psycopg/ folder.
    
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
        
        # Get all SQL files from temp directory (separate from pgbench)
        import tempfile
        temp_dir = Path(tempfile.gettempdir()) / "lakebase_psycopg_queries"
        if not temp_dir.exists():
            raise HTTPException(status_code=404, detail="No concurrency queries folder found")
        
        sql_files = list(temp_dir.glob("*.sql"))
        if not sql_files:
            raise HTTPException(status_code=404, detail="No SQL files found in concurrency queries folder")
        
        print(f"🔍 Found {len(sql_files)} SQL files in temp directory:")
        for sql_file in sql_files:
            print(f"   - {sql_file.name}")
        
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

@app.post("/api/concurrency-test/run-predefined-tests")
async def run_predefined_tests(test_request: dict):
    """
    Run concurrency tests using predefined queries.
    
    Args:
        test_request: Test configuration with databricks_profile, instance_name, database_name, concurrency_level, query_configs
        
    Returns:
        ConcurrencyTestReport with test results and metrics
    """
    try:
        # Debug: Log the incoming request
        print(f"🔍 Incoming predefined test_request: {test_request}")
        
        # Extract test configuration
        databricks_profile = test_request.get("databricks_profile", "DEFAULT")
        workspace_url = test_request.get("workspace_url")
        instance_name = test_request.get("instance_name")
        database_name = test_request.get("database_name", "databricks_postgres")
        concurrency_level = test_request.get("concurrency_level", 10)
        query_configs = test_request.get("query_configs", [])

        print(f"🔍 Extracted values:")
        print(f"   databricks_profile: {databricks_profile}")
        print(f"   workspace_url: {workspace_url}")
        print(f"   instance_name: {instance_name}")
        print(f"   database_name: {database_name}")
        print(f"   concurrency_level: {concurrency_level}")
        print(f"   query_configs count: {len(query_configs)}")

        if not instance_name:
            raise HTTPException(status_code=400, detail="instance_name is required")

        if not workspace_url:
            raise HTTPException(status_code=400, detail="workspace_url is required. Please provide your Databricks workspace URL.")

        if not query_configs:
            raise HTTPException(status_code=400, detail="No predefined queries provided")
        
        # Parse each predefined query and prepare execution queries
        execution_queries = []
        for query_config in query_configs:
            try:
                query_content = query_config.get("content", "")
                query_name = query_config.get("name", "unnamed_query")
                
                # Parse query content to extract parameters and exec count
                lines = query_content.split('\n')
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
                    "query_identifier": query_name,
                    "query_content": query_content,
                    "test_scenarios": test_scenarios
                })
                
            except Exception as e:
                print(f"Error parsing predefined query {query_config.get('name', 'unknown')}: {e}")
                continue
        
        if not execution_queries:
            raise HTTPException(status_code=400, detail="No valid predefined queries found")
        
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
        raise HTTPException(status_code=500, detail=f"Predefined test execution failed: {str(e)}")

@app.post("/api/concurrency-test/clear-temp-files")
async def clear_temp_files():
    """
    Clear all temporary SQL files from the concurrency testing temp directory.
    
    Returns:
        Success message
    """
    try:
        import tempfile
        import shutil
        
        temp_dir = Path(tempfile.gettempdir()) / "lakebase_psycopg_queries"
        
        if temp_dir.exists():
            # Remove all .sql files
            for sql_file in temp_dir.glob("*.sql"):
                sql_file.unlink()
                print(f"Deleted temp file: {sql_file}")
            
            return {"message": "Temporary files cleared successfully", "files_cleared": True}
        else:
            return {"message": "No temp directory found", "files_cleared": False}
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to clear temp files: {str(e)}")

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

        # Ensure the file is in the temp queries directory for security
        import tempfile
        temp_dir = Path(tempfile.gettempdir()) / "lakebase_queries"
        file_to_delete = Path(file_path)

        # Security check: ensure the file is within the temp queries directory
        if not str(file_to_delete).startswith(str(temp_dir)):
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

        # Save file to temporary storage (Databricks Apps can't write to workspace directly)
        import tempfile
        import os
        
        # Use system temp directory
        temp_dir = Path(tempfile.gettempdir()) / "lakebase_queries"
        temp_dir.mkdir(exist_ok=True)
        
        file_path = temp_dir / file.filename
        print(f"DEBUG: Saving to temp path: {file_path}")
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(query_content)
        
        print(f"DEBUG: File saved successfully to {file_path}")

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

        # Get all SQL files from temp directory
        import tempfile
        temp_dir = Path(tempfile.gettempdir()) / "lakebase_queries"
        print(f"DEBUG: Looking for queries in temp_dir: {temp_dir}")
        print(f"DEBUG: temp_dir exists: {temp_dir.exists()}")
        
        if not temp_dir.exists():
            raise HTTPException(status_code=404, detail="No queries folder found")

        sql_files = list(temp_dir.glob("*.sql"))
        print(f"DEBUG: Found {len(sql_files)} SQL files: {[f.name for f in sql_files]}")
        
        if not sql_files:
            raise HTTPException(status_code=404, detail="No SQL files found in queries folder")

        # Prepare queries for pgbench with weight parsing
        queries_with_weights = []
        for sql_file in sql_files:
            try:
                content = sql_file.read_text(encoding='utf-8')
                
                # Parse weight from comments in the SQL file
                weight = 1  # Default weight
                lines = content.split('\n')
                for line in lines:
                    line = line.strip()
                    if line.startswith('-- weight:'):
                        try:
                            weight_str = line.replace('-- weight:', '').strip()
                            weight = int(weight_str)
                            print(f"DEBUG: Found weight {weight} in {sql_file.name}")
                            break
                        except ValueError:
                            print(f"Warning: Invalid weight format in {sql_file.name}: {line}")
                            weight = 1
                            break
                
                queries_with_weights.append({
                    "query_identifier": sql_file.stem,
                    "query_content": content,
                    "weight": weight
                })
                
                print(f"DEBUG: Added uploaded query '{sql_file.stem}' with weight {weight}")
                
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

@app.post("/api/pgbench-test/run-predefined-tests")
async def run_pgbench_predefined_tests(test_request: dict):
    """
    Run pgbench tests using predefined queries with weights.

    Args:
        test_request: pgbench test configuration with connection, benchmark settings, and predefined queries

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

        # Extract predefined queries
        predefined_queries = test_request.get("predefined_queries", [])

        if not instance_name:
            raise HTTPException(status_code=400, detail="instance_name is required")

        if not workspace_url:
            raise HTTPException(status_code=400, detail="workspace_url is required")

        if not predefined_queries:
            raise HTTPException(status_code=400, detail="No predefined queries provided")

        # Prepare queries for pgbench with weights
        queries_with_weights = []
        for query_config in predefined_queries:
            try:
                query_name = query_config.get("name", "unnamed_query")
                query_content = query_config.get("content", "")
                weight = query_config.get("weight", 1)  # Default weight is 1

                if not query_content.strip():
                    print(f"Warning: Skipping empty query: {query_name}")
                    continue

                queries_with_weights.append({
                    "query_identifier": query_name,
                    "query_content": query_content,
                    "weight": weight
                })
                
                print(f"DEBUG: Added predefined query '{query_name}' with weight {weight}")
                
            except Exception as e:
                print(f"Error processing predefined query: {e}")
                continue

        if not queries_with_weights:
            raise HTTPException(status_code=400, detail="No valid predefined queries found")

        print(f"DEBUG: Processing {len(queries_with_weights)} predefined queries for pgbench")

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
        raise HTTPException(status_code=500, detail=f"pgbench predefined test execution failed: {str(e)}")

@app.post("/api/pgbench-test/clear-temp-files")
async def clear_pgbench_temp_files():
    """
    Clear all temporary SQL files from the pgbench testing temp directory.
    
    Returns:
        Success message
    """
    try:
        import tempfile
        import shutil
        
        temp_dir = Path(tempfile.gettempdir()) / "lakebase_queries"
        
        if temp_dir.exists():
            # Remove all .sql files
            sql_files = list(temp_dir.glob("*.sql"))
            for sql_file in sql_files:
                try:
                    sql_file.unlink()
                    print(f"DEBUG: Deleted {sql_file}")
                except Exception as e:
                    print(f"Warning: Could not delete {sql_file}: {e}")
            
            print(f"DEBUG: Cleared {len(sql_files)} SQL files from {temp_dir}")
            return {"message": f"Cleared {len(sql_files)} temporary SQL files"}
        else:
            print(f"DEBUG: Temp directory {temp_dir} does not exist")
            return {"message": "No temporary files to clear"}
            
    except Exception as e:
        print(f"Error clearing temp files: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to clear temporary files: {str(e)}")

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
                "/api/pgbench-test/run-uploaded-tests",
                "/api/pgbench-test/run-predefined-tests",
                "/api/pgbench-test/clear-temp-files"
            ]
        }

    except Exception as e:
        return {
            "service_status": "error",
            "error": str(e)
        }

# Databricks Jobs API Endpoints

class JobSubmissionRequest(BaseModel):
    lakebase_instance_name: str = Field(..., description="Lakebase instance name")
    database_name: str = Field("databricks_postgres", description="Database name")
    cluster_id: Optional[str] = Field(None, description="Databricks cluster ID (optional - will create job cluster if not provided)")
    workspace_url: str = Field(..., description="Databricks workspace URL")
    databricks_profile: str = Field("DEFAULT", description="Databricks CLI profile name")
    pgbench_config: Dict[str, Any] = Field(..., description="pgbench configuration")
    query_configs: Optional[List[Dict[str, Any]]] = Field(None, description="Query configurations (for upload approach)")
    query_workspace_path: Optional[str] = Field(None, description="Workspace path to queries folder (for workspace approach)")

@app.get("/api/databricks/clusters")
async def get_databricks_clusters():
    """
    Get list of available Databricks clusters
    
    Returns:
        List of cluster information
    """
    try:
        print("CLUSTER_API: Starting cluster retrieval")
        jobs_service = DatabricksJobsService()
        
        print("CLUSTER_API: Calling get_clusters()")
        clusters = jobs_service.get_clusters()
        
        print(f"CLUSTER_API: Retrieved {len(clusters)} clusters")
        if clusters:
            for i, cluster in enumerate(clusters[:3]):
                print(f"CLUSTER_API: Cluster {i+1}: {cluster.get('cluster_name', 'Unknown')}")
            
            # Log the exact JSON structure being returned
            import json
            print(f"CLUSTER_API: Sample cluster JSON: {json.dumps(clusters[0], indent=2)}")
        else:
            print("CLUSTER_API: No clusters found - returning empty list")
        
        return clusters
        
    except Exception as e:
        error_msg = str(e)
        print(f"CLUSTER_API: Exception occurred: {error_msg}")
        
        # Always return empty list to prevent frontend issues
        print("CLUSTER_API: Returning empty list due to error")
        return []

@app.get("/api/databricks/clusters/static")
async def get_static_clusters():
    """
    Return a static list of clusters for testing when dynamic API fails
    """
    print("STATIC_CLUSTERS: Returning static cluster list")
    return [
        {
            "cluster_id": "static-shared-apj",
            "cluster_name": "Shared Autoscaling APJ (Static)",
            "state": "RUNNING",
            "node_type_id": "Standard_L8s",
            "num_workers": 2,
            "spark_version": "14.3.x-cpu-ml-scala2.12",
            "driver_node_type_id": "Standard_L16s"
        },
        {
            "cluster_id": "static-shared-emea", 
            "cluster_name": "Shared Autoscaling EMEA (Static)",
            "state": "TERMINATED",
            "node_type_id": "Standard_L8s",
            "num_workers": 0,
            "spark_version": "14.3.x-cpu-ml-scala2.12",
            "driver_node_type_id": "Standard_L16s"
        }
    ]

@app.post("/api/databricks/submit-pgbench-job")
async def submit_pgbench_job(request: JobSubmissionRequest):
    """
    Submit a pgbench job to Databricks
    
    Args:
        request: Job submission configuration
        
    Returns:
        Job submission result with job_id and run_id
    """
    try:
        # Initialize service with workspace URL and profile from request
        jobs_service = DatabricksJobsService(
            profile=request.databricks_profile,
            workspace_url=request.workspace_url
        )
        
        result = jobs_service.submit_pgbench_job(
            lakebase_instance_name=request.lakebase_instance_name,
            database_name=request.database_name,
            cluster_id=request.cluster_id,
            pgbench_config=request.pgbench_config,
            query_configs=request.query_configs,
            query_workspace_path=request.query_workspace_path
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to submit job: {str(e)}")

@app.get("/api/databricks/job-status/{run_id}")
async def get_job_status(run_id: str):
    """
    Get status of a Databricks job run
    
    Args:
        run_id: Job run ID
        
    Returns:
        Job status information
    """
    try:
        jobs_service = DatabricksJobsService()
        status = jobs_service.get_run_status(run_id)
        return status
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get job status: {str(e)}")

@app.delete("/api/databricks/job/{job_id}")
async def delete_databricks_job(job_id: str):
    """
    Delete a Databricks job
    
    Args:
        job_id: Job ID to delete
        
    Returns:
        Success message
    """
    try:
        jobs_service = DatabricksJobsService()
        success = jobs_service.delete_job(job_id)
        
        if success:
            return {"message": f"Job {job_id} deleted successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to delete job")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete job: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)