#!/usr/bin/env python3
"""
FastAPI backend for Databricks Lakebase Accelerator
Provides API endpoints for cost estimation, table generation, and configuration management.
"""

import os
import sys
import tempfile
import subprocess
import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel, Field
import yaml
from dotenv import load_dotenv

# Add the src directory to the Python path to import existing modules
sys.path.append(str(Path(__file__).parent.parent.parent / "src"))

# Load environment variables
load_dotenv()

app = FastAPI(
    title="Databricks Lakebase Accelerator API",
    description="API for cost estimation, table generation, and workload configuration",
    version="1.0.0"
)

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
    lakebase_instance_name: str = Field("lakebase-instance", description="Name for the Lakebase instance")

class CostEstimationRequest(BaseModel):
    workload_config: WorkloadConfigRequest
    calculate_table_sizes: bool = Field(False, description="Whether to calculate actual table sizes from Databricks")

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
    Optionally calculate actual table sizes from Databricks.
    """
    try:
        # Create temporary workload config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as tmp_file:
            workload_data = {
                'database_instance': request.workload_config.database_instance.dict(),
                'database_storage': request.workload_config.database_storage.dict(),
                'delta_synchronization': request.workload_config.delta_synchronization.dict()
            }
            yaml.dump(workload_data, tmp_file)
            tmp_config_path = tmp_file.name

        # Create temporary output file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_output:
            tmp_output_path = tmp_output.name

        try:
            # Build command to run cost estimator
            cmd = [
                sys.executable, 
                str(Path(__file__).parent.parent.parent / "src" / "lakebase_cost_estimator.py"),
                "--config", tmp_config_path,
                "--output", tmp_output_path
            ]
            
            if request.calculate_table_sizes:
                cmd.append("--calculate-table-sizes")

            # Run the cost estimation script
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path(__file__).parent.parent.parent)
            
            if result.returncode != 0:
                raise HTTPException(
                    status_code=500, 
                    detail=f"Cost estimation failed: {result.stderr}"
                )

            # Read the generated cost report
            with open(tmp_output_path, 'r') as f:
                cost_report = json.load(f)

            return JSONResponse(content=cost_report)

        finally:
            # Cleanup temporary files
            os.unlink(tmp_config_path)
            if os.path.exists(tmp_output_path):
                os.unlink(tmp_output_path)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during cost estimation: {str(e)}")

@app.post("/api/generate-synced-tables")
async def generate_synced_tables(request: WorkloadConfigRequest):
    """
    Generate synced tables configuration from workload config.
    """
    try:
        # Create temporary workload config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as tmp_file:
            workload_data = {
                'database_instance': request.database_instance.dict(),
                'database_storage': request.database_storage.dict(),
                'delta_synchronization': request.delta_synchronization.dict()
            }
            yaml.dump(workload_data, tmp_file)
            tmp_config_path = tmp_file.name

        # Create temporary output file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as tmp_output:
            tmp_output_path = tmp_output.name

        try:
            # Run the table generation script
            cmd = [
                sys.executable,
                str(Path(__file__).parent.parent.parent / "src" / "generate_synced_tables.py"),
                "--config", tmp_config_path,
                "--output", tmp_output_path
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path(__file__).parent.parent.parent)
            
            if result.returncode != 0:
                raise HTTPException(
                    status_code=500, 
                    detail=f"Table generation failed: {result.stderr}"
                )

            # Read the generated synced tables config
            with open(tmp_output_path, 'r') as f:
                synced_tables_config = yaml.safe_load(f)

            return JSONResponse(content=synced_tables_config)

        finally:
            # Cleanup temporary files
            os.unlink(tmp_config_path)
            if os.path.exists(tmp_output_path):
                os.unlink(tmp_output_path)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating synced tables: {str(e)}")

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
    try:
        lakebase_config = {
            'resources': {
                'online_tables': {
                    request.lakebase_instance_name: {
                        'name': request.lakebase_instance_name,
                        'spec': {
                            'primary_key_columns': [],  # Will be populated based on tables
                            'timeseries_key': None,
                            'source_table_full_name': '',  # Will be set based on main table
                            'perform_full_copy': True,
                            'run_triggered': {
                                'enabled': True
                            }
                        }
                    }
                }
            }
        }

        return JSONResponse(content=lakebase_config)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating lakebase instance config: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)