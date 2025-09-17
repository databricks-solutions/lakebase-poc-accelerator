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
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel, Field
import yaml
from dotenv import load_dotenv

# Add the src directory to the Python path to import existing modules
sys.path.append(str(Path(__file__).parent.parent.parent / "src"))

# Import the cost estimator and table generator functions
from lakebase_cost_estimator import estimate_cost_from_config
from generate_synced_tables import generate_synced_tables_from_config

# Load environment variables from project root
env_path = Path(__file__).parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)
    print(f"Loaded environment variables from: {env_path}")
else:
    print(f"Warning: .env file not found at {env_path}")
    print("Please create a .env file with your Databricks credentials")

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
    lakebase_instance_name: str = Field("lakebase-accelerator-instance", description="Name for the Lakebase instance")
    uc_catalog_name: str = Field("lakebase-accelerator-catalog", description="Name for the UC catalog")
    database_name: str = Field("databricks_postgres", description="Name for the database")
    recommended_cu: int = Field(1, description="Recommended CU from cost estimation")

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
        # Prepare workload data for the cost estimator
        workload_data = {
            'database_instance': request.workload_config.database_instance.dict(),
            'database_storage': request.workload_config.database_storage.dict(),
            'delta_synchronization': request.workload_config.delta_synchronization.dict()
        }
        
        # Call the cost estimator directly
        cost_report = estimate_cost_from_config(
            workload_data, 
            calculate_table_sizes=request.calculate_table_sizes
        )
        
        # Transform the response to match frontend expectations
        transformed_response = {
            "timestamp": cost_report.get("timestamp"),
            "cost_breakdown": cost_report.get("cost_breakdown"),
            "cost_efficiency_metrics": {
                "cost_per_gb_monthly": cost_report.get("cost_breakdown", {}).get("cost_per_gb", 0),
                "cost_per_qps_monthly": cost_report.get("cost_breakdown", {}).get("cost_per_qps", 0),
                "cost_per_cu_monthly": cost_report.get("cost_breakdown", {}).get("cost_per_cu", 0)
            },
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

@app.post("/api/generate-synced-tables")
async def generate_synced_tables(request: WorkloadConfigRequest):
    """
    Generate synced tables configuration from workload config.
    """
    try:
        # Prepare workload data for the table generator
        workload_data = {
            'database_instance': request.database_instance.dict(),
            'database_storage': request.database_storage.dict(),
            'delta_synchronization': request.delta_synchronization.dict()
        }
        
        # Call the table generator directly
        synced_tables_config = generate_synced_tables_from_config(workload_data)
        
        return JSONResponse(content=synced_tables_config)

    except Exception as e:
        print(f"Error generating synced tables: {str(e)}")
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
        # Generate YAML content that matches the existing lakebase_instance.yml structure
        yaml_content = f"""resources:
#  Provision Lakebase database instance
#  https://docs.databricks.com/aws/en/dev-tools/bundles/resources#database_instances
  database_instances:
    my_instance:
      name: {request.lakebase_instance_name}
      capacity: capacity_{request.recommended_cu}

  # Register Lakebase database as a read-only UC catalog to enable federated queries + data governance
  database_catalogs:
    my_catalog:
      database_instance_name: ${{resources.database_instances.my_instance.name}} # {request.lakebase_instance_name}
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)