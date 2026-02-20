#!/usr/bin/env python3
"""
Databricks Jobs Service for pgbench testing
Handles creation and management of Databricks jobs for running pgbench tests
"""

import json
import time
import importlib.metadata
from typing import Dict, Any, List, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterSpec
import logging

logger = logging.getLogger(__name__)

# Log the Databricks SDK version for debugging
try:
    sdk_version = importlib.metadata.version("databricks-sdk")
    logger.info(f"Using databricks-sdk version: {sdk_version}")
except importlib.metadata.PackageNotFoundError:
    logger.warning("Could not determine databricks-sdk version")

# Import jobs-related classes with error handling
try:
    from databricks.sdk.service.jobs import Task, NotebookTask, JobSettings, JobCluster
    from databricks.sdk.service.workspace import ImportFormat
    # , RunNow
except ImportError as e:
    logger.error(f"Failed to import Databricks SDK jobs classes: {e}")
    logger.error("Please ensure you have databricks-sdk>=0.30.0 installed")
    raise ImportError(
        "Cannot import required Databricks SDK classes. "
        "Please update databricks-sdk to version 0.30.0 or later: "
        "pip install --upgrade 'databricks-sdk>=0.30.0'"
    ) from e

class DatabricksJobsService:
    """Service for managing Databricks jobs for pgbench testing"""
    
    def __init__(self, profile: Optional[str] = None, workspace_url: Optional[str] = None):
        """Initialize the Databricks Jobs Service
        
        Args:
            profile: Databricks profile name for authentication
            workspace_url: Databricks workspace URL
        """
        self.profile = profile
        self.workspace_url = workspace_url
        self.client = None
        
    def _get_client(self) -> WorkspaceClient:
        """Get or create Databricks workspace client"""
        if self.client is None:
            try:
                logger.info("DEBUG: Creating new Databricks client...")
                # Try environment variables first
                import os
                host = os.getenv('DATABRICKS_HOST')
                token = os.getenv('DATABRICKS_TOKEN')
                
                if host and token:
                    logger.info("DEBUG: Using environment variables for Databricks authentication")
                    logger.info(f"DEBUG: Host: {host}")
                    self.client = WorkspaceClient(host=host, token=token)
                elif self.workspace_url:
                    logger.info(f"DEBUG: Using workspace URL: {self.workspace_url}")
                    if self.profile:
                        logger.info(f"DEBUG: Using profile: {self.profile}")
                        self.client = WorkspaceClient(profile=self.profile, host=self.workspace_url)
                    else:
                        self.client = WorkspaceClient(host=self.workspace_url)
                else:
                    if self.profile:
                        logger.info(f"DEBUG: Using profile: {self.profile}")
                        self.client = WorkspaceClient(profile=self.profile)
                    else:
                        logger.info("DEBUG: Using default Databricks authentication (CLI)")
                        self.client = WorkspaceClient()
                
                logger.info("DEBUG: Testing Databricks connection...")        
                # Test connection
                current_user = self.client.current_user.me()
                logger.info(f"DEBUG: Successfully connected to Databricks as user: {current_user.user_name}")
                
            except Exception as e:
                logger.error(f"DEBUG: Failed to initialize Databricks client: {e}")
                logger.error("Please ensure you are authenticated with Databricks:")
                logger.error("  Option 1: Run 'databricks auth login --host <your-workspace-url>'")
                logger.error("  Option 2: Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables")
                raise
        else:
            logger.info("DEBUG: Reusing existing Databricks client")
                
        return self.client
    
    def _detect_cloud_provider(self) -> str:
        """Detect cloud provider (AWS, Azure, or GCP) from workspace URL
        
        Returns:
            'aws', 'azure', or 'gcp'
        """
        try:
            client = self._get_client()
            
            # First, try to get workspace URL from the client's config
            import os
            host = os.getenv('DATABRICKS_HOST') or self.workspace_url
            
            if not host:
                # Try to get from client config
                if hasattr(client, 'config') and hasattr(client.config, 'host'):
                    host = client.config.host
            
            if host:
                print(f"CLOUD_DETECT: Detecting cloud provider from host: {host}")
                host_lower = host.lower()
                
                # Azure workspace URLs contain 'azuredatabricks.net'
                if 'azuredatabricks.net' in host_lower or 'azure' in host_lower:
                    print("CLOUD_DETECT: Detected Azure cloud provider")
                    return 'azure'
                
                # GCP workspace URLs contain 'gcp.databricks.com'
                if 'gcp.databricks.com' in host_lower:
                    print("CLOUD_DETECT: Detected GCP cloud provider")
                    return 'gcp'
                
                # AWS workspace URLs contain 'cloud.databricks.com', 'dbc-' patterns, or other AWS-specific patterns
                if 'cloud.databricks.com' in host_lower or '.cloud.databricks.com' in host_lower:
                    print("CLOUD_DETECT: Detected AWS cloud provider")
                    return 'aws'
            
            # Default to Azure if we can't determine (for backward compatibility)
            print("CLOUD_DETECT: Could not detect cloud provider, defaulting to Azure")
            return 'azure'
            
        except Exception as e:
            logger.warning(f"DEBUG: Failed to detect cloud provider: {e}, defaulting to Azure")
            return 'azure'
    
    def _get_node_type_for_workload(self, threads: int, clients: int) -> str:
        """Select appropriate node type based on workload requirements (threads and clients)
        
        Uses tiered selection:
        - Primary: Match cores to threads (capped at 64)
        - Secondary: Ensure sufficient memory for clients (~200MB per client)
        - Returns memory-optimized instances for better performance
        
        Args:
            threads: Number of pgbench worker threads (-j parameter)
            clients: Number of concurrent database connections (-c parameter)
            
        Returns:
            Node type ID string appropriate for the cloud provider and workload
        """
        cloud = self._detect_cloud_provider()
        
        # Cap threads at 64 cores maximum
        threads = min(threads, 64)
        
        # Calculate required memory (200MB per client as conservative estimate)
        required_memory_gb = (clients * 200) / 1024
        
        # Instance type mapping: cloud -> tier -> instance_type
        # Using general purpose instances (m6i for AWS - best price/performance for pgbench)
        INSTANCE_MAP = {
            'aws': {
                'small': 'm6i.xlarge',      # 4 cores, 16 GB
                'medium': 'm6i.2xlarge',    # 8 cores, 32 GB
                'large': 'm6i.4xlarge',     # 16 cores, 64 GB
                'xlarge': 'm6i.8xlarge',    # 32 cores, 128 GB
                '2xlarge': 'm6i.16xlarge',  # 64 cores, 256 GB
            },
            'azure': {
                'small': 'Standard_E4s_v4',     # 4 cores, 32 GB
                'medium': 'Standard_E8s_v3',    # 8 cores, 64 GB
                'large': 'Standard_E16s_v3',    # 16 cores, 128 GB
                'xlarge': 'Standard_E32s_v3',   # 32 cores, 256 GB
                '2xlarge': 'Standard_E64s_v3',  # 64 cores, 432 GB
            },
            'gcp': {
                'small': 'n2-highmem-4',    # 4 cores, 32 GB
                'medium': 'n2-highmem-8',   # 8 cores, 64 GB
                'large': 'n2-highmem-16',   # 16 cores, 128 GB
                'xlarge': 'n2-highmem-32',  # 32 cores, 256 GB
                '2xlarge': 'n2-highmem-64', # 64 cores, 512 GB
            }
        }
        
        # Tier specifications: (tier_name, max_threads, memory_gb)
        # Memory values based on m6i family (4GB per vCPU)
        TIERS = [
            ('small', 4, 16),      # m6i.xlarge
            ('medium', 8, 32),     # m6i.2xlarge
            ('large', 16, 64),     # m6i.4xlarge
            ('xlarge', 32, 128),   # m6i.8xlarge
            ('2xlarge', 64, 256),  # m6i.16xlarge
        ]
        
        # Select tier based on threads (primary criterion)
        selected_tier = 'small'
        for tier_name, max_threads, tier_memory in TIERS:
            if threads <= max_threads:
                selected_tier = tier_name
                tier_mem_gb = tier_memory
                break
        else:
            # If threads > 64, use largest tier
            selected_tier = '2xlarge'
            tier_mem_gb = 512
        
        # Upgrade tier if insufficient memory for clients
        if required_memory_gb > tier_mem_gb:
            logger.info(f"Upgrading tier: {clients} clients need {required_memory_gb:.1f}GB, "
                       f"tier '{selected_tier}' only has {tier_mem_gb}GB")
            for tier_name, max_threads, tier_memory in TIERS:
                if required_memory_gb <= tier_memory:
                    selected_tier = tier_name
                    tier_mem_gb = tier_memory
                    break
            else:
                # Use largest tier if memory requirement exceeds all tiers
                selected_tier = '2xlarge'
                tier_mem_gb = 512
        
        # Get cloud-specific instance type
        node_type = INSTANCE_MAP.get(cloud, INSTANCE_MAP['azure']).get(selected_tier, 'Standard_E8s_v3')
        
        logger.info(f"Selected node type '{node_type}' (tier: {selected_tier}) for "
                   f"workload: {threads} threads, {clients} clients on {cloud} cloud")
        logger.info(f"Estimated memory requirement: {required_memory_gb:.1f}GB, "
                   f"tier provides: {tier_mem_gb}GB")
        
        return node_type
    
    def get_clusters(self) -> List[Dict[str, Any]]:
        """Get list of available Databricks clusters
        
        Returns:
            List of cluster information dictionaries
        """
        try:
            logger.info("DEBUG: get_clusters() called")
            logger.info("DEBUG: Getting Databricks client...")
            client = self._get_client()
            logger.info("DEBUG: Client obtained, listing clusters...")
            
            clusters = []
            cluster_count = 0
            for cluster in client.clusters.list():
                cluster_count += 1
                logger.info(f"DEBUG: Processing cluster {cluster_count}: {cluster.cluster_name}")
                clusters.append({
                    "cluster_id": cluster.cluster_id,
                    "cluster_name": cluster.cluster_name,
                    "state": cluster.state.value if cluster.state else "UNKNOWN",
                    "node_type_id": cluster.node_type_id,
                    "num_workers": cluster.num_workers or 0,
                    "spark_version": cluster.spark_version,
                    "driver_node_type_id": cluster.driver_node_type_id
                })
            
            logger.info(f"DEBUG: Successfully retrieved {len(clusters)} clusters")
            return clusters
            
        except Exception as e:
            logger.error(f"DEBUG: Failed to get clusters: {e}")
            import traceback
            logger.error(f"DEBUG: Full traceback: {traceback.format_exc()}")
            raise
    
    def _create_workspace_directory(self, directory_path: str):
        """Create directory in Databricks workspace if it doesn't exist
        
        Args:
            directory_path: Path to directory in workspace
        """
        try:
            client = self._get_client()
            
            # Check if directory exists
            try:
                client.workspace.get_status(directory_path)
                logger.info(f"Directory {directory_path} already exists")
                return
            except Exception:
                # Directory doesn't exist, create it
                logger.info(f"Creating directory {directory_path}")
                client.workspace.mkdirs(directory_path)
                logger.info(f"Successfully created directory {directory_path}")
                
        except Exception as e:
            logger.error(f"Failed to create directory {directory_path}: {e}")
            raise


    def _convert_databricks_to_jupyter(self, databricks_content: str) -> str:
        """Convert Databricks notebook source format to Jupyter notebook JSON format"""
        import json
        import re
        
        lines = databricks_content.split('\n')
        cells = []
        current_cell = None
        current_source = []
        
        for line in lines:
            if line.strip() == "# Databricks notebook source":
                continue
            elif line.startswith("# COMMAND ----------"):
                # End current cell and start new one
                if current_cell is not None:
                    current_cell["source"] = current_source
                    cells.append(current_cell)
                current_cell = None
                current_source = []
            elif line.startswith("# MAGIC %md"):
                # Start markdown cell
                if current_cell is not None:
                    current_cell["source"] = current_source
                    cells.append(current_cell)
                current_cell = {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": []
                }
                current_source = []
                # Add the markdown content (remove # MAGIC prefix)
                md_content = line.replace("# MAGIC %md", "").strip()
                if md_content:
                    current_source.append(md_content + "\n")
            elif line.startswith("# MAGIC"):
                # Continue markdown cell
                if current_cell and current_cell["cell_type"] == "markdown":
                    md_content = line.replace("# MAGIC", "").strip()
                    current_source.append(md_content + "\n")
            else:
                # Code cell content
                if current_cell is None:
                    current_cell = {
                        "cell_type": "code",
                        "execution_count": None,
                        "metadata": {},
                        "outputs": [],
                        "source": []
                    }
                    current_source = []
                current_source.append(line + "\n")
        
        # Add final cell
        if current_cell is not None:
            current_cell["source"] = current_source
            cells.append(current_cell)
        
        # Create Jupyter notebook structure
        notebook = {
            "cells": cells,
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3"
                },
                "language_info": {
                    "name": "python",
                    "version": "3.8.0"
                }
            },
            "nbformat": 4,
            "nbformat_minor": 4
        }
        
        return json.dumps(notebook, indent=2)

    def _get_current_service_principal(self) -> str:
        """Get the current service principal ID"""
        try:
            client = self._get_client()
            current_user_info = client.current_user.me()
            logger.info(f"SERVICE_PRINCIPAL: Current user info: {current_user_info}")
            
            # For service principals, we want the application_id or user_name
            if hasattr(current_user_info, 'application_id') and current_user_info.application_id:
                return current_user_info.application_id
            elif hasattr(current_user_info, 'user_name') and current_user_info.user_name:
                return current_user_info.user_name
            elif hasattr(current_user_info, 'id') and current_user_info.id:
                return current_user_info.id
            else:
                logger.warning(f"SERVICE_PRINCIPAL: Could not extract user identifier")
                return "8647325b-c774-4b63-8a46-2c5e8be1b777"  # Fallback
                
        except Exception as e:
            logger.warning(f"SERVICE_PRINCIPAL: Could not get current user info: {e}")
            return "8647325b-c774-4b63-8a46-2c5e8be1b777"  # Known service principal ID

    def _get_workspace_url(self) -> str:
        """Get the Databricks workspace URL"""
        try:
            # Try to get workspace URL from client config
            client = self._get_client()
            
            # Try to get from environment variables first (most reliable)
            import os
            host = os.getenv('DATABRICKS_HOST')
            if host:
                logger.info(f"WORKSPACE_URL: Raw DATABRICKS_HOST: {host}")
                
                # Ensure it's a proper workspace URL (not app URL)
                workspace_url = host.rstrip('/')
                
                # Remove any app-specific prefixes and get the actual workspace URL
                if 'databricksapps.com' in workspace_url:
                    logger.info(f"WORKSPACE_URL: Detected app URL format, extracting workspace URL")
                    # Extract the actual workspace URL from app URL
                    # Format: https://app-name.cloud.databricksapps.com/adb-xxxxx.cloud.azuredatabricks.net
                    if '/' in workspace_url and 'adb-' in workspace_url:
                        parts = workspace_url.split('/')
                        logger.info(f"WORKSPACE_URL: URL parts: {parts}")
                        for part in parts:
                            if part.startswith('adb-') and 'databricks' in part:
                                workspace_url = f"https://{part}"
                                logger.info(f"WORKSPACE_URL: Extracted workspace URL: {workspace_url}")
                                break
                    else:
                        logger.warning(f"WORKSPACE_URL: Could not extract workspace URL from app URL: {workspace_url}")
                
                logger.info(f"WORKSPACE_URL: Final workspace URL: {workspace_url}")
                return workspace_url
            
            # Check if we have workspace URL from initialization
            if self.workspace_url:
                workspace_url = self.workspace_url.rstrip('/')
                logger.info(f"WORKSPACE_URL: Using initialization URL: {workspace_url}")
                return workspace_url
            
            # Try to get from client configuration
            if hasattr(client, 'config') and hasattr(client.config, 'host'):
                workspace_url = client.config.host.rstrip('/')
                logger.info(f"WORKSPACE_URL: Using client config: {workspace_url}")
                return workspace_url
            
            # Fallback: try to extract from client
            if hasattr(client, '_client') and hasattr(client._client, '_host'):
                workspace_url = client._client._host.rstrip('/')
                logger.info(f"WORKSPACE_URL: Using client host: {workspace_url}")
                return workspace_url
            
            # Last resort: return a placeholder
            logger.warning("Could not determine workspace URL, using placeholder")
            return "https://your-workspace.databricks.com"
            
        except Exception as e:
            logger.warning(f"Error getting workspace URL: {e}")
            return "https://your-workspace.databricks.com"

    def upload_notebook(self, notebook_path: str, content: str) -> str:
        """Upload notebook to Databricks workspace
        
        Args:
            notebook_path: Path in workspace where notebook should be uploaded
            content: Notebook content as JSON string
            
        Returns:
            Workspace path of uploaded notebook
        """
        try:
            # Extract directory path and create it if necessary
            import os
            directory_path = os.path.dirname(notebook_path)
            if directory_path:
                self._create_workspace_directory(directory_path)
            
            # Convert notebook content to bytes
            notebook_bytes = content.encode('utf-8')
            
            # Upload notebook to workspace
            client = self._get_client()
            client.workspace.upload(
                path=notebook_path,
                content=notebook_bytes,
                format=ImportFormat.JUPYTER,
                overwrite=True
            )
            
            logger.info(f"Successfully uploaded notebook to {notebook_path}")
            return notebook_path
            
        except Exception as e:
            logger.error(f"Failed to upload notebook: {e}")
            raise
    
    def _upload_init_script(self) -> str:
        """
        Upload the pgbench init script to the workspace (reusable location).
        Returns the workspace path to the uploaded script.
        Checks if script already exists and only uploads if missing or different.
        """
        try:
            import os
            client = self._get_client()
            
            # Use a fixed workspace path (not per-job) for reusability
            workspace_path = "/Shared/pgbench_resources/init.sh"
            logger.info(f"INIT_SCRIPT: Uploading latest version to {workspace_path}")
            
            # Find the init.sh script locally
            current_dir = os.path.dirname(os.path.abspath(__file__))
            parent_dir = os.path.dirname(current_dir)
            
            possible_paths = [
                os.path.join(parent_dir, "notebooks", "init.sh"),
                os.path.join(os.getcwd(), "app", "notebooks", "init.sh"),
                "/app/python/source_code/app/notebooks/init.sh",
            ]
            
            init_script_path = None
            for path in possible_paths:
                logger.info(f"INIT_SCRIPT: Checking path: {path}")
                if os.path.exists(path):
                    init_script_path = path
                    logger.info(f"INIT_SCRIPT: Found init script at: {path}")
                    break
            
            if not init_script_path:
                raise FileNotFoundError(f"Could not find init.sh. Tried: {possible_paths}")
            
            # Read the init script
            with open(init_script_path, 'r') as f:
                init_script_content = f.read()
            
            logger.info(f"INIT_SCRIPT: Read {len(init_script_content)} bytes from init.sh")
            logger.info(f"INIT_SCRIPT: Uploading to workspace path: {workspace_path}")
            
            # Create directory if it doesn't exist
            directory_path = os.path.dirname(workspace_path)
            if directory_path:
                self._create_workspace_directory(directory_path)
            
            # Upload script using the import API with AUTO format
            # This allows Databricks to detect it's a plain file, not a notebook
            import base64
            script_bytes = init_script_content.encode('utf-8')
            script_base64 = base64.b64encode(script_bytes).decode('utf-8')
            
            # Use the REST API for more control over file import
            response = client.api_client.do(
                'POST',
                '/api/2.0/workspace/import',
                body={
                    'path': workspace_path,
                    'format': 'AUTO',
                    'content': script_base64,
                    'overwrite': True
                }
            )
            
            logger.info(f"INIT_SCRIPT: Successfully uploaded init script to {workspace_path}")
            return workspace_path
            
        except Exception as e:
            logger.error(f"Failed to upload init script: {str(e)}")
            raise
    
    def create_pgbench_job(self, 
                          job_name: str,
                          cluster_id: str,
                          notebook_path: str,
                          parameters: Dict[str, Any]) -> str:
        """Create a Databricks job for pgbench testing
        
        Args:
            job_name: Name for the job
            cluster_id: ID of cluster to run the job on
            notebook_path: Workspace path to the pgbench notebook
            parameters: Job parameters to pass to the notebook
            
        Returns:
            Job ID of created job
        """
        try:
            # Convert parameters to string format expected by Databricks
            base_parameters = {}
            for key, value in parameters.items():
                if isinstance(value, (dict, list)):
                    base_parameters[key] = json.dumps(value)
                else:
                    base_parameters[key] = str(value)
            
            # Smart cluster selection: use existing cluster if provided, otherwise create job cluster
            client = self._get_client()
            
            if cluster_id and cluster_id.strip():
                # Use existing cluster approach
                logger.info(f"EXISTING_CLUSTER: Using existing cluster {cluster_id} for job")
                
                job_settings = JobSettings(
                    name=job_name,
                    tasks=[
                        Task(
                            task_key="pgbench_test",
                            notebook_task=NotebookTask(
                                notebook_path=notebook_path,
                                base_parameters=base_parameters
                            ),
                            existing_cluster_id=cluster_id,
                            timeout_seconds=3600  # 1 hour timeout
                        )
                    ],
                    max_concurrent_runs=1,
                    timeout_seconds=3600
                )
                
                logger.info(f"EXISTING_CLUSTER: Created JobSettings for existing cluster")
                
                # Create the job
                job = client.jobs.create(
                    name=job_settings.name,
                    tasks=job_settings.tasks,
                    max_concurrent_runs=job_settings.max_concurrent_runs,
                    timeout_seconds=job_settings.timeout_seconds
                )
                job_id = str(job.job_id)
                logger.info(f"EXISTING_CLUSTER: Successfully created job: {job_id}")
                
            else:
                # No cluster ID provided - create job cluster using REST API
                logger.info("JOB_CLUSTER: No cluster ID provided, creating job cluster using REST API")
                
                # Get service principal for single-user cluster
                current_user = self._get_current_service_principal()
                logger.info(f"JOB_CLUSTER: Using service principal: {current_user}")
                
                # Get init script path (uploads only if not already in workspace)
                init_script_path = self._upload_init_script()
                logger.info(f"JOB_CLUSTER: Using init script at {init_script_path}")
                
                # Use SDK's internal API client for REST call
                # This automatically handles authentication (OAuth, service principal, PAT, etc.)
                client = self._get_client()
                
                logger.info(f"JOB_CLUSTER: Using SDK API client for job creation")
                
                # Get the appropriate node type based on workload (threads and clients)
                pgbench_threads = int(parameters.get('pgbench_jobs', 8))
                pgbench_clients = int(parameters.get('pgbench_clients', 100))
                node_type = self._get_node_type_for_workload(pgbench_threads, pgbench_clients)
                
                # Detect cloud provider for cloud-specific configurations
                cloud = self._detect_cloud_provider()
                
                # Build new_cluster configuration
                new_cluster_config = {
                    "spark_version": "14.3.x-scala2.12",
                    "node_type_id": node_type,
                    "num_workers": 0,
                    "spark_conf": {
                        "spark.databricks.cluster.profile": "singleNode",
                        "spark.master": "local[*]"
                    },
                    "custom_tags": {
                        "ResourceClass": "SingleNode",
                        "pgbench_job": "true"
                    },
                    "data_security_mode": "SINGLE_USER",
                    "single_user_name": current_user,
                    "init_scripts": [
                        {
                            "workspace": {
                                "destination": init_script_path
                            }
                        }
                    ]
                }
                
                # Add cloud-specific attributes
                # AWS 6th gen Intel instances (m6i, r6i, c6i, etc.) require EBS volumes (no local storage)
                if cloud == 'aws' and any(family in node_type for family in ['m6i', 'r6i', 'c6i', 'm6a', 'r6a', 'c6a']):
                    new_cluster_config["aws_attributes"] = {
                        "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                        "ebs_volume_count": 1,
                        "ebs_volume_size": 100
                    }
                    logger.info(f"Added EBS volume configuration for AWS 6th gen instance type: {node_type}")
                
                # Build job configuration for REST API
                job_payload = {
                    "name": job_name,
                    "tasks": [
                        {
                            "task_key": "pgbench_test",
                            "notebook_task": {
                                "notebook_path": notebook_path,
                                "base_parameters": base_parameters
                            },
                            "new_cluster": new_cluster_config,
                            "timeout_seconds": 3600
                        }
                    ],
                    "max_concurrent_runs": 1,
                    "timeout_seconds": 3600
                }
                
                logger.info(f"JOB_CLUSTER: Sending API request to create job with new_cluster")
                
                # Use SDK's internal API client which handles auth automatically
                response = client.api_client.do(
                    'POST',
                    '/api/2.1/jobs/create',
                    body=job_payload
                )
                
                job_id = str(response.get("job_id"))
                logger.info(f"JOB_CLUSTER: Successfully created job with auto cluster: {job_id}")
            
            logger.info(f"Successfully created job {job_id}: {job_name}")
            return job_id
            
        except Exception as e:
            error_msg = str(e).lower()
            if "permission" in error_msg or "single-user" in error_msg or "attach" in error_msg:
                helpful_error = (
                    f"Cluster permissions error: {e}\n\n"
                    f"SOLUTION: The cluster '{cluster_id}' needs proper permissions for the service principal.\n"
                    f"Please ask your Databricks admin to:\n"
                    f"1. Go to Compute → Clusters → {cluster_id}\n"
                    f"2. Click 'Permissions' tab\n"
                    f"3. Add the service principal with 'Can Attach To' permission\n"
                    f"   OR change the cluster access mode from 'Single User' to 'Shared'\n\n"
                    f"Alternative: Use a different cluster that allows shared access."
                )
                logger.error(helpful_error)
                raise Exception(helpful_error) from e
            else:
                logger.error(f"Failed to create job: {e}")
                raise
    
    def run_job(self, job_id: str, parameters: Optional[Dict[str, Any]] = None) -> str:
        """Run a Databricks job
        
        Args:
            job_id: ID of job to run
            parameters: Optional additional parameters for this run
            
        Returns:
            Run ID of started job run
        """
        try:
            # Prepare run parameters
            run_parameters = {}
            if parameters:
                for key, value in parameters.items():
                    if isinstance(value, (dict, list)):
                        run_parameters[key] = json.dumps(value)
                    else:
                        run_parameters[key] = str(value)
            
            # Start the job run
            client = self._get_client()
            run = client.jobs.run_now(
                job_id=int(job_id),
                notebook_params=run_parameters if run_parameters else None
            )

            run_id = str(run.run_id)
            
            logger.info(f"Successfully started job run {run_id} for job {job_id}")
            return run_id
            
        except Exception as e:
            logger.error(f"Failed to run job {job_id}: {e}")
            raise
    
    def get_run_status(self, run_id: str) -> Dict[str, Any]:
        """Get status of a job run
        
        Args:
            run_id: ID of job run to check
            
        Returns:
            Dictionary containing run status information
        """
        try:
            client = self._get_client()
            run = client.jobs.get_run(int(run_id))
            
            # Map Databricks run states to our status format
            state_mapping = {
                "PENDING": "pending",
                "RUNNING": "running", 
                "TERMINATING": "running",
                "TERMINATED": "completed",
                "SKIPPED": "failed",
                "INTERNAL_ERROR": "failed"
            }
            
            life_cycle_state = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else "UNKNOWN"
            result_state = run.state.result_state.value if run.state and run.state.result_state else None
            
            status = state_mapping.get(life_cycle_state, "unknown")
            
            # If terminated, check if it was successful
            if life_cycle_state == "TERMINATED":
                if result_state == "SUCCESS":
                    status = "completed"
                else:
                    status = "failed"
            
            # Calculate progress (rough estimate based on state)
            progress = 0
            if status == "pending":
                progress = 0
            elif status == "running":
                progress = 50  # Rough estimate
            elif status == "completed":
                progress = 100
            elif status == "failed":
                progress = 100
            
            # Get run output if available
            results = None
            pgbench_results = None
            if status == "completed":
                try:
                    # For multi-task jobs, we need to get output from the specific task
                    logger.info(f"Attempting to get run output for run_id={run_id}")
                    
                    # First, get the run to find task runs
                    run = client.jobs.get_run(int(run_id))
                    
                    # Find the pgbench_test task run
                    pgbench_task_run_id = None
                    if run.tasks:
                        for task_run in run.tasks:
                            if task_run.task_key == "pgbench_test":
                                pgbench_task_run_id = task_run.run_id
                                logger.info(f"Found pgbench_test task run: {pgbench_task_run_id}")
                                break
                    
                    if pgbench_task_run_id:
                        # Get output from the specific task run
                        output = client.jobs.get_run_output(pgbench_task_run_id)
                        logger.info(f"Got output object from task run: {output is not None}")
                        
                        if output:
                            logger.info(f"Output has notebook_output: {hasattr(output, 'notebook_output') and output.notebook_output is not None}")
                            if output.notebook_output:
                                result_str = output.notebook_output.result
                                logger.info(f"notebook_output.result type: {type(result_str)}, length: {len(result_str) if result_str else 0}")
                                if result_str:
                                    logger.info(f"First 200 chars of result: {result_str[:200]}")
                                
                                # Parse results from notebook output
                                results = self._parse_notebook_results(result_str)
                                logger.info(f"Parsed results: {results is not None}, keys: {list(results.keys()) if results else None}")
                                
                                # Extract pgbench summary stats from raw_output field
                                if results and 'raw_output' in results:
                                    raw_output = results['raw_output']
                                    logger.info(f"Found raw_output in results, length: {len(raw_output)}")
                                    pgbench_results = self._parse_pgbench_results(raw_output)
                                    if pgbench_results:
                                        logger.info(f"Successfully parsed pgbench summary: TPS={pgbench_results.get('tps')}")
                                    else:
                                        logger.warning("Could not parse pgbench summary from raw_output")
                                else:
                                    logger.warning(f"No raw_output in results. Results keys: {list(results.keys()) if results else 'None'}")
                            else:
                                logger.warning("output.notebook_output is None")
                        else:
                            logger.warning("output is None")
                    else:
                        logger.warning(f"Could not find pgbench_test task in run {run_id}")
                        
                except Exception as e:
                    logger.error(f"Could not retrieve run output: {e}", exc_info=True)
            
            response = {
                "run_id": run_id,
                "status": status,
                "message": self._get_status_message(life_cycle_state, result_state),
                "progress": progress,
                "start_time": run.start_time,
                "end_time": run.end_time,
                "results": results
            }
            
            # Add pgbench summary stats if available
            if pgbench_results:
                response["pgbench_results"] = pgbench_results
            
            return response
            
        except Exception as e:
            logger.error(f"Failed to get run status for {run_id}: {e}")
            return {
                "run_id": run_id,
                "status": "failed",
                "message": f"Error getting run status: {e}",
                "progress": 0
            }
    
    def _get_status_message(self, life_cycle_state: str, result_state: Optional[str]) -> str:
        """Get human-readable status message"""
        if life_cycle_state == "PENDING":
            return "Job is pending execution"
        elif life_cycle_state == "RUNNING":
            return "Job is running pgbench test"
        elif life_cycle_state == "TERMINATING":
            return "Job is finishing up"
        elif life_cycle_state == "TERMINATED":
            if result_state == "SUCCESS":
                return "pgbench test completed successfully"
            else:
                return f"Job failed with result: {result_state}"
        else:
            return f"Job status: {life_cycle_state}"
    
    def _parse_notebook_results(self, notebook_result: str) -> Optional[Dict[str, Any]]:
        """Parse results from notebook output
        
        Args:
            notebook_result: Raw notebook output string
            
        Returns:
            Parsed results dictionary or None
        """
        try:
            if not notebook_result:
                return None
                
            # Try to parse as JSON directly (from dbutils.notebook.exit())
            try:
                return json.loads(notebook_result)
            except json.JSONDecodeError:
                pass
            
            # Fall back to looking for JSON in the output lines
            lines = notebook_result.split('\n')
            for line in lines:
                line = line.strip()
                if line.startswith('{') and 'test_parameters' in line:
                    return json.loads(line)
            
            # If no JSON found, return basic info
            return {
                "test_status": "completed",
                "raw_output": notebook_result
            }
            
        except Exception as e:
            logger.warning(f"Could not parse notebook results: {e}")
            return None
    
    def _parse_pgbench_results(self, raw_output: str) -> Optional[Dict[str, Any]]:
        """Parse pgbench summary statistics from raw output
        
        Args:
            raw_output: Raw pgbench stdout containing summary stats
            
        Returns:
            Dictionary with parsed pgbench summary stats or None
        """
        import re
        
        try:
            if not raw_output:
                return None
                
            results = {}
            
            # Extract summary stats from pgbench output
            patterns = {
                'transaction_type': r'transaction type:\s*(.+)',
                'scaling_factor': r'scaling factor:\s*(\d+)',
                'query_mode': r'query mode:\s*(\w+)',
                'num_clients': r'number of clients:\s*(\d+)',
                'num_threads': r'number of threads:\s*(\d+)',
                'duration': r'duration:\s*(\d+)\s*s',
                'total_transactions': r'number of transactions actually processed:\s*(\d+)',
                'failed_transactions': r'number of failed transactions:\s*(\d+)',
                'latency_avg_ms': r'latency average\s*=\s*([\d.]+)\s*ms',
                'latency_stddev_ms': r'latency stddev\s*=\s*([\d.]+)\s*ms',
                'initial_connection_time_ms': r'initial connection time\s*=\s*([\d.]+)\s*ms',
                'tps': r'tps\s*=\s*([\d.]+)',
            }
            
            for key, pattern in patterns.items():
                match = re.search(pattern, raw_output)
                if match:
                    value = match.group(1)
                    # Convert numeric values
                    if key in ['scaling_factor', 'num_clients', 'num_threads', 'duration', 'total_transactions', 'failed_transactions']:
                        results[key] = int(value)
                    elif key in ['latency_avg_ms', 'latency_stddev_ms', 'initial_connection_time_ms', 'tps']:
                        results[key] = float(value)
                    else:
                        results[key] = value.strip()
            
            # Calculate success rate if we have the data
            if 'total_transactions' in results and 'failed_transactions' in results:
                total = results['total_transactions']
                failed = results['failed_transactions']
                if total > 0:
                    results['success_rate'] = round((total - failed) / total * 100, 2)
            
            # Parse per-query statistics
            per_query_stats = []
            sql_script_pattern = r'SQL script \d+: (.+?)\n.*?- weight: (\d+).*?\n.*?- (\d+) transactions.*?tps = ([\d.]+)\).*?\n.*?- latency average = ([\d.]+) ms.*?\n.*?- latency stddev = ([\d.]+) ms'
            
            for match in re.finditer(sql_script_pattern, raw_output, re.DOTALL):
                query_path = match.group(1).strip()
                query_name = query_path.split('/')[-1].replace('.sql', '') if '/' in query_path else query_path
                
                query_stat = {
                    'query_name': query_name,
                    'query_path': query_path,
                    'weight': int(match.group(2)),
                    'transactions': int(match.group(3)),
                    'tps': float(match.group(4)),
                    'latency_avg_ms': float(match.group(5)),
                    'latency_stddev_ms': float(match.group(6))
                }
                per_query_stats.append(query_stat)
            
            if per_query_stats:
                results['per_query_stats'] = per_query_stats
                logger.info(f"Parsed {len(per_query_stats)} per-query statistics")
            
            # Only return if we found the key metric (TPS)
            if 'tps' in results:
                return results
            else:
                logger.warning("Could not find TPS in pgbench output")
                return None
                
        except Exception as e:
            logger.error(f"Error parsing pgbench results: {e}")
            return None
    
    def _get_or_create_pgbench_job(self, cluster_id: Optional[str]) -> str:
        """Get existing pgbench job or create/update it based on cluster configuration.
        
        The job is reusable across all test runs with different parameters.
        Job configuration is updated if cluster type changes (interactive <-> job cluster).
        
        Args:
            cluster_id: Cluster ID to use (None for auto job cluster)
            
        Returns:
            Job ID string
        """
        try:
            client = self._get_client()
            
            # Get app name from environment (set by Databricks Apps)
            import os
            app_name = os.getenv('DATABRICKS_APP_NAME', 'lakebase_app')
            
            # Unified job name for all pgbench tests
            job_name = f"{app_name}_pgbench_job"
            
            print(f"PGBENCH_JOB: Using job name: {job_name}")
            print(f"PGBENCH_JOB: Cluster config - Interactive: {bool(cluster_id)}")
            
            # Check if job already exists
            print(f"PGBENCH_JOB: Checking if job '{job_name}' exists")
            
            existing_job = None
            existing_job_id = None
            try:
                # List all jobs and find ours by name
                for job in client.jobs.list():
                    if job.settings and job.settings.name == job_name:
                        existing_job_id = job.job_id
                        print(f"PGBENCH_JOB: Found existing job ID: {existing_job_id}")
                        break
                
                # Fetch full job details if found
                if existing_job_id:
                    print(f"PGBENCH_JOB: Fetching full job details for job {existing_job_id}")
                    existing_job = client.jobs.get(job_id=existing_job_id)
                    print(f"PGBENCH_JOB: Successfully fetched full job details")
            except Exception as e:
                print(f"PGBENCH_JOB: Error finding/fetching job: {e}")
            
            # If job exists, check if cluster configuration matches
            if existing_job:
                job_id = str(existing_job.job_id)
                needs_update = False
                
                print(f"PGBENCH_JOB: Checking if job config needs update...")
                print(f"PGBENCH_JOB:   - existing_job.settings exists: {existing_job.settings is not None}")
                if existing_job.settings:
                    print(f"PGBENCH_JOB:   - existing_job.settings.tasks exists: {existing_job.settings.tasks is not None}")
                    if existing_job.settings.tasks:
                        print(f"PGBENCH_JOB:   - number of tasks: {len(existing_job.settings.tasks)}")
                
                # Check if cluster configuration matches current request
                if existing_job.settings and existing_job.settings.tasks:
                    task = existing_job.settings.tasks[0]
                    
                    # Log current job configuration
                    print(f"PGBENCH_JOB: Current job task config:")
                    print(f"PGBENCH_JOB:   - existing_cluster_id: {task.existing_cluster_id}")
                    print(f"PGBENCH_JOB:   - has new_cluster: {task.new_cluster is not None}")
                    print(f"PGBENCH_JOB: Requested cluster_id: {repr(cluster_id)}")
                    
                    # Check cluster type mismatch
                    using_interactive = task.existing_cluster_id is not None
                    wants_interactive = cluster_id and cluster_id.strip()
                    
                    print(f"PGBENCH_JOB: Comparison:")
                    print(f"PGBENCH_JOB:   - Job currently uses interactive: {using_interactive}")
                    print(f"PGBENCH_JOB:   - Request wants interactive: {bool(wants_interactive)}")
                    
                    if using_interactive != bool(wants_interactive):
                        needs_update = True
                        print(f"PGBENCH_JOB: ✓ Cluster type changed - Was interactive: {using_interactive}, Now: {bool(wants_interactive)}")
                    elif using_interactive and task.existing_cluster_id != cluster_id:
                        needs_update = True
                        print(f"PGBENCH_JOB: ✓ Interactive cluster ID changed - Was: {task.existing_cluster_id}, Now: {cluster_id}")
                    else:
                        print(f"PGBENCH_JOB: ✓ Cluster configuration matches")
                
                if not needs_update:
                    print(f"PGBENCH_JOB: Job configuration matches, reusing job {job_id}")
                    return job_id
                
                # Configuration doesn't match, update the job
                print(f"PGBENCH_JOB: Updating job {job_id} with new cluster configuration")
                
                # Get notebook path
                notebook_path = "/Shared/pgbench_resources/pgbench_parameterized"
                
                if cluster_id and cluster_id.strip():
                    # Update to use interactive cluster
                    updated_settings = JobSettings(
                        name=job_name,
                        tasks=[
                            Task(
                                task_key="pgbench_test",
                                notebook_task=NotebookTask(
                                    notebook_path=notebook_path,
                                    base_parameters={}
                                ),
                                existing_cluster_id=cluster_id,
                                timeout_seconds=3600
                            )
                        ],
                        max_concurrent_runs=1,
                        timeout_seconds=3600
                    )
                else:
                    # Update to use job cluster
                    current_user = self._get_current_service_principal()
                    init_script_path = self._upload_init_script()
                    
                    # Detect cloud and select appropriate node type
                    cloud = self._detect_cloud_provider()
                    print(f"PGBENCH_JOB: Detected cloud: {cloud}")
                    
                    # Use default medium instance for job update (actual sizing happens at run time)
                    if cloud == 'aws':
                        default_node_type = 'm6i.2xlarge'
                    elif cloud == 'azure':
                        default_node_type = 'Standard_E8s_v3'
                    else:  # gcp
                        default_node_type = 'n2-highmem-8'
                    
                    print(f"PGBENCH_JOB: Using default node type for job update: {default_node_type}")
                    
                    # Build new_cluster config
                    new_cluster_config = {
                        "spark_version": "14.3.x-scala2.12",
                        "node_type_id": default_node_type,
                        "num_workers": 0,
                        "spark_conf": {
                            "spark.databricks.cluster.profile": "singleNode",
                            "spark.master": "local[*]"
                        },
                        "custom_tags": {
                            "ResourceClass": "SingleNode",
                            "pgbench_job": "true"
                        },
                        "data_security_mode": "SINGLE_USER",
                        "single_user_name": current_user,
                        "init_scripts": [
                            {
                                "workspace": {
                                    "destination": init_script_path
                                }
                            }
                        ]
                    }
                    
                    # Add AWS-specific EBS volumes for m6i instances
                    if cloud == 'aws' and 'm6i' in default_node_type:
                        new_cluster_config["aws_attributes"] = {
                            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                            "ebs_volume_count": 1,
                            "ebs_volume_size": 100
                        }
                    
                    # Build updated job configuration
                    job_payload = {
                        "job_id": int(job_id),
                        "new_settings": {
                            "name": job_name,
                            "tasks": [
                                {
                                    "task_key": "pgbench_test",
                                    "notebook_task": {
                                        "notebook_path": notebook_path,
                                        "base_parameters": {}
                                    },
                                    "new_cluster": new_cluster_config,
                                    "timeout_seconds": 3600
                                }
                            ],
                            "max_concurrent_runs": 1,
                            "timeout_seconds": 3600
                        }
                    }
                    
                    print(f"PGBENCH_JOB: Updating job with cloud-specific config for {cloud}")
                    client.api_client.do('POST', '/api/2.1/jobs/update', body=job_payload)
                    print(f"PGBENCH_JOB: Updated job {job_id} to use job cluster")
                    return job_id
                
                # Update for interactive cluster (using SDK)
                client.jobs.update(
                    job_id=int(job_id),
                    new_settings=updated_settings
                )
                logger.info(f"PGBENCH_JOB: Updated job {job_id} to use interactive cluster {cluster_id}")
                return job_id
            
            
            # Job doesn't exist, create it
            logger.info(f"PGBENCH_JOB: Job not found, creating new job '{job_name}'")
            
            # Notebook path (already uploaded in submit_pgbench_job)
            notebook_path = "/Shared/pgbench_resources/pgbench_parameterized"
            
            if cluster_id and cluster_id.strip():
                # Use existing cluster
                logger.info(f"PGBENCH_JOB: Creating job with existing cluster {cluster_id}")
                
                job_settings = JobSettings(
                    name=job_name,
                    tasks=[
                        Task(
                            task_key="pgbench_test",
                            notebook_task=NotebookTask(
                                notebook_path=notebook_path,
                                base_parameters={}  # Parameters provided at runtime
                            ),
                            existing_cluster_id=cluster_id,
                            timeout_seconds=3600
                        )
                    ],
                    max_concurrent_runs=1,
                    timeout_seconds=3600
                )
                
                job = client.jobs.create(
                    name=job_settings.name,
                    tasks=job_settings.tasks,
                    max_concurrent_runs=job_settings.max_concurrent_runs,
                    timeout_seconds=job_settings.timeout_seconds
                )
                job_id = str(job.job_id)
                
            else:
                # Create with auto job cluster
                print(f"PGBENCH_JOB: Creating job with auto job cluster")
                
                # Get service principal
                current_user = self._get_current_service_principal()
                
                # Get init script path
                init_script_path = self._upload_init_script()
                
                # Detect cloud and select appropriate node type
                cloud = self._detect_cloud_provider()
                print(f"PGBENCH_JOB: Detected cloud for job creation: {cloud}")
                
                # Use default medium instance for job creation (actual sizing happens at run time)
                if cloud == 'aws':
                    default_node_type = 'm6i.2xlarge'
                elif cloud == 'azure':
                    default_node_type = 'Standard_E8s_v3'
                else:  # gcp
                    default_node_type = 'n2-highmem-8'
                
                print(f"PGBENCH_JOB: Using default node type for job creation: {default_node_type}")
                
                # Build new_cluster config
                new_cluster_config = {
                    "spark_version": "14.3.x-scala2.12",
                    "node_type_id": default_node_type,
                    "num_workers": 0,
                    "spark_conf": {
                        "spark.databricks.cluster.profile": "singleNode",
                        "spark.master": "local[*]"
                    },
                    "custom_tags": {
                        "ResourceClass": "SingleNode",
                        "pgbench_job": "true"
                    },
                    "data_security_mode": "SINGLE_USER",
                    "single_user_name": current_user,
                    "init_scripts": [
                        {
                            "workspace": {
                                "destination": init_script_path
                            }
                        }
                    ]
                }
                
                # Add AWS-specific EBS volumes for m6i instances
                if cloud == 'aws' and 'm6i' in default_node_type:
                    new_cluster_config["aws_attributes"] = {
                        "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                        "ebs_volume_count": 1,
                        "ebs_volume_size": 100
                    }
                
                # Build job configuration
                job_payload = {
                    "name": job_name,
                    "tasks": [
                        {
                            "task_key": "pgbench_test",
                            "notebook_task": {
                                "notebook_path": notebook_path,
                                "base_parameters": {}  # Parameters provided at runtime
                            },
                            "new_cluster": new_cluster_config,
                            "timeout_seconds": 3600
                        }
                    ],
                    "max_concurrent_runs": 1,
                    "timeout_seconds": 3600
                }
                
                response = client.api_client.do(
                    'POST',
                    '/api/2.1/jobs/create',
                    body=job_payload
                )
                
                job_id = str(response.get("job_id"))
            
            logger.info(f"PGBENCH_JOB: Created job {job_id}")
            return job_id
            
        except Exception as e:
            logger.error(f"PGBENCH_JOB: Failed to get/create job: {e}")
            raise
    
    
    def submit_pgbench_job(self, 
                                      pghost: str,
                                      pgport: int,
                                      pgdatabase: str,
                                      pguser: str,
                                      pgpassword: str,
                                      pgsslmode: str,
                                      cluster_id: Optional[str],
                                      pgbench_config: Dict[str, Any],
                                      query_configs: Optional[List[Dict[str, Any]]] = None,
                                      query_workspace_path: Optional[str] = None) -> Dict[str, Any]:
        """Submit a pgbench job using PostgreSQL credentials.
        
        Unified entry point for both provisioned and autoscaling Lakebase:
        connect with pghost, pguser, pgpassword. Same customer table schema and
        queries are used for both instance types.
        
        Args:
            pghost: PostgreSQL host endpoint
            pgport: PostgreSQL port
            pgdatabase: Database name
            pguser: PostgreSQL username
            pgpassword: PostgreSQL password
            pgsslmode: SSL mode
            cluster_id: Cluster ID to run on (None for auto job cluster)
            pgbench_config: pgbench configuration parameters
            query_configs: List of query configurations (for upload approach)
            query_workspace_path: Workspace path to queries folder (for workspace approach)
            
        Returns:
            Dictionary with job_id, run_id, job_name, and workspace URLs
        """
        try:
            print(f"PGBENCH_JOB: Submitting job with pghost={pghost}, pguser={pguser}")
            
            # Use fixed notebook path
            notebook_path = "/Shared/pgbench_resources/pgbench_parameterized"
            
            # Upload latest notebook version
            print(f"PGBENCH_JOB: Uploading latest notebook version to {notebook_path}")
            import os
            current_dir = os.path.dirname(os.path.abspath(__file__))
            parent_dir = os.path.dirname(current_dir)
            
            # Try to find the notebook file
            possible_paths = [
                os.path.join(current_dir, '..', '..', 'notebooks', 'pgbench_parameterized.ipynb'),
                os.path.join(parent_dir, '..', 'notebooks', 'pgbench_parameterized.ipynb'),
                '/app/notebooks/pgbench_parameterized.ipynb',
            ]
            
            notebook_path_local = None
            for path in possible_paths:
                abs_path = os.path.abspath(path)
                if os.path.exists(abs_path):
                    notebook_path_local = abs_path
                    print(f"PGBENCH_JOB: Found notebook at: {notebook_path_local}")
                    break
            
            if notebook_path_local:
                with open(notebook_path_local, 'r') as f:
                    notebook_content = f.read()
                self.upload_notebook(notebook_path, notebook_content)
                print(f"PGBENCH_JOB: Successfully uploaded notebook")
            else:
                print(f"PGBENCH_JOB: WARNING - Could not find notebook file, using existing version")
            
            # Build parameters with PostgreSQL credentials
            parameters = {
                # Database info
                "lakebase_instance_name": pghost,  # Use pghost as identifier
                "database_name": pgdatabase,
                
                # PostgreSQL connection parameters
                "pghost": pghost,
                "pgport": str(pgport),
                "pgdatabase": pgdatabase,
                "pguser": pguser,
                "pgpassword": pgpassword,
                "pgsslmode": pgsslmode,
                
                # pgbench configuration
                "pgbench_clients": str(pgbench_config.get("pgbench_clients", 8)),
                "pgbench_jobs": str(pgbench_config.get("pgbench_jobs", 8)),
                "pgbench_duration": str(pgbench_config.get("pgbench_duration", 30)),
                "pgbench_progress_interval": str(pgbench_config.get("pgbench_progress_interval", 5)),
                "pgbench_protocol": pgbench_config.get("pgbench_protocol", "prepared"),
                "pgbench_per_statement_latency": str(pgbench_config.get("pgbench_per_statement_latency", True)),
                "pgbench_detailed_logging": str(pgbench_config.get("pgbench_detailed_logging", True)),
                "pgbench_connect_per_transaction": str(pgbench_config.get("pgbench_connect_per_transaction", False)),
            }
            
            # Handle query source
            if query_workspace_path:
                parameters["query_workspace_path"] = query_workspace_path
                query_source = "workspace"
            elif query_configs:
                # Upload queries to workspace (same logic as provisioned)
                query_json = json.dumps(query_configs)
                
                if len(query_json) > 8192:  # 8KB threshold
                    # Upload to workspace
                    query_folder_path = f"/Shared/pgbench_resources/queries_{int(time.time())}"
                    self._upload_queries_to_workspace(query_configs, query_folder_path)
                    parameters["query_workspace_path"] = query_folder_path
                    query_source = "workspace"
                else:
                    # Pass inline (note: singular 'query_config' to match notebook parameter)
                    parameters["query_config"] = query_json
                    query_source = "inline"
            else:
                raise ValueError("Either query_configs or query_workspace_path must be provided")
            
            print(f"PGBENCH_JOB: Query source: {query_source}")
            
            # Get or create unified pgbench job
            job_id = self._get_or_create_pgbench_job(cluster_id)
            
            # Run the job with parameters
            run_id = self.run_job(job_id, parameters)
            
            # Job name for response (namespaced by app)
            import os
            app_name = os.getenv('DATABRICKS_APP_NAME', 'lakebase_app')
            job_name = f"{app_name}_pgbench_job"
            
            # Generate workspace links
            workspace_url = self._get_workspace_url()
            job_run_url = f"{workspace_url}#job/{job_id}/run/{run_id}"
            job_url = f"{workspace_url}#job/{job_id}"
            
            result = {
                "job_id": job_id,
                "run_id": run_id,
                "job_name": job_name,
                "notebook_path": notebook_path,
                "status": "submitted",
                "job_run_url": job_run_url,
                "job_url": job_url,
                "workspace_url": workspace_url
            }
            
            logger.info(f"PGBENCH_JOB: Job submitted successfully: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to submit pgbench job: {e}")
            raise
    
    def delete_job(self, job_id: str) -> bool:
        """Delete a Databricks job
        
        Args:
            job_id: ID of job to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            client = self._get_client()
            client.jobs.delete(int(job_id))
            logger.info(f"Successfully deleted job {job_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete job {job_id}: {e}")
            return False
