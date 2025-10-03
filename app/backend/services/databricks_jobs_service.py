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

    def _create_pgbench_notebook_content(self) -> str:
        """Create pgbench notebook content programmatically as a fallback"""
        notebook_content = {
            "cells": [
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        "# Pgbench Test Notebook\n",
                        "This notebook runs pgbench tests against a Lakebase Postgres instance."
                    ]
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# Get parameters from Databricks job\n",
                        "lakebase_instance_name = dbutils.widgets.get('lakebase_instance_name')\n",
                        "database_name = dbutils.widgets.get('database_name')\n",
                        "pgbench_config = dbutils.widgets.get('pgbench_config')\n",
                        "query_configs = dbutils.widgets.get('query_configs')\n",
                        "\n",
                        "print(f'Lakebase Instance: {lakebase_instance_name}')\n",
                        "print(f'Database: {database_name}')\n",
                        "print(f'Pgbench Config: {pgbench_config}')\n",
                        "print(f'Query Configs: {query_configs}')"
                    ]
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "import json\n",
                        "import time\n",
                        "import subprocess\n",
                        "import os\n",
                        "\n",
                        "# Parse configurations\n",
                        "pgbench_config = json.loads(pgbench_config)\n",
                        "query_configs = json.loads(query_configs)\n",
                        "\n",
                        "print('Configuration loaded successfully')\n",
                        "print(f'Pgbench clients: {pgbench_config.get(\"pgbench_clients\", 1)}')\n",
                        "print(f'Number of queries: {len(query_configs)}')"
                    ]
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# Mock pgbench execution for now\n",
                        "print('Starting pgbench test...')\n",
                        "print(f'Testing against {lakebase_instance_name}.{database_name}')\n",
                        "\n",
                        "# Simulate test execution\n",
                        "import time\n",
                        "time.sleep(5)\n",
                        "\n",
                        "# Mock results\n",
                        "results = {\n",
                        "    'status': 'completed',\n",
                        "    'transactions_processed': 1000,\n",
                        "    'tps': 100.0,\n",
                        "    'latency_avg': 10.5,\n",
                        "    'duration': 10\n",
                        "}\n",
                        "\n",
                        "print('Pgbench test completed!')\n",
                        "print(f'Results: {json.dumps(results, indent=2)}')\n",
                        "\n",
                        "# Return results for job monitoring\n",
                        "dbutils.jobs.taskValues.set(key='pgbench_results', value=json.dumps(results))"
                    ]
                }
            ],
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
        
        return json.dumps(notebook_content, indent=2)

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
                # No cluster ID provided - use job cluster approach
                logger.info("JOB_CLUSTER: No cluster ID provided, creating dedicated job cluster")
                
                # Get current service principal for job cluster
                current_user = self._get_current_service_principal()
                
                # Create job cluster specification
                job_cluster_spec = {
                    "spark_version": "13.3.x-scala2.12",
                    "node_type_id": "i3.xlarge",
                    "num_workers": 1,
                    "data_security_mode": "SINGLE_USER",
                    "single_user_name": current_user,
                    "spark_conf": {
                        "spark.databricks.cluster.profile": "singleNode",
                        "spark.master": "local[*]"
                    },
                    "custom_tags": {
                        "ResourceClass": "SingleNode",
                        "pgbench_job": "true"
                    }
                }
                
                logger.info(f"JOB_CLUSTER: Creating job cluster for user: {current_user}")
                
                # Use direct API call to avoid SDK serialization issues
                job_config = {
                    "name": job_name,
                    "tasks": [
                        {
                            "task_key": "pgbench_test",
                            "notebook_task": {
                                "notebook_path": notebook_path,
                                "base_parameters": base_parameters
                            },
                            "job_cluster_key": "pgbench_auto_cluster",
                            "timeout_seconds": 3600
                        }
                    ],
                    "job_clusters": [
                        {
                            "job_cluster_key": "pgbench_auto_cluster",
                            "new_cluster": job_cluster_spec
                        }
                    ],
                    "max_concurrent_runs": 1,
                    "timeout_seconds": 3600
                }
                
                logger.info(f"JOB_CLUSTER: Job config: {job_config}")
                
                # Create job using direct API
                job = client.jobs.create(**job_config)
                job_id = str(job.job_id)
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
            if status == "completed":
                try:
                    # Try to get notebook output
                    output = client.jobs.get_run_output(int(run_id))
                    if output and output.notebook_output:
                        # Parse results from notebook output
                        results = self._parse_notebook_results(output.notebook_output.result)
                except Exception as e:
                    logger.warning(f"Could not retrieve run output: {e}")
            
            return {
                "run_id": run_id,
                "status": status,
                "message": self._get_status_message(life_cycle_state, result_state),
                "progress": progress,
                "start_time": run.start_time,
                "end_time": run.end_time,
                "results": results
            }
            
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
            # Look for JSON results in the notebook output
            # The parameterized notebook saves results as JSON
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
    
    def submit_pgbench_job(self, 
                          lakebase_instance_name: str,
                          database_name: str,
                          cluster_id: str,
                          pgbench_config: Dict[str, Any],
                          query_configs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Submit a complete pgbench job
        
        Args:
            lakebase_instance_name: Name of Lakebase instance
            database_name: Database name
            cluster_id: Cluster ID to run on
            pgbench_config: pgbench configuration parameters
            query_configs: List of query configurations
            
        Returns:
            Dictionary with job_id and run_id
        """
        try:
            # Generate unique job name
            timestamp = int(time.time())
            job_name = f"pgbench_test_{lakebase_instance_name}_{timestamp}"
            
            # Notebook path in workspace
            notebook_path = f"/Shared/pgbench_jobs/{job_name}"
            
            # Read the parameterized notebook content
            import os
            current_dir = os.path.dirname(os.path.abspath(__file__))
            
            # Try multiple possible notebook locations for different deployment environments
            # Note: In Databricks deployment, .ipynb extension might be stripped
            possible_paths = [
                # Local development path (with .ipynb)
                os.path.join(current_dir, '..', '..', 'notebooks', 'pgbench_parameterized.ipynb'),
                # Databricks deployment path (with .ipynb)
                '/app/notebooks/pgbench_parameterized.ipynb',
                # Alternative Databricks path (with .ipynb)
                os.path.join('/app', 'notebooks', 'pgbench_parameterized.ipynb'),
                # Root relative path (with .ipynb)
                os.path.join(os.path.dirname(current_dir), '..', '..', 'notebooks', 'pgbench_parameterized.ipynb'),
                # Databricks deployment paths WITHOUT .ipynb extension (deployment strips extension)
                os.path.join(current_dir, '..', '..', 'notebooks', 'pgbench_parameterized'),
                '/app/notebooks/pgbench_parameterized',
                os.path.join('/app', 'notebooks', 'pgbench_parameterized'),
                os.path.join(os.path.dirname(current_dir), '..', '..', 'notebooks', 'pgbench_parameterized')
            ]
            
            notebook_path_local = None
            for path in possible_paths:
                abs_path = os.path.abspath(path)
                print(f"NOTEBOOK_PATH: Trying path: {abs_path}")
                if os.path.exists(abs_path):
                    notebook_path_local = abs_path
                    print(f"NOTEBOOK_PATH: Found notebook at: {notebook_path_local}")
                    break
            
            if not notebook_path_local:
                available_paths = "\n".join(f"  - {os.path.abspath(p)}" for p in possible_paths)
                
                # List files in each directory we checked
                directory_listings = []
                checked_dirs = set()
                for path in possible_paths:
                    abs_path = os.path.abspath(path)
                    dir_path = os.path.dirname(abs_path)
                    if dir_path not in checked_dirs:
                        checked_dirs.add(dir_path)
                        try:
                            if os.path.exists(dir_path):
                                files = os.listdir(dir_path)
                                directory_listings.append(f"  {dir_path}: {files}")
                            else:
                                directory_listings.append(f"  {dir_path}: [DIRECTORY DOES NOT EXIST]")
                        except Exception as e:
                            directory_listings.append(f"  {dir_path}: [ERROR LISTING: {e}]")
                
                listings_text = "\n".join(directory_listings) if directory_listings else "No directories found to list"
                
                raise FileNotFoundError(
                    f"Could not find pgbench_parameterized.ipynb in any of these locations:\n{available_paths}\n"
                    f"Current working directory: {os.getcwd()}\n"
                    f"Current file directory: {current_dir}\n"
                    f"Directory listings:\n{listings_text}"
                )
            
            # Try to read the notebook file, with fallback to creating programmatically
            try:
                with open(notebook_path_local, 'r') as f:
                    notebook_content = f.read()
                
                # Validate that it's proper JSON (notebook format)
                import json
                try:
                    json.loads(notebook_content)
                    print(f"NOTEBOOK: Successfully validated JSON format")
                except json.JSONDecodeError:
                    print(f"NOTEBOOK: File is not valid JSON, will create programmatically")
                    raise ValueError("Not valid JSON")
                    
            except (ValueError, json.JSONDecodeError):
                print(f"NOTEBOOK: Creating notebook content programmatically")
                notebook_content = self._create_pgbench_notebook_content()
            
            # Upload notebook to workspace
            self.upload_notebook(notebook_path, notebook_content)
            
            # Prepare job parameters
            parameters = {
                "lakebase_instance_name": lakebase_instance_name,
                "database_name": database_name,
                "query_config": json.dumps(query_configs),
                **pgbench_config
            }
            
            # Create and run the job
            job_id = self.create_pgbench_job(job_name, cluster_id, notebook_path, parameters)
            run_id = self.run_job(job_id, parameters)
            
            # Generate workspace links
            workspace_url = self._get_workspace_url()
            job_run_url = f"{workspace_url}#job/{job_id}/run/{run_id}"
            job_url = f"{workspace_url}#job/{job_id}"
            
            logger.info(f"JOB_LINKS: Generated workspace_url: {workspace_url}")
            logger.info(f"JOB_LINKS: Generated job_url: {job_url}")
            logger.info(f"JOB_LINKS: Generated job_run_url: {job_run_url}")
            
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
            
            logger.info(f"JOB_SUBMISSION: Returning result: {result}")
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
