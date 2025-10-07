#!/usr/bin/env python3
"""
Databricks Deployment Service

Handles direct deployment of Lakebase instances using Databricks SDK
instead of bundle deploy. Creates database instances, catalogs, and synced tables.
"""

import asyncio
import logging
from datetime import timedelta
from typing import Dict, Any, List, Optional, Callable
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

# Import database service classes with error handling
try:
    from databricks.sdk.service.database import (
        DatabaseInstance,
        DatabaseCatalog,
        SyncedDatabaseTable,
        SyncedTableSpec,
        NewPipelineSpec,
        SyncedTableSchedulingPolicy
    )
    DATABASE_SERVICE_AVAILABLE = True
except ImportError as e:
    # Create logger here since it's not defined yet
    import logging
    logger = logging.getLogger(__name__)
    logger.warning(f"Databricks SDK database classes not available: {e}")
    logger.warning("Database deployment features will be disabled")
    
    # Create dummy classes to prevent import errors
    class DatabaseInstance: pass
    class DatabaseCatalog: pass
    class SyncedDatabaseTable: pass
    class SyncedTableSpec: pass
    class NewPipelineSpec: pass
    class SyncedTableSchedulingPolicy: pass
    
    DATABASE_SERVICE_AVAILABLE = False

logger = logging.getLogger(__name__)

class DeploymentProgress:
    """Tracks deployment progress for UI updates"""
    def __init__(self):
        self.steps = []
        self.current_step = 0
        self.total_steps = 0
        self.status = "pending"  # pending, in_progress, completed, failed
        self.error_message = None

    def add_step(self, description: str):
        self.steps.append({"description": description, "status": "pending", "error": None, "details": ""})
        self.total_steps = len(self.steps)

    def start_step(self, index: int, details: str = ""):
        if index < len(self.steps):
            self.steps[index]["status"] = "in_progress"
            self.steps[index]["details"] = details
            self.current_step = index

    def update_step(self, index: int, details: str):
        if index < len(self.steps):
            self.steps[index]["details"] = details

    def complete_step(self, index: int, details: str = ""):
        if index < len(self.steps):
            self.steps[index]["status"] = "completed"
            self.steps[index]["details"] = details

    def fail_step(self, index: int, error: str):
        if index < len(self.steps):
            self.steps[index]["status"] = "failed"
            self.steps[index]["error"] = error
            self.steps[index]["details"] = f"Error: {error}"
            self.status = "failed"
            self.error_message = error

class DatabricksDeploymentService:
    """
    Service for deploying Lakebase instances directly using Databricks SDK
    """

    def __init__(self):
        self._workspace_client = None
        self._progress = None
        self._progress_callback = None

    def set_progress_callback(self, callback: Callable[[DeploymentProgress], None]):
        """Set callback function for progress updates"""
        self._progress_callback = callback

    def _update_progress(self):
        """Send progress update to callback if set"""
        if self._progress_callback and self._progress:
            self._progress_callback(self._progress)

    async def deploy_lakebase_instance(
        self,
        config: Dict[str, Any],
        profile: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Deploy a complete Lakebase instance with database, catalog, and synced tables

        Args:
            config: Configuration from the calculator form
            profile: Databricks profile name for authentication

        Returns:
            Deployment result with status and details
        """
        # Check if database service is available
        if not DATABASE_SERVICE_AVAILABLE:
            return {
                "status": "failed",
                "error": "Database service not available. Please update databricks-sdk to version 0.30.0 or later.",
                "details": "The deployment service requires databricks-sdk database service classes."
            }
            
        try:
            # Initialize progress tracking
            self._progress = DeploymentProgress()
            self._progress.status = "in_progress"

            # Plan deployment steps
            self._progress.add_step("Initializing Databricks connection")
            self._progress.add_step("Creating database instance")
            self._progress.add_step("Creating database catalog")
            self._progress.add_step("Creating synced delta tables")
            self._progress.add_step("Finalizing deployment")

            self._update_progress()

            # Step 1: Initialize Databricks connection
            self._progress.start_step(0, "Connecting to Databricks workspace...")
            self._update_progress()
            
            logger.info(f"DEBUG DEPLOY: About to initialize client with profile='{profile}'")
            logger.info(f"DEBUG DEPLOY: workspace_url from config: '{config.get('databricks_workspace_url')}'")
            
            await self._initialize_client(config.get('databricks_workspace_url'), profile)
            self._progress.complete_step(0, "Successfully connected to Databricks")
            self._update_progress()

            # Step 2: Create database instance
            self._progress.start_step(1, "Checking for existing database instance...")
            self._update_progress()
            instance = await self._create_database_instance(config)
            self._progress.complete_step(1, f"Database instance '{instance.name}' is ready")
            self._update_progress()

            # Step 3: Create database catalog
            self._progress.start_step(2, "Checking for existing database catalog...")
            self._update_progress()
            catalog = await self._create_catalog(config)
            self._progress.complete_step(2, f"Database catalog '{catalog['name']}' is ready")
            self._update_progress()

            # Step 4: Create synced delta tables
            self._progress.start_step(3, "Creating synced delta tables...")
            self._update_progress()
            # Use the instance name from the catalog to ensure consistency
            catalog_instance_name = catalog.get('database_instance_name', instance.name)
            if catalog_instance_name != instance.name:
                logger.info(f"Using existing catalog's instance name '{catalog_instance_name}' instead of '{instance.name}' for synced tables")
            tables = await self._create_synced_tables(config, catalog_instance_name)
            table_count = len([t for t in tables if t.get('status') in ['created', 'exists']])
            self._progress.complete_step(3, f"Processed {table_count} synced tables")
            self._update_progress()

            # Step 5: Finalize
            self._progress.start_step(4, "Finalizing deployment...")
            self._update_progress()
            self._progress.complete_step(4, "Deployment completed successfully")
            self._progress.status = "completed"
            self._update_progress()

            # Debug: Log available instance properties
            logger.info(f"Instance object attributes: {dir(instance)}")
            logger.info(f"Instance state: {getattr(instance, 'state', 'N/A')}")
            logger.info(f"Instance read_write_dns: {getattr(instance, 'read_write_dns', 'N/A')}")
            logger.info(f"Instance uid: {getattr(instance, 'uid', 'N/A')}")

            return {
                "success": True,
                "message": "Lakebase instance deployed successfully",
                "instance": {
                    "name": instance.name,
                    "id": getattr(instance, 'uid', None) or getattr(instance, 'id', None),
                    "host": getattr(instance, 'read_write_dns', None) or getattr(instance, 'host', None),
                    "port": getattr(instance, 'port', 5432),  # Default PostgreSQL port
                    "capacity": getattr(instance, 'capacity', None),
                    "state": str(instance.state) if hasattr(instance, 'state') and instance.state else "unknown",
                    "read_write_dns": getattr(instance, 'read_write_dns', None),
                    "read_only_dns": getattr(instance, 'read_only_dns', None),
                    "creation_time": getattr(instance, 'creation_time', None),
                    "node_count": getattr(instance, 'node_count', None)
                },
                "catalog": catalog,
                "tables": tables,
                "progress": self._progress.__dict__
            }

        except Exception as e:
            error_msg = f"Deployment failed: {str(e)}"
            logger.error(error_msg, exc_info=True)

            if self._progress:
                self._progress.fail_step(self._progress.current_step, error_msg)
                self._update_progress()

            return {
                "success": False,
                "message": error_msg,
                "progress": self._progress.__dict__ if self._progress else None
            }

    async def _initialize_client(self, workspace_url: Optional[str], profile: Optional[str]):
        """Initialize Databricks workspace client"""
        try:
            # Priority: profile > workspace_url > environment variables > default
            import os
            
            logger.info(f"DEBUG: _initialize_client called with profile='{profile}', workspace_url='{workspace_url}'")
            
            if profile:
                # User specified a profile - use it (highest priority)
                logger.info(f"Using Databricks profile: {profile}")
                if workspace_url:
                    self._workspace_client = WorkspaceClient(profile=profile, host=workspace_url)
                else:
                    self._workspace_client = WorkspaceClient(profile=profile)
            elif workspace_url:
                # Workspace URL provided but no profile
                logger.info(f"Using workspace URL: {workspace_url}")
                self._workspace_client = WorkspaceClient(host=workspace_url)
            else:
                # No profile or workspace URL - try environment variables
                host = os.getenv('DATABRICKS_HOST')
                token = os.getenv('DATABRICKS_TOKEN')
                
                # Only use env vars if token is not a placeholder
                if host and token and token != 'your_token_here':
                    logger.info("Using environment variables for Databricks authentication")
                    self._workspace_client = WorkspaceClient(host=host, token=token)
                else:
                    # Fall back to default authentication (CLI config)
                    logger.info("Using default Databricks authentication")
                    self._workspace_client = WorkspaceClient()

            # Test connection
            current_user = self._workspace_client.current_user.me()
            logger.info(f"Connected to Databricks as user: {current_user.user_name}")

        except DatabricksError as e:
            raise Exception(f"Failed to connect to Databricks: {str(e)}")

    async def _create_database_instance(self, config: Dict[str, Any]) -> DatabaseInstance:
        """Create Lakebase database instance"""
        try:
            instance_name = config.get('lakebase_instance_name', 'lakebase-accelerator-instance')
            database_name = config.get('database_name', 'databricks_postgres')

            # Check if instance already exists
            try:
                existing_instance = self._workspace_client.database.get_database_instance(name=instance_name)
                current_state = str(existing_instance.state).upper() if existing_instance.state else 'UNKNOWN'
                logger.info(f"Database instance '{instance_name}' already exists with state: {current_state}")

                # Update progress with current state
                self._progress.update_step(1, f"Found existing instance '{instance_name}' (state: {current_state})")
                self._update_progress()

                # Only wait if instance is in a transitional state
                if current_state in ['STARTING', 'CREATING']:
                    self._progress.update_step(1, f"Instance is {current_state}, waiting for it to become available...")
                    self._update_progress()
                    try:
                        await self._wait_for_instance_available(instance_name, timeout=120)  # Wait max 2 minutes
                        existing_instance = self._workspace_client.database.get_database_instance(name=instance_name)
                        self._progress.update_step(1, f"Instance is now {existing_instance.state}")
                        self._update_progress()
                    except Exception as e:
                        logger.warning(f"Timeout waiting for instance, proceeding anyway: {str(e)}")
                        self._progress.update_step(1, f"Timeout waiting, proceeding with current state: {current_state}")
                        self._update_progress()
                elif current_state == 'AVAILABLE':
                    self._progress.update_step(1, "Instance is already available")
                    self._update_progress()
                else:
                    self._progress.update_step(1, f"Instance state is {current_state}, proceeding")
                    self._update_progress()

                return existing_instance
            except DatabricksError:
                pass  # Instance doesn't exist, create it

            # Create new database instance
            cu_count = config.get('recommended_cu', 1)

            # Map CU count to valid capacity values (CU_1, CU_2, CU_4, CU_8)
            if cu_count <= 1:
                capacity = "CU_1"
            elif cu_count <= 2:
                capacity = "CU_2"
            elif cu_count <= 4:
                capacity = "CU_4"
            else:
                capacity = "CU_8"

            # Create database instance with proper parameters
            database_instance = DatabaseInstance(
                name=instance_name,
                capacity=capacity,
                enable_readable_secondaries=cu_count > 1,  # Enable if more than 1 CU
                retention_window_in_days=7  # Default retention
            )

            instance_waiter = self._workspace_client.database.create_database_instance(database_instance)

            # Wait for instance to be ready
            instance = instance_waiter.result(timeout=timedelta(minutes=5))

            logger.info(f"Created database instance: {instance.name}")
            return instance

        except DatabricksError as e:
            raise Exception(f"Failed to create database instance: {str(e)}")
    # Wait for 20 minutes for the instance to become available
    async def _wait_for_instance_available(self, instance_name: str, timeout: int = 1200):
        """Wait for database instance to become available"""
        start_time = asyncio.get_event_loop().time()

        while True:
            try:
                instance = self._workspace_client.database.get_database_instance(name=instance_name)
                current_state = str(instance.state).upper() if instance.state else 'UNKNOWN'

                if current_state == 'AVAILABLE':
                    logger.info(f"Instance {instance_name} is now available")
                    return

                elapsed = int(asyncio.get_event_loop().time() - start_time)
                if elapsed > timeout:
                    raise Exception(f"Timeout after {timeout}s waiting for instance {instance_name} to become available")

                # Update progress with current wait status
                self._progress.update_step(1, f"Instance state: {current_state}, waiting... ({elapsed}s/{timeout}s)")
                self._update_progress()

                logger.info(f"Instance {instance_name} state: {current_state}, waiting... ({elapsed}s)")
                await asyncio.sleep(10)  # Check every 10 seconds

            except DatabricksError as e:
                raise Exception(f"Error checking instance status: {str(e)}")


    async def _create_catalog(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Create Database Catalog (not Unity Catalog)"""
        try:
            catalog_name = config.get('uc_catalog_name', 'lakebase_accelerator_catalog')
            instance_name = config.get('lakebase_instance_name', 'lakebase-accelerator-instance')
            database_name = config.get('database_name', 'databricks_postgres')

            # Check if database catalog already exists
            try:
                existing_catalog = self._workspace_client.database.get_database_catalog(name=catalog_name)
                existing_instance_name = getattr(existing_catalog, 'database_instance_name', instance_name)
                logger.info(f"Database catalog '{catalog_name}' already exists, associated with instance: {existing_instance_name}")
                return {
                    "name": existing_catalog.name,
                    "database_name": getattr(existing_catalog, 'database_name', database_name),
                    "database_instance_name": existing_instance_name
                }
            except DatabricksError:
                pass  # Catalog doesn't exist, create it

            # Create new database catalog
            database_catalog = DatabaseCatalog(
                name=catalog_name,
                database_instance_name=instance_name,
                database_name=database_name,
                create_database_if_not_exists=True
            )

            catalog = self._workspace_client.database.create_database_catalog(database_catalog)

            logger.info(f"Created database catalog: {catalog.name}")
            return {
                "name": catalog.name,
                "database_name": catalog.database_name,
                "database_instance_name": getattr(catalog, 'database_instance_name', instance_name)
            }

        except DatabricksError as e:
            raise Exception(f"Failed to create database catalog: {str(e)}")

    async def _create_synced_tables(self, config: Dict[str, Any], instance_name: str) -> List[Dict[str, Any]]:
        """Create synced delta tables"""
        try:
            tables = config.get('tables', [])
            catalog_name = config.get('uc_catalog_name', 'lakebase_accelerator_catalog')
            created_tables = []

            for i, table_config in enumerate(tables):
                try:
                    source_full_table_name = table_config.get('table_name')
                    primary_keys = table_config.get('primary_keys', [])
                    sync_policy = table_config.get('sync_policy', 'SNAPSHOT')

                    if not source_full_table_name:
                        logger.warning("Skipping table with no name")
                        continue

                    # Extract just the table name from the source (last part after dots)
                    table_name = source_full_table_name.split('.')[-1]

                    # Update progress for current table
                    self._progress.update_step(3, f"Processing table {i+1}/{len(tables)}: {table_name}")
                    self._update_progress()

                    # Create target full table name for Unity Catalog in lakebase catalog
                    target_full_table_name = f"{catalog_name}.public.{table_name}"
                    logger.info(f"Debug: catalog_name='{catalog_name}', source_full_table_name='{source_full_table_name}', table_name='{table_name}', target_full_table_name='{target_full_table_name}'")

                    # Check if table already exists
                    try:
                        existing_table = self._workspace_client.database.get_synced_database_table(name=target_full_table_name)
                        logger.info(f"Synced table '{target_full_table_name}' already exists")
                        created_tables.append({
                            "name": existing_table.name,
                            "status": "exists",
                            "database_instance": getattr(existing_table, 'database_instance_name', instance_name)
                        })
                        continue
                    except DatabricksError as e:
                        logger.debug(f"Table {target_full_table_name} doesn't exist, will create it: {str(e)}")
                        pass  # Table doesn't exist, create it

                    # Create synced database table with proper spec
                    logger.info(f"Creating synced table: {target_full_table_name}")

                    # Map sync_policy to the SDK enum
                    if sync_policy.upper() == 'CONTINUOUS':
                        scheduling_policy = SyncedTableSchedulingPolicy.CONTINUOUS
                    elif sync_policy.upper() == 'TRIGGERED':
                        scheduling_policy = SyncedTableSchedulingPolicy.TRIGGERED
                    else:  # Default to SNAPSHOT
                        scheduling_policy = SyncedTableSchedulingPolicy.SNAPSHOT

                    # Create the sync table spec
                    sync_spec = SyncedTableSpec(
                        source_table_full_name=source_full_table_name,
                        primary_key_columns=primary_keys,
                        scheduling_policy=scheduling_policy,
                        new_pipeline_spec=NewPipelineSpec(
                            storage_catalog=config.get('storage_catalog', 'main'),
                            storage_schema=config.get('storage_schema', 'default')
                        )
                    )

                    synced_table = SyncedDatabaseTable(
                        name=target_full_table_name,
                        database_instance_name=instance_name,
                        spec=sync_spec
                    )

                    created_table = self._workspace_client.database.create_synced_database_table(synced_table)

                    logger.info(f"Successfully created synced table: {created_table.name}")
                    created_tables.append({
                        "name": created_table.name,
                        "status": "created",
                        "database_instance": getattr(created_table, 'database_instance_name', instance_name)
                    })

                except DatabricksError as e:
                    logger.error(f"Failed to create table {source_full_table_name}: {str(e)}")
                    created_tables.append({
                        "name": source_full_table_name,
                        "status": "failed",
                        "error": str(e)
                    })

            return created_tables

        except Exception as e:
            raise Exception(f"Failed to create synced tables: {str(e)}")