#!/usr/bin/env python3
"""
Lakebase Postgres Cost Estimator

This script calculates the cost of running Lakebase Postgres instances based on
workload characteristics and usage patterns.

Usage:
    # From base directory:
    python app/backend/services/lakebase_cost_estimator.py --config sample_workload_config.yml --output cost_report.json
    
    # Note: Databricks credentials are now passed from the application,
    # not loaded from .env file
    
"""

import argparse
import concurrent.futures
import re
import json
import logging
import math
import os
import sys
from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional, List
from datetime import datetime
import yaml

# Configure logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from databricks import sql
    DATABRICKS_SQL_AVAILABLE = True
except ImportError:
    DATABRICKS_SQL_AVAILABLE = False
    logger.warning("databricks-sql-connector not available. Table size calculation will be skipped.")

try:
    from databricks.sdk import WorkspaceClient
    DATABRICKS_SDK_AVAILABLE = True
except Exception:
    DATABRICKS_SDK_AVAILABLE = False

def _parse_warehouse_id(http_path: str) -> Optional[str]:
    """Extract warehouse id from HTTP path like /sql/1.0/warehouses/<id>."""
    if not http_path:
        return None
    match = re.search(r"/warehouses/([A-Za-z0-9\-]+)", http_path.strip())
    return match.group(1) if match else None

def _warehouse_exists(http_path: str, profile: Optional[str] = None) -> tuple[bool, str]:
    """Check existence of a SQL warehouse using Databricks SDK. Returns (exists, message)."""
    if not DATABRICKS_SDK_AVAILABLE:
        return True, "Databricks SDK not available; skipping existence check"
    wid = _parse_warehouse_id(http_path)
    if not wid:
        return False, "Invalid DATABRICKS_HTTP_PATH (cannot parse warehouse id)"
    try:
        client = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
        wh = client.warehouses.get(id=wid)
        return True, f"Warehouse {wid} exists (state={getattr(wh, 'state', 'unknown')})"
    except Exception as e:
        # If SDK cannot authenticate, don't block; report error and allow timeout path below
        return False, f"Warehouse check failed: {e}"


@dataclass
class WorkloadConfig:
    """Workload configuration from YAML file."""
    # Database Instance Unit
    bulk_writes_per_second: int
    continuous_writes_per_second: int
    reads_per_second: int
    number_of_readable_secondaries: int
    readable_secondary_size_cu: int
    
    # Database Storage
    data_stored_gb: float
    estimated_data_deleted_daily_gb: float
    
    # Delta Synchronization
    number_of_continuous_pipelines: int
    expected_data_per_sync_gb: float
    sync_mode: str
    sync_frequency: str
    
    # Optional promotion/discount
    promotion_percentage: float = 0.0  # 0.0 = no promotion, 50.0 = 50% off
    
    # Restore Windows
    restore_windows_days: int = 0


@dataclass
class CostBreakdown:
    """Detailed cost breakdown."""
    # CU calculations
    bulk_cu: float
    continuous_cu: float
    read_cu: float
    total_cu: float
    recommended_cu: int
    
    # Compute costs
    main_instance_cost: float
    readable_secondaries_cost: float
    total_compute_cost: float
    
    # Storage costs
    storage_cost: float
    
    # Delta sync costs
    continuous_sync_cost: float
    triggered_sync_cost: float
    total_sync_cost: float
    estimated_sync_time_hours: float
    
    # Total costs
    total_monthly_cost: float
    


class LakebaseCostEstimator:
    """Main cost estimation logic for Lakebase Postgres."""
    
    def __init__(self):
        # Cost constants (as per specifications)
        self.cu_base_cost = 3500 / 12  # $3500 per year / 12 months
        self.storage_cost_per_gb = 0.345  # $0.345 per GB per month
        self.continuous_pipeline_cost = 548  # $548 per continuous pipeline per month
        self.triggered_sync_cost_per_hour = 0.75  # $0.75 per hour for triggered sync
        
        # CU calculation constants
        self.bulk_cu_ratio = 14000  # rows per second per CU
        self.continuous_cu_ratio = 1500  # rows per second per CU
        self.read_cu_ratio = 10000  # QPS per CU
        
        # Sync throughput rates (GB per hour)
        self.sync_throughput = {
            "SNAPSHOT": {1: 54, 2: 108, 4: 216},
            "TRIGGERED": {1: 4.5, 2: 9, 4: 18}
        }
        
        # Frequency multipliers
        self.frequency_multipliers = {
            "Per week": 1,
            "Per day": 7,
            "Per hour": 168
        }
    
    def calculate_cu_requirements(self, config: WorkloadConfig) -> Dict[str, float]:
        """Calculate CU requirements based on workload."""
        # Calculate individual CU requirements
        bulk_cu = config.bulk_writes_per_second / self.bulk_cu_ratio
        continuous_cu = config.continuous_writes_per_second / self.continuous_cu_ratio
        read_cu = config.reads_per_second / self.read_cu_ratio
        
        # Total CU requirement
        total_cu = bulk_cu + continuous_cu + read_cu
        
        # Round up to nearest valid tier
        recommended_cu = self._round_up_to_valid_cu(total_cu)
        
        return {
            "bulk_cu": bulk_cu,
            "continuous_cu": continuous_cu,
            "read_cu": read_cu,
            "total_cu": total_cu,
            "recommended_cu": recommended_cu
        }
    
    def _round_up_to_valid_cu(self, total_cu: float) -> int:
        """Round up to nearest valid CU tier (1, 2, 4, or 8)."""
        if total_cu <= 1:
            return 1
        elif total_cu <= 2:
            return 2
        elif total_cu <= 4:
            return 4
        else:
            return 8
    
    def estimate_sync_time(self, expected_data_gb: float, cus: int, sync_mode: str) -> float:
        """
        Estimate sync time in hours.
        
        Parameters:
            expected_data_gb (float): Expected data to be written (GB).
            cus (int): Capacity Units (1, 2, or 4).
            sync_mode (str): "SNAPSHOT" or "TRIGGERED" (case-insensitive).
        
        Returns:
            float: Estimated time in hours.
        """
        # Fixed overhead: 10 minutes
        overhead_hours = 10 / 60
        
        # Normalize sync mode to uppercase
        sync_mode_upper = sync_mode.upper()
        if sync_mode_upper not in self.sync_throughput:
            raise ValueError("Invalid sync mode. Use 'SNAPSHOT' or 'TRIGGERED'.")
        if cus not in self.sync_throughput[sync_mode_upper]:
            raise ValueError("Invalid CU. Use 1, 2, or 4.")
        
        transfer_rate = self.sync_throughput[sync_mode_upper][cus]
        
        # Formula: overhead + (data ÷ rate)
        return overhead_hours + (expected_data_gb / transfer_rate)
    
    def calculate_compute_cost(self, config: WorkloadConfig, cu_requirements: Dict[str, float]) -> Dict[str, float]:
        """Calculate compute costs."""
        recommended_cu = cu_requirements["recommended_cu"]
        
        # Main instance cost
        main_instance_cost = self.cu_base_cost * recommended_cu
        
        # Readable secondaries cost
        readable_secondaries_cost = (
            self.cu_base_cost * 
            config.number_of_readable_secondaries * 
            config.readable_secondary_size_cu
        )
        
        # Total compute cost before promotion
        total_compute_cost_before_promotion = main_instance_cost + readable_secondaries_cost
        
        # Apply promotion if specified
        if config.promotion_percentage > 0:
            promotion_factor = 1.0 - (config.promotion_percentage / 100.0)
            main_instance_cost = main_instance_cost * promotion_factor
            readable_secondaries_cost = readable_secondaries_cost * promotion_factor
        
        # Total compute cost after promotion
        total_compute_cost = main_instance_cost + readable_secondaries_cost
        
        return {
            "main_instance_cost": main_instance_cost,
            "readable_secondaries_cost": readable_secondaries_cost,
            "total_compute_cost": total_compute_cost
        }
    
    def calculate_storage_cost(self, config: WorkloadConfig) -> float:
        """Calculate storage costs."""
        return config.data_stored_gb * self.storage_cost_per_gb
    
    def calculate_sync_cost(self, config: WorkloadConfig, cu_requirements: Dict[str, float]) -> Dict[str, float]:
        """Calculate Delta synchronization costs."""
        recommended_cu = cu_requirements["recommended_cu"]
        
        # Continuous synchronization costs
        continuous_sync_cost = self.continuous_pipeline_cost * config.number_of_continuous_pipelines
        
        # Triggered synchronization costs
        if config.expected_data_per_sync_gb > 0:
            sync_time_hours = self.estimate_sync_time(
                config.expected_data_per_sync_gb, 
                min(recommended_cu, 4),  # Cap at 4 CU for sync calculations
                config.sync_mode
            )
            
            frequency_multiplier = self.frequency_multipliers.get(config.sync_frequency, 1)
            triggered_sync_cost = (
                4 * # 4 is the number of weeks in a month
                self.triggered_sync_cost_per_hour * 
                sync_time_hours * 
                frequency_multiplier # 7 if run per day, 168 if run per hour, 1 if run per week
            )
        else:
            triggered_sync_cost = 0
        
        total_sync_cost = continuous_sync_cost + triggered_sync_cost
        
        # Calculate sync time for display
        sync_time_hours = 0
        if config.expected_data_per_sync_gb > 0:
            sync_time_hours = self.estimate_sync_time(
                config.expected_data_per_sync_gb, 
                min(recommended_cu, 4),  # Cap at 4 CU for sync calculations
                config.sync_mode
            )
        
        return {
            "continuous_sync_cost": continuous_sync_cost,
            "triggered_sync_cost": triggered_sync_cost,
            "total_sync_cost": total_sync_cost,
            "estimated_sync_time_hours": sync_time_hours
        }
    
    def calculate_total_cost(self, config: WorkloadConfig) -> CostBreakdown:
        """Calculate total cost breakdown."""
        # Calculate CU requirements
        cu_requirements = self.calculate_cu_requirements(config)
        
        # Calculate individual cost components
        compute_costs = self.calculate_compute_cost(config, cu_requirements)
        storage_cost = self.calculate_storage_cost(config)
        sync_costs = self.calculate_sync_cost(config, cu_requirements)
        
        # Calculate total monthly cost
        total_monthly_cost = (
            compute_costs["total_compute_cost"] + 
            storage_cost + 
            sync_costs["total_sync_cost"]
        )
        
        
        return CostBreakdown(
            # CU calculations
            bulk_cu=cu_requirements["bulk_cu"],
            continuous_cu=cu_requirements["continuous_cu"],
            read_cu=cu_requirements["read_cu"],
            total_cu=cu_requirements["total_cu"],
            recommended_cu=cu_requirements["recommended_cu"],
            
            # Compute costs
            main_instance_cost=compute_costs["main_instance_cost"],
            readable_secondaries_cost=compute_costs["readable_secondaries_cost"],
            total_compute_cost=compute_costs["total_compute_cost"],
            
            # Storage costs
            storage_cost=storage_cost,
            
            # Delta sync costs
            continuous_sync_cost=sync_costs["continuous_sync_cost"],
            triggered_sync_cost=sync_costs["triggered_sync_cost"],
            total_sync_cost=sync_costs["total_sync_cost"],
            estimated_sync_time_hours=sync_costs["estimated_sync_time_hours"],
            
            # Total costs
            total_monthly_cost=total_monthly_cost
        )


def load_workload_config(config_file: str) -> tuple[WorkloadConfig, List[Dict[str, Any]]]:
    """Load workload configuration from YAML file."""
    with open(config_file, 'r') as f:
        config_data = yaml.safe_load(f)
    
    # Extract database instance config
    db_instance = config_data.get('database_instance', {})
    
    # Extract storage config
    storage = config_data.get('database_storage', {})
    
    # Extract sync config
    sync = config_data.get('delta_synchronization', {})
    
    # Extract tables to sync
    tables_to_sync = sync.get('tables_to_sync', [])
    
    config = WorkloadConfig(
        bulk_writes_per_second=db_instance.get('bulk_writes_per_second', 0),
        continuous_writes_per_second=db_instance.get('continuous_writes_per_second', 0),
        reads_per_second=db_instance.get('reads_per_second', 0),
        number_of_readable_secondaries=db_instance.get('number_of_readable_secondaries', 0),
        readable_secondary_size_cu=db_instance.get('readable_secondary_size_cu', 1),
        data_stored_gb=storage.get('data_stored_gb', 0),
        estimated_data_deleted_daily_gb=storage.get('estimated_data_deleted_daily_gb', 0),
        restore_windows_days=storage.get('restore_windows_days', 0),
        number_of_continuous_pipelines=sync.get('number_of_continuous_pipelines', 0),
        expected_data_per_sync_gb=sync.get('expected_data_per_sync_gb', 0),
        sync_mode=sync.get('sync_mode', 'Snapshot'),
        sync_frequency=sync.get('sync_frequency', 'Per day'),
        promotion_percentage=db_instance.get('promotion_percentage', config_data.get('promotion_percentage', 0.0))
    )
    
    return config, tables_to_sync


def get_delta_table_sizes(tables_config: List[Dict[str, Any]], 
                         server_hostname: str,
                         http_path: str,
                         access_token: str) -> Dict[str, Any]:
    """
    Calculate total uncompressed size of Delta tables using Databricks SQL connector.
    
    Args:
        tables_config: List of table configurations from YAML
        server_hostname: Databricks workspace hostname
        http_path: SQL warehouse HTTP path
        access_token: Databricks access token
        
    Returns:
        Dictionary with total size and per-table details
    """
    if not DATABRICKS_SQL_AVAILABLE:
        return {"error": "databricks-sql-connector not available"}
    
    try:
        # Connect to Databricks
        with sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        ) as connection:
            with connection.cursor() as cursor:
                # Validate tables configuration
                if not tables_config:
                    return {"error": "No valid tables found in configuration"}
                
                logger.info(f"Executing individual queries to calculate table sizes...")
                
                # Get individual table sizes and sum them up
                individual_sizes = []
                total_uncompressed_bytes = 0
                
                for i, table_config in enumerate(tables_config):
                    table_name = table_config.get('name', '')
                    if not table_name:
                        continue
                    
                    try:
                        # Query individual table size
                        individual_query = f"select sum(len(to_csv(struct(*))) + 32) as uncompressed_bytes FROM {table_name}"
                        logger.debug(f"Executing query for {table_name}: {individual_query}")
                        cursor.execute(individual_query)
                        individual_result = cursor.fetchone()
                        
                        if individual_result and individual_result[0] is not None:
                            table_bytes = individual_result[0]
                            table_size_mb = table_bytes / (1024**2)
                            total_uncompressed_bytes += table_bytes
                            
                            individual_sizes.append({
                                'table_name': table_name,
                                'uncompressed_bytes': table_bytes,
                                'size_mb': table_size_mb
                            })
                        else:
                            logger.warning(f"No data returned for table {table_name}")
                            individual_sizes.append({
                                'table_name': table_name,
                                'uncompressed_bytes': 0,
                                'size_mb': 0.0,
                                'error': 'No data returned'
                            })
                    except Exception as e:
                        logger.warning(f"Could not get size for table {table_name}: {e}")
                        individual_sizes.append({
                            'table_name': table_name,
                            'uncompressed_bytes': 0,
                            'size_mb': 0.0,
                            'error': str(e)
                        })
                
                total_size_mb = total_uncompressed_bytes / (1024**2)  # Convert to MB
                
                return {
                    'total_uncompressed_bytes': total_uncompressed_bytes,
                    'total_size_mb': total_size_mb,
                    'table_details': individual_sizes,
                    'query_used': f"Individual queries for {len(individual_sizes)} tables"
                }
                    
    except Exception as e:
        logger.error(f"Error calculating table sizes: {e}")
        return {"error": str(e)}


def estimate_cost_from_config(config_data: Dict[str, Any], warehouse_http_path: str = None, workspace_url: str = None, profile: str = None) -> Dict[str, Any]:
    """
    Estimate cost from configuration data and return JSON structure for the app.

    Args:
        config_data: Dictionary containing workload configuration
        warehouse_http_path: SQL warehouse HTTP path for table size calculation
        workspace_url: Databricks workspace URL
        profile: Databricks CLI profile name for authentication

    Returns:
        Dictionary with cost estimation results
    """
    try:
        estimator = LakebaseCostEstimator()
        
        # Create WorkloadConfig from the data
        db_instance = config_data.get('database_instance', {})
        storage = config_data.get('database_storage', {})
        sync = config_data.get('delta_synchronization', {})
        
        config = WorkloadConfig(
            bulk_writes_per_second=db_instance.get('bulk_writes_per_second', 0),
            continuous_writes_per_second=db_instance.get('continuous_writes_per_second', 0),
            reads_per_second=db_instance.get('reads_per_second', 0),
            number_of_readable_secondaries=db_instance.get('number_of_readable_secondaries', 0),
            readable_secondary_size_cu=db_instance.get('readable_secondary_size_cu', 1),
            data_stored_gb=storage.get('data_stored_gb', 0),
            estimated_data_deleted_daily_gb=storage.get('estimated_data_deleted_daily_gb', 0),
            restore_windows_days=storage.get('restore_windows_days', 0),
            number_of_continuous_pipelines=sync.get('number_of_continuous_pipelines', 0),
            expected_data_per_sync_gb=sync.get('expected_data_per_sync_gb', 0),
            sync_mode=sync.get('sync_mode', 'SNAPSHOT'),
            sync_frequency=sync.get('sync_frequency', 'Per day'),
            promotion_percentage=db_instance.get('promotion_percentage', 0.0)
        )
        
        # Calculate cost breakdown
        cost_breakdown = estimator.calculate_total_cost(config)
        
        # Calculate table sizes if requested and credentials are available
        table_sizes = None
        tables_to_sync = sync.get('tables_to_sync', [])
        
        # Always calculate table sizes when tables are provided
        should_calculate = bool(tables_to_sync)

        if should_calculate and tables_to_sync:
            # Validate required parameters
            if not workspace_url or not warehouse_http_path:
                logger.info("Workspace URL or warehouse HTTP path not provided - skipping table size calculation")
                table_sizes = {"error": "Workspace URL and warehouse HTTP path are required for table size calculation"}
            elif not isinstance(warehouse_http_path, str) or not warehouse_http_path.strip():
                logger.info("Warehouse HTTP path is empty - skipping table size calculation")
                table_sizes = {"error": "Warehouse HTTP path is empty or missing"}
            else:
                try:
                    # Extract hostname from workspace URL
                    hostname = workspace_url.replace('https://', '').replace('http://', '')

                    # Check warehouse existence via SDK
                    exists, msg = _warehouse_exists(warehouse_http_path, profile=profile)
                    if not exists:
                        logger.error(f"Warehouse existence check failed: {msg}")
                        table_sizes = {"error": msg}
                    else:
                        # Generate temporary access token using WorkspaceClient
                        if DATABRICKS_SDK_AVAILABLE:
                            try:
                                # Initialize WorkspaceClient with profile or default auth
                                client = WorkspaceClient(profile=profile, host=workspace_url) if profile else WorkspaceClient(host=workspace_url)

                                # Generate a temporary token for SQL operations
                                # Token lifetime: 1 hour (3600 seconds)
                                temp_token = client.tokens.create(
                                    comment="Temporary token for Lakebase cost estimation",
                                    lifetime_seconds=3600
                                )
                                access_token = temp_token.token_value

                                logger.info("Generated temporary access token for table size calculation")

                                # Execute with a hard timeout so we never hang if the warehouse was deleted
                                with concurrent.futures.ThreadPoolExecutor(max_workers=len(tables_to_sync)) as executor:
                                    future = executor.submit(
                                        get_delta_table_sizes,
                                        tables_to_sync,
                                        hostname,
                                        warehouse_http_path,
                                        access_token,
                                    )
                                    # Fail fast after 10 minutes to keep the UI responsive
                                    table_sizes = future.result(timeout=10*60)

                                # Clean up the temporary token after use
                                try:
                                    client.tokens.delete(temp_token.token_id)
                                    logger.info("Cleaned up temporary access token")
                                except Exception as cleanup_error:
                                    logger.warning(f"Failed to clean up temporary token: {cleanup_error}")

                            except Exception as token_error:
                                logger.error(f"Failed to generate temporary token: {token_error}")
                                table_sizes = {"error": f"Failed to generate access token: {str(token_error)}"}
                        else:
                            logger.error("Databricks SDK not available for token generation")
                            table_sizes = {"error": "Databricks SDK not available for token generation"}

                except concurrent.futures.TimeoutError:
                    logger.error("Timed out calculating table sizes (warehouse may be deleted or unreachable)")
                    table_sizes = {"error": "Timed out calculating table sizes. Verify your SQL warehouse HTTP path."}
                except Exception as e:
                    logger.error(f"Failed calculating table sizes: {e}")
                    table_sizes = {"error": str(e)}
        
        # Build results dictionary
        results = {
            "timestamp": datetime.now().isoformat(),
            "workload_config": asdict(config),
            "cost_breakdown": asdict(cost_breakdown)
        }
        
        if table_sizes:
            results["table_sizes"] = table_sizes
            
        return results
        
    except Exception as e:
        logger.error(f"Error processing configuration: {e}")
        raise e


def main():
    # Note: Environment variables are no longer loaded from .env file
    
    parser = argparse.ArgumentParser(
        description='Lakebase Postgres Cost Estimator - Load configuration from YAML file'
    )
    
    parser.add_argument(
        '--config',
        required=True,
        help='Path to workload configuration file (YAML) - Required'
    )
    
    parser.add_argument(
        '--output',
        help='Output file for results (JSON)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--calculate-table-sizes',
        action='store_true',
        help='Force table size calculation (normally automatic when credentials are available)'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Load from configuration file
    try:
        config, tables_to_sync = load_workload_config(args.config)
        
        # Convert to dictionary format for the new function
        config_data = {
            'database_instance': {
                'bulk_writes_per_second': config.bulk_writes_per_second,
                'continuous_writes_per_second': config.continuous_writes_per_second,
                'reads_per_second': config.reads_per_second,
                'number_of_readable_secondaries': config.number_of_readable_secondaries,
                'readable_secondary_size_cu': config.readable_secondary_size_cu,
                'promotion_percentage': config.promotion_percentage
            },
            'database_storage': {
                'data_stored_gb': config.data_stored_gb,
                'estimated_data_deleted_daily_gb': config.estimated_data_deleted_daily_gb,
                'restore_windows_days': config.restore_windows_days
            },
            'delta_synchronization': {
                'number_of_continuous_pipelines': config.number_of_continuous_pipelines,
                'expected_data_per_sync_gb': config.expected_data_per_sync_gb,
                'sync_mode': config.sync_mode,
                'sync_frequency': config.sync_frequency,
                'tables_to_sync': tables_to_sync
            }
        }
        
        # Use the new function - automatically calculate table sizes if credentials are available
        # Check if Databricks credentials are available
        hostname = os.getenv('DATABRICKS_SERVER_HOSTNAME')
        http_path = os.getenv('DATABRICKS_HTTP_PATH')
        access_token = os.getenv('DATABRICKS_ACCESS_TOKEN')
        auto_calculate_table_sizes = all([hostname, http_path, access_token]) and tables_to_sync
        
        # Use command line flag if provided, otherwise auto-calculate
        calculate_table_sizes = args.calculate_table_sizes or auto_calculate_table_sizes
        
        results = estimate_cost_from_config(config_data, calculate_table_sizes)
        
        # Display results
        print("\n" + "="*60)
        print("COST ESTIMATION RESULTS")
        print("="*60)
        
        print(f"\nConfiguration:")
        print(f"  Bulk writes/sec: {config.bulk_writes_per_second:,}")
        print(f"  Continuous writes/sec: {config.continuous_writes_per_second:,}")
        print(f"  Reads/sec: {config.reads_per_second:,}")
        print(f"  Readable secondaries: {config.number_of_readable_secondaries} x {config.readable_secondary_size_cu}CU")
        print(f"  Data stored: {config.data_stored_gb:,} GB")
        print(f"  Sync data: {config.expected_data_per_sync_gb:,} GB")
        print(f"  Restore windows: {config.restore_windows_days} days")
        print(f"  Promotion: {config.promotion_percentage}%")
        
        cost_breakdown = results['cost_breakdown']
        print(f"\nCalculated Results:")
        print(f"  Recommended CU: {cost_breakdown['recommended_cu']}")
        print(f"  Main instance cost: ${cost_breakdown['main_instance_cost']:.2f}")
        print(f"  Readable secondaries cost: ${cost_breakdown['readable_secondaries_cost']:.2f}")
        print(f"  Total compute cost: ${cost_breakdown['total_compute_cost']:.2f}")
        print(f"  Storage cost: ${cost_breakdown['storage_cost']:.2f}")
        print(f"  Sync cost: ${cost_breakdown['total_sync_cost']:.2f}")
        print(f"  Estimated sync time: {cost_breakdown['estimated_sync_time_hours']:.2f} hours")
        print(f"  Total monthly cost: ${cost_breakdown['total_monthly_cost']:.2f}")
        
        
        # Display table size information if calculated
        if 'table_sizes' in results:
            table_sizes = results['table_sizes']
            if 'error' not in table_sizes:
                print(f"\nTable Size Analysis:")
                print(f"  Total uncompressed size: {table_sizes['total_size_mb']:.2f} MB")
                print(f"  Total uncompressed bytes: {table_sizes['total_uncompressed_bytes']:,}")
                print(f"  Number of tables: {len(table_sizes['table_details'])}")
                
                if table_sizes['table_details']:
                    print(f"\nPer-table details:")
                    for table_detail in table_sizes['table_details']:
                        if 'error' in table_detail:
                            print(f"    {table_detail['table_name']}: Error - {table_detail['error']}")
                        else:
                            print(f"    {table_detail['table_name']}: {table_detail['size_mb']:.2f} MB ({table_detail['uncompressed_bytes']:,} bytes)")
            elif 'error' in table_sizes:
                print(f"\nTable Size Analysis: Error - {table_sizes['error']}")
        elif tables_to_sync and not calculate_table_sizes:
            print(f"\nTable Size Analysis: Skipped (Databricks credentials not configured)")
        
        # Save results if requested
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            print(f"\nResults saved to: {args.output}")
    
    except Exception as e:
        logger.error(f"Error processing configuration: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
