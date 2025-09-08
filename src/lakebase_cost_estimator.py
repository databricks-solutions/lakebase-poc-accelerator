#!/usr/bin/env python3
"""
Lakebase Postgres Cost Estimator

This script calculates the cost of running Lakebase Postgres instances based on
workload characteristics and usage patterns.

Usage:
    python lakebase_cost_estimator.py --config workload_sizing.yml
    python lakebase_cost_estimator.py --interactive
    python lakebase_cost_estimator.py --config workload_sizing.yml --output cost_report.json
"""

import argparse
import json
import logging
import math
import sys
from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional
from datetime import datetime
import yaml

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


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
    
    # Cost per unit metrics
    cost_per_gb: float
    cost_per_qps: float
    cost_per_cu: float


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
            "Snapshot": {1: 54, 2: 108, 4: 216},
            "Triggered": {1: 4.5, 2: 9, 4: 18}
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
            sync_mode (str): "Snapshot" or "Triggered".
        
        Returns:
            float: Estimated time in hours.
        """
        # Fixed overhead: 10 minutes
        overhead_hours = 10 / 60
        
        if sync_mode not in self.sync_throughput:
            raise ValueError("Invalid sync mode. Use 'Snapshot' or 'Triggered'.")
        if cus not in self.sync_throughput[sync_mode]:
            raise ValueError("Invalid CU. Use 1, 2, or 4.")
        
        transfer_rate = self.sync_throughput[sync_mode][cus]
        
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
        
        # Calculate cost per unit metrics
        total_qps = config.reads_per_second + config.bulk_writes_per_second + config.continuous_writes_per_second
        cost_per_gb = total_monthly_cost / config.data_stored_gb if config.data_stored_gb > 0 else 0
        cost_per_qps = total_monthly_cost / total_qps if total_qps > 0 else 0
        cost_per_cu = total_monthly_cost / cu_requirements["recommended_cu"]
        
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
            total_monthly_cost=total_monthly_cost,
            
            # Cost per unit metrics
            cost_per_gb=cost_per_gb,
            cost_per_qps=cost_per_qps,
            cost_per_cu=cost_per_cu
        )


def load_workload_config(config_file: str) -> WorkloadConfig:
    """Load workload configuration from YAML file."""
    with open(config_file, 'r') as f:
        config_data = yaml.safe_load(f)
    
    # Extract database instance config
    db_instance = config_data.get('database_instance', {})
    
    # Extract storage config
    storage = config_data.get('database_storage', {})
    
    # Extract sync config
    sync = config_data.get('delta_synchronization', {})
    
    return WorkloadConfig(
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


def main():
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
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    estimator = LakebaseCostEstimator()
    
    # Load from configuration file
    try:
        config = load_workload_config(args.config)
        cost_breakdown = estimator.calculate_total_cost(config)
        
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
        
        print(f"\nCalculated Results:")
        print(f"  Recommended CU: {cost_breakdown.recommended_cu}")
        print(f"  Main instance cost: ${cost_breakdown.main_instance_cost:.2f}")
        print(f"  Readable secondaries cost: ${cost_breakdown.readable_secondaries_cost:.2f}")
        print(f"  Total compute cost: ${cost_breakdown.total_compute_cost:.2f}")
        print(f"  Storage cost: ${cost_breakdown.storage_cost:.2f}")
        print(f"  Sync cost: ${cost_breakdown.total_sync_cost:.2f}")
        print(f"  Estimated sync time: {cost_breakdown.estimated_sync_time_hours:.2f} hours")
        print(f"  Total monthly cost: ${cost_breakdown.total_monthly_cost:.2f}")
        
        print(f"\nCost Efficiency Metrics:")
        print(f"  Cost per GB: ${cost_breakdown.cost_per_gb:.4f}")
        print(f"  Cost per QPS: ${cost_breakdown.cost_per_qps:.4f}")
        print(f"  Cost per CU: ${cost_breakdown.cost_per_cu:.2f}")
        
        # Save results if requested
        if args.output:
            results = {
                "timestamp": datetime.now().isoformat(),
                "workload_config": asdict(config),
                "cost_breakdown": asdict(cost_breakdown)
            }
            with open(args.output, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            print(f"\nResults saved to: {args.output}")
    
    except Exception as e:
        logger.error(f"Error processing configuration: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
