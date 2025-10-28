#!/usr/bin/env python3
"""
Script to generate synced_delta_tables.yml from workload configuration files
Reads the tables_to_sync parameter and generates the corresponding synced database tables configuration.

Usage:
    # From base directory:
    python app/backend/services/generate_synced_tables.py --config workload_config.yml --output resources/synced_delta_tables.yml
    python app/backend/services/generate_synced_tables.py --config quickstarts/quickstarts_workload_config.yml --output quickstarts/resources/synced_delta_tables.yml
    
    
Note: Use relative paths (e.g., 'resources/synced_delta_tables.yml') not absolute paths (e.g., '/resources/synced_delta_tables.yml')
"""

import argparse
import yaml
import os
import sys
from pathlib import Path


def generate_table_name(table_full_name):
    """Extract table name from fully qualified name."""
    return table_full_name.split('.')[-1]
    
def generate_synced_tables_from_config(config_data: dict) -> dict:
    """
    Generate synced tables configuration from config data and return dictionary.
    
    Args:
        config_data: Dictionary containing workload configuration
        
    Returns:
        Dictionary with synced tables configuration
    """
    # Extract tables to sync from delta_synchronization section
    tables_to_sync = config_data.get('delta_synchronization', {}).get('tables_to_sync', [])
    database_instance_name = config_data.get('lakebase_instance_name'.replace('-', '_'))
    catalog_name = config_data.get('uc_catalog_name')
    storage_catalog = config_data.get('storage_catalog', 'main')
    storage_schema = config_data.get('storage_schema', 'default')
    
    if not tables_to_sync:
        return {
            'resources': {
                'synced_database_tables': {}
            }
        }
    
    # Base template for synced database tables
    synced_tables_config = {
        'resources': {
            'synced_database_tables': {}
        }
    }
    
    for table_config in tables_to_sync:
        table_name = table_config['name']
        primary_keys = table_config['primary_keys']
        table_short_name = generate_table_name(table_name)
        
        # Get scheduling_policy from table config or use TRIGGERED as default
        # Normalize to uppercase for consistency
        scheduling_policy = table_config.get('scheduling_policy', 'TRIGGERED').upper()
        
        # Generate the synced table configuration
        synced_table = {
            'name': f'${{resources.database_catalogs.{catalog_name}.name}}.public.{table_short_name}',
            'database_instance_name': f'${{resources.database_catalogs.{catalog_name}.database_instance_name}}',
            'logical_database_name': f'${{resources.database_catalogs.{catalog_name}.database_name}}',
            'spec': {
                'source_table_full_name': table_name,
                'scheduling_policy': scheduling_policy,
                'primary_key_columns': primary_keys,
                'new_pipeline_spec': {
                    'storage_catalog': storage_catalog,
                    'storage_schema': storage_schema
                }
            }
        }
        
        synced_tables_config['resources']['synced_database_tables'][table_short_name] = synced_table
    
    # Add a convenient tables list for frontend consumption
    synced_tables_config['synced_tables'] = [
        {
            'table_name': table_config['name'],
            'primary_keys': table_config['primary_keys'],
            'sync_policy': table_config.get('scheduling_policy', 'TRIGGERED').upper()
        }
        for table_config in tables_to_sync
    ]

    return synced_tables_config


def generate_synced_tables_yaml_from_config(config_data: dict) -> str:
    """
    Generate synced tables YAML string from config data.
    
    Args:
        config_data: Dictionary containing workload configuration
        
    Returns:
        YAML string with synced tables configuration
    """
    
    # Extract tables to sync from delta_synchronization section
    tables_to_sync = config_data.get('delta_synchronization', {}).get('tables_to_sync', [])
    catalog_name = config_data.get('uc_catalog_name')
<<<<<<< HEAD
    storage_catalog = config_data.get('storage_catalog', 'main')
    storage_schema = config_data.get('storage_schema', 'default')
=======
>>>>>>> origin/main
    
    if not tables_to_sync:
        return yaml.dump({
            'resources': {
                'synced_database_tables': {}
            }
        }, default_flow_style=False, sort_keys=False, indent=2)
    
    # Base template for synced database tables
    synced_tables_config = {
        'resources': {
            'synced_database_tables': {}
        }
    }
    
    for table_config in tables_to_sync:
        table_name = table_config['name']
        primary_keys = table_config['primary_keys']
        table_short_name = generate_table_name(table_name)
        
        # Get scheduling_policy from table config or use TRIGGERED as default
        # Normalize to uppercase for consistency
        scheduling_policy = table_config.get('scheduling_policy', 'TRIGGERED').upper()
        
        # Generate the synced table configuration
        synced_table = {
            'name': f'${{resources.database_catalogs.{catalog_name}.name}}.public.{table_short_name}',
            'database_instance_name': f'${{resources.database_catalogs.{catalog_name}.database_instance_name}}',
            'logical_database_name': f'${{resources.database_catalogs.{catalog_name}.database_name}}',
            'spec': {
                'source_table_full_name': table_name,
                'scheduling_policy': scheduling_policy,
                'primary_key_columns': primary_keys,
                'new_pipeline_spec': {
<<<<<<< HEAD
                    'storage_catalog': storage_catalog,
                    'storage_schema': storage_schema
=======
                    'storage_catalog': 'main',
                    'storage_schema': 'default'
>>>>>>> origin/main
                }
            }
        }
        
        synced_tables_config['resources']['synced_database_tables'][table_short_name] = synced_table
    
    return yaml.dump(synced_tables_config, default_flow_style=False, sort_keys=False, indent=2)


# if __name__ == "__main__":
#     exit(main())
