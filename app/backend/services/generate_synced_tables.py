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


# def load_workload_config(config_path):
#     """Load the workload sizing configuration file."""
#     with open(config_path, 'r') as file:
#         return yaml.safe_load(file)


# def generate_synced_tables_config(tables_to_sync, output_path):
#     """Generate synced_delta_tables.yml configuration from tables_to_sync."""
    
#     # Base template for synced database tables
#     synced_tables_config = {
#         'resources': {
#             'synced_database_tables': {}
#         }
#     }
    
#     for table_config in tables_to_sync:
#         table_name = table_config['name']
#         primary_keys = table_config['primary_keys']
#         table_short_name = generate_table_name(table_name)
        
#         # Get scheduling_policy from table config or use TRIGGERED as default
#         # Normalize to uppercase for consistency
#         scheduling_policy = table_config.get('scheduling_policy', 'TRIGGERED').upper()
        
#         # Generate the synced table configuration
#         synced_table = {
#             'name': f'${{resources.database_catalogs.my_catalog.name}}.public.{table_short_name}',
#             'database_instance_name': '${resources.database_catalogs.my_catalog.database_instance_name}',
#             'logical_database_name': '${resources.database_catalogs.my_catalog.database_name}',
#             'spec': {
#                 'source_table_full_name': table_name,
#                 'scheduling_policy': scheduling_policy,
#                 'primary_key_columns': primary_keys,
#                 'new_pipeline_spec': {
#                     'storage_catalog': 'main',
#                     'storage_schema': 'default'
#                 }
#             }
#         }
        
#         synced_tables_config['resources']['synced_database_tables'][table_short_name] = synced_table
    
#     # Create output directory if it doesn't exist
#     output_path.parent.mkdir(parents=True, exist_ok=True)
    
#     # Write the configuration to file
#     with open(output_path, 'w') as file:
#         file.write("# Generated synced table configuration from workload sizing\n")
#         file.write("# Define synced table: https://docs.databricks.com/aws/en/dev-tools/bundles/resources#synced_database_tables\n")
#         yaml.dump(synced_tables_config, file, default_flow_style=False, sort_keys=False, indent=2)
    
#     print(f"Generated synced_delta_tables.yml with {len(tables_to_sync)} tables")
#     print(f"Output written to: {output_path}")


# def main():
#     """Main function to generate synced tables configuration."""
    
#     # Set up argument parser
#     parser = argparse.ArgumentParser(
#         description='Generate synced_delta_tables.yml from workload configuration files'
#     )
    
#     parser.add_argument(
#         '--config',
#         required=True,
#         help='Path to workload configuration file (YAML)'
#     )
    
#     parser.add_argument(
#         '--output',
#         help='Output file path for synced_delta_tables.yml (default: auto-generated)'
#     )
    
#     parser.add_argument(
#         '--verbose',
#         action='store_true',
#         help='Enable verbose output'
#     )
    
#     args = parser.parse_args()
    
#     # Resolve paths - handle both relative and absolute paths
#     workload_config_path = Path(args.config)
#     if not workload_config_path.is_absolute():
#         # If relative path, make it relative to current working directory
#         workload_config_path = Path.cwd() / workload_config_path
    
#     # Generate output path if not provided
#     if args.output:
#         output_path = Path(args.output)
#         if not output_path.is_absolute():
#             # If relative path, make it relative to current working directory
#             output_path = Path.cwd() / output_path
#     else:
#         # Auto-generate output path based on config file location
#         if 'quickstarts' in str(workload_config_path):
#             output_path = workload_config_path.parent / "resources" / "synced_delta_tables.yml"
#         else:
#             output_path = workload_config_path.parent / "synced_delta_tables.yml"
    
#     # Check if workload config exists
#     if not workload_config_path.exists():
#         print(f"Error: Workload configuration file not found at {workload_config_path}")
#         return 1
    
#     try:
#         # Load workload configuration
#         workload_config = load_workload_config(workload_config_path)
        
#         # Extract tables_to_sync
#         delta_sync_config = workload_config.get('delta_synchronization', {})
#         tables_to_sync = delta_sync_config.get('tables_to_sync', [])
        
#         if not tables_to_sync:
#             print("Error: No tables_to_sync found in workload configuration")
#             return 1
        
#         print(f"Found {len(tables_to_sync)} tables to sync:")
#         for table in tables_to_sync:
#             scheduling_policy = table.get('scheduling_policy', 'TRIGGERED')
#             print(f"  - {table['name']} (PK: {', '.join(table['primary_keys'])}, Policy: {scheduling_policy})")
        
#         # Generate synced tables configuration
#         generate_synced_tables_config(tables_to_sync, output_path)
        
#         return 0
        
#     except Exception as e:
#         print(f"Error: {e}")
#         if args.verbose:
#             import traceback
#             traceback.print_exc()
#         return 1

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
                    'storage_catalog': 'main',
                    'storage_schema': 'default'
                }
            }
        }
        
        synced_tables_config['resources']['synced_database_tables'][table_short_name] = synced_table
    
    return synced_tables_config


# if __name__ == "__main__":
#     exit(main())
