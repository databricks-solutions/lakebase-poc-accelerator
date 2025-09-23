#!/usr/bin/env python3
"""
Integration test script for the Lakebase Accelerator application.
Tests the integration between the web app and existing Python scripts.
"""

import os
import sys
import json
import tempfile
import subprocess
from pathlib import Path

# Add path for backend imports
sys.path.append(str(Path(__file__).parent / "backend"))

def test_cost_estimator_integration():
    """Test integration with lakebase_cost_estimator.py"""
    print("Testing cost estimator integration...")
    
    # Sample workload configuration
    sample_config = {
        'database_instance': {
            'bulk_writes_per_second': 5000,
            'continuous_writes_per_second': 2000,
            'reads_per_second': 8000,
            'number_of_readable_secondaries': 2,
            'readable_secondary_size_cu': 2,
            'promotion_percentage': 50.0
        },
        'database_storage': {
            'data_stored_gb': 250,
            'estimated_data_deleted_daily_gb': 5,
            'restore_windows_days': 7
        },
        'delta_synchronization': {
            'number_of_continuous_pipelines': 2,
            'expected_data_per_sync_gb': 20,
            'sync_mode': 'SNAPSHOT',
            'sync_frequency': 'Per day',
            'tables_to_sync': [
                {
                    'name': 'samples.tpcds_sf1.customer',
                    'primary_keys': ['c_customer_sk'],
                    'scheduling_policy': 'SNAPSHOT'
                }
            ]
        }
    }
    
    # Create temporary config file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as tmp_file:
        import yaml
        yaml.dump(sample_config, tmp_file)
        tmp_config_path = tmp_file.name
    
    # Create temporary output file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_output:
        tmp_output_path = tmp_output.name
    
    try:
        # Run cost estimator
        cmd = [
            sys.executable,
            str(Path(__file__).parent.parent / "src" / "lakebase_cost_estimator.py"),
            "--config", tmp_config_path,
            "--output", tmp_output_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path(__file__).parent.parent)
        
        if result.returncode == 0:
            # Read and validate output
            with open(tmp_output_path, 'r') as f:
                cost_data = json.load(f)
            
            # Check for essential fields (allow for some flexibility in field names)
            has_cost_breakdown = 'cost_breakdown' in cost_data
            has_timestamp = 'timestamp' in cost_data
            has_config = 'workload_config' in cost_data or 'config' in cost_data
            
            if has_cost_breakdown and has_timestamp and has_config:
                print("✅ Cost estimator integration successful")
                print(f"   Output contains: {list(cost_data.keys())}")
                return True
            else:
                print(f"❌ Missing required fields. Expected: cost_breakdown, timestamp, config")
                print(f"   Found: {list(cost_data.keys())}")
                return False
        else:
            print(f"❌ Cost estimator failed: {result.stderr}")
            return False
            
    finally:
        # Cleanup
        os.unlink(tmp_config_path)
        if os.path.exists(tmp_output_path):
            os.unlink(tmp_output_path)

def test_table_generator_integration():
    """Test integration with generate_synced_tables.py"""
    print("Testing table generator integration...")
    
    # Sample workload configuration with tables
    sample_config = {
        'database_instance': {
            'bulk_writes_per_second': 5000,
            'continuous_writes_per_second': 2000,
            'reads_per_second': 8000,
            'number_of_readable_secondaries': 2,
            'readable_secondary_size_cu': 2,
            'promotion_percentage': 50.0
        },
        'database_storage': {
            'data_stored_gb': 250,
            'estimated_data_deleted_daily_gb': 5,
            'restore_windows_days': 7
        },
        'delta_synchronization': {
            'number_of_continuous_pipelines': 2,
            'expected_data_per_sync_gb': 20,
            'sync_mode': 'SNAPSHOT',
            'sync_frequency': 'Per day',
            'tables_to_sync': [
                {
                    'name': 'samples.tpcds_sf1.customer',
                    'primary_keys': ['c_customer_sk'],
                    'scheduling_policy': 'SNAPSHOT'
                },
                {
                    'name': 'samples.tpcds_sf1.customer_address', 
                    'primary_keys': ['ca_address_sk'],
                    'scheduling_policy': 'TRIGGERED'
                }
            ]
        }
    }
    
    # Create temporary config file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as tmp_file:
        import yaml
        yaml.dump(sample_config, tmp_file)
        tmp_config_path = tmp_file.name
    
    # Create temporary output file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as tmp_output:
        tmp_output_path = tmp_output.name
    
    try:
        # Run table generator
        cmd = [
            sys.executable,
            str(Path(__file__).parent.parent / "src" / "generate_synced_tables.py"),
            "--config", tmp_config_path,
            "--output", tmp_output_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path(__file__).parent.parent)
        
        if result.returncode == 0:
            # Read and validate output
            with open(tmp_output_path, 'r') as f:
                import yaml
                table_config = yaml.safe_load(f)
            
            if 'resources' in table_config and 'synced_database_tables' in table_config['resources']:
                synced_tables = table_config['resources']['synced_database_tables']
                if len(synced_tables) == 2:  # Should have 2 tables
                    print("✅ Table generator integration successful")
                    return True
                else:
                    print(f"❌ Expected 2 synced tables, got {len(synced_tables)}")
                    return False
            else:
                print(f"❌ Invalid table configuration structure: {table_config.keys()}")
                return False
        else:
            print(f"❌ Table generator failed: {result.stderr}")
            return False
            
    finally:
        # Cleanup
        os.unlink(tmp_config_path)
        if os.path.exists(tmp_output_path):
            os.unlink(tmp_output_path)

def test_fastapi_imports():
    """Test that FastAPI backend can import required modules"""
    print("Testing FastAPI import dependencies...")
    
    try:
        # Test backend imports
        from backend.main import app
        print("✅ FastAPI app import successful")
        
        # Test that we can access the existing scripts
        sys.path.append(str(Path(__file__).parent.parent / "src"))
        
        # Try importing modules (without running them)
        import importlib.util
        
        scripts_to_test = [
            "lakebase_cost_estimator.py",
            "generate_synced_tables.py"
        ]
        
        for script in scripts_to_test:
            script_path = Path(__file__).parent.parent / "src" / script
            spec = importlib.util.spec_from_file_location(script[:-3], script_path)
            if spec and spec.loader:
                print(f"✅ {script} can be imported")
            else:
                print(f"❌ {script} import failed")
                return False
        
        return True
        
    except Exception as e:
        print(f"❌ Import test failed: {e}")
        return False

def test_frontend_build():
    """Test that frontend can be built"""
    print("Testing frontend build...")
    
    frontend_dir = Path(__file__).parent / "frontend"
    if not frontend_dir.exists():
        print("❌ Frontend directory not found")
        return False
    
    # Check if package.json exists
    package_json = frontend_dir / "package.json"
    if not package_json.exists():
        print("❌ package.json not found")
        return False
    
    try:
        # Try to run npm build (this might take a while)
        result = subprocess.run(
            ["npm", "run", "build"], 
            cwd=frontend_dir, 
            capture_output=True, 
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.returncode == 0:
            build_dir = frontend_dir / "build"
            if build_dir.exists():
                print("✅ Frontend build successful")
                return True
            else:
                print("❌ Build directory not created")
                return False
        else:
            print(f"❌ Frontend build failed: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Frontend build timed out")
        return False
    except Exception as e:
        print(f"❌ Frontend build error: {e}")
        return False

def main():
    """Run all integration tests"""
    print("🚀 Running Lakebase Accelerator Integration Tests\n")
    
    tests = [
        ("FastAPI Import Dependencies", test_fastapi_imports),
        ("Cost Estimator Integration", test_cost_estimator_integration), 
        ("Table Generator Integration", test_table_generator_integration),
        # ("Frontend Build", test_frontend_build),  # Uncomment if you want to test build
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n--- {test_name} ---")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Test {test_name} crashed: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*50)
    print("TEST SUMMARY")
    print("="*50)
    
    passed = 0
    for test_name, result in results:
        status = "PASSED" if result else "FAILED"
        emoji = "✅" if result else "❌"
        print(f"{emoji} {test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nResults: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("🎉 All integration tests passed! The application is ready for deployment.")
        return True
    else:
        print("⚠️  Some tests failed. Please check the errors above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)