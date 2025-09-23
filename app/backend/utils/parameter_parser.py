import re
from typing import List, Dict, Any
from models.query_models import QueryParameter, ParameterType, ParameterSet, QueryConfiguration

class SimpleParameterParser:
    """
    Simple utility to process user-uploaded queries and parameters.
    """
    
    @staticmethod
    def count_parameters(query: str) -> int:
        """Count %s placeholders in SQL query."""
        return len(re.findall(r'%s', query))
    
    @staticmethod
    def validate_query_format(query: str) -> Dict[str, Any]:
        """
        Validate query and return parameter information.
        
        Returns:
            {
                "is_valid": bool,
                "parameter_count": int,
                "error_message": str or None
            }
        """
        try:
            # Basic validation
            if not query.strip():
                return {"is_valid": False, "parameter_count": 0, "error_message": "Query is empty"}
            
            # Count parameters
            param_count = SimpleParameterParser.count_parameters(query)
            
            # Check for common issues
            if '%' in query and param_count == 0:
                return {"is_valid": False, "parameter_count": 0, "error_message": "Found % but no %s placeholders"}
            
            return {"is_valid": True, "parameter_count": param_count, "error_message": None}
            
        except Exception as e:
            return {"is_valid": False, "parameter_count": 0, "error_message": f"Query validation error: {str(e)}"}
    
    @staticmethod
    def process_user_parameters(
        query: str, 
        parameter_configs: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Process user-provided parameter configuration.
        
        Args:
            query: SQL query string
            parameter_configs: List of parameter configurations from user
            
        Returns:
            Processed parameter configuration ready for execution
        """
        param_count = SimpleParameterParser.count_parameters(query)
        
        if len(parameter_configs) != param_count:
            raise ValueError(f"Expected {param_count} parameters, got {len(parameter_configs)}")
        
        # Process each parameter
        processed_params = []
        for i, config in enumerate(parameter_configs):
            processed_params.append({
                "index": i + 1,
                "name": config.get("name", f"param_{i+1}"),
                "type": config.get("type", "string"),
                "sample_value": config.get("sample_value")
            })
        
        return {
            "query": query,
            "parameter_count": param_count,
            "parameters": processed_params
        }
    
    @staticmethod
    def create_test_scenarios(
        parameter_definitions: List[Dict[str, Any]],
        user_scenarios: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Create test scenarios from user input.
        
        Args:
            parameter_definitions: Parameter definitions
            user_scenarios: User-provided test scenarios
            
        Returns:
            List of test scenarios ready for execution
        """
        scenarios = []
        
        for scenario in user_scenarios:
            # Validate parameter count
            if len(scenario["parameters"]) != len(parameter_definitions):
                raise ValueError(f"Scenario '{scenario['name']}' has wrong parameter count")
            
            # Create scenario
            test_scenario = {
                "name": scenario["name"],
                "parameters": scenario["parameters"],
                "execution_count": scenario.get("execution_count", 1),
                "description": scenario.get("description", "")
            }
            scenarios.append(test_scenario)
        
        return scenarios
    
    @staticmethod
    def convert_to_query_configuration(
        query_identifier: str,
        query_content: str,
        parameter_configs: List[Dict[str, Any]],
        test_scenarios: List[Dict[str, Any]]
    ) -> QueryConfiguration:
        """
        Convert simple user input to QueryConfiguration model.
        
        Args:
            query_identifier: Unique identifier for the query
            query_content: SQL query content
            parameter_configs: Parameter configurations
            test_scenarios: Test scenarios
            
        Returns:
            QueryConfiguration object
        """
        # Validate query format
        validation = SimpleParameterParser.validate_query_format(query_content)
        if not validation["is_valid"]:
            raise ValueError(validation["error_message"])
        
        # Create parameter definitions
        parameter_definitions = []
        for i, config in enumerate(parameter_configs):
            param = QueryParameter(
                parameter_index=i + 1,
                parameter_name=config["name"],
                data_type=ParameterType(config["type"]),
                sample_value=config["sample_value"],
                required=config.get("required", True),
                min_value=config.get("min_value"),
                max_value=config.get("max_value"),
                pattern=config.get("pattern")
            )
            parameter_definitions.append(param)
        
        # Create parameter sets
        parameter_sets = []
        for scenario in test_scenarios:
            param_set = ParameterSet(
                set_name=scenario["name"],
                parameters=scenario["parameters"],
                execution_count=scenario.get("execution_count", 1),
                description=scenario.get("description")
            )
            parameter_sets.append(param_set)
        
        # Calculate total executions
        total_executions = sum(ps.execution_count for ps in parameter_sets)
        
        return QueryConfiguration(
            query_identifier=query_identifier,
            query_content=query_content,
            parameter_definitions=parameter_definitions,
            parameter_sets=parameter_sets,
            total_executions=total_executions
        )
    
    @staticmethod
    def parse_query_file(file_path: str) -> tuple[str, str]:
        """
        Parse SQL query file and extract query content and identifier.
        
        Args:
            file_path: Path to SQL file
            
        Returns:
            Tuple of (query_identifier, query_content)
        """
        import os
        
        # Extract identifier from filename
        filename = os.path.basename(file_path)
        query_identifier = os.path.splitext(filename)[0]
        
        # Read query content
        with open(file_path, 'r', encoding='utf-8') as f:
            query_content = f.read().strip()
        
        return query_identifier, query_content
