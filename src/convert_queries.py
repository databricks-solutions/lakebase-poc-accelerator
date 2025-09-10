#!/usr/bin/env python3
"""
Databricks SQL to Postgres SQL Query Converter

This script converts Databricks SQL queries to Postgres-compatible SQL using 
the Databricks LLM endpoint. It handles common syntax differences between 
Databricks SQL and Postgres SQL.

Usage:
    python convert_queries.py --source-dir queries/source --target-dir queries/target
    python convert_queries.py --file queries/source/specific_query.sql --output queries/target/converted.sql

Requirements:
    - openai (for Databricks LLM endpoint)
    - sqlparse (for SQL syntax validation)
    - pyyaml (for configuration)
"""

import argparse
import logging
import os
import sys
import glob
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import json
from datetime import datetime

# Load environment variables from .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # dotenv is optional at runtime; requirements include it.
    pass

# Required imports
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print("Error: openai package not found. Install with: pip install openai")

try:
    import sqlparse
    from sqlparse import sql, tokens
    SQLPARSE_AVAILABLE = True
except ImportError:
    SQLPARSE_AVAILABLE = False
    print("Warning: sqlparse not available. SQL validation disabled.")

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False
    print("Warning: pyyaml not available. Configuration file support disabled.")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SQLSyntaxValidator:
    """Validates SQL syntax without execution."""
    
    def __init__(self):
        self.postgres_keywords = {
            'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL',
            'OUTER', 'ON', 'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN',
            'LIKE', 'ILIKE', 'GROUP', 'BY', 'ORDER', 'HAVING', 'LIMIT', 'OFFSET',
            'UNION', 'INTERSECT', 'EXCEPT', 'WITH', 'AS', 'CASE', 'WHEN', 'THEN',
            'ELSE', 'END', 'CREATE', 'TABLE', 'INSERT', 'UPDATE', 'DELETE',
            'ALTER', 'DROP', 'INDEX', 'VIEW', 'FUNCTION', 'PROCEDURE'
        }
        
        self.postgres_functions = {
            'NOW()', 'CURRENT_TIMESTAMP', 'CURRENT_DATE', 'CURRENT_TIME',
            'EXTRACT', 'DATE_PART', 'DATE_TRUNC', 'AGE', 'INTERVAL',
            'STRING_AGG', 'ARRAY_AGG', 'JSON_AGG', 'JSON_OBJECT_AGG',
            'COALESCE', 'NULLIF', 'GREATEST', 'LEAST', 'SUBSTRING',
            'LENGTH', 'TRIM', 'LTRIM', 'RTRIM', 'UPPER', 'LOWER',
            'CONCAT', 'CONCAT_WS', 'SPLIT_PART', 'REGEXP_REPLACE',
            'REGEXP_SPLIT_TO_ARRAY', 'POSITION', 'STRPOS'
        }
        
        self.postgres_data_types = {
            'INTEGER', 'BIGINT', 'SMALLINT', 'DECIMAL', 'NUMERIC', 'REAL',
            'DOUBLE PRECISION', 'SERIAL', 'BIGSERIAL', 'VARCHAR', 'CHAR',
            'TEXT', 'BOOLEAN', 'DATE', 'TIME', 'TIMESTAMP', 'TIMESTAMPTZ',
            'INTERVAL', 'UUID', 'JSON', 'JSONB', 'ARRAY', 'BYTEA'
        }
    
    def validate_syntax(self, sql: str) -> Tuple[bool, List[str]]:
        """Validate SQL syntax and return validation results."""
        if not SQLPARSE_AVAILABLE:
            return True, ["SQL parsing not available - install sqlparse"]
        
        warnings = []
        
        try:
            # Parse the SQL
            parsed = sqlparse.parse(sql)
            if not parsed:
                return False, ["Unable to parse SQL"]
            
            # Check for common syntax issues
            warnings.extend(self._check_databricks_syntax(sql))
            warnings.extend(self._check_postgres_compatibility(sql))
            
            return True, warnings
            
        except Exception as e:
            return False, [f"Syntax validation error: {str(e)}"]
    
    def _check_databricks_syntax(self, sql: str) -> List[str]:
        """Check for Databricks-specific syntax that needs conversion."""
        warnings = []
        sql_upper = sql.upper()
        
        # Check for Databricks-specific functions
        databricks_functions = [
            'DATE_FORMAT', 'UNIX_TIMESTAMP', 'FROM_UNIXTIME',
            'COLLECT_LIST', 'COLLECT_SET', 'SIZE', 'SORT_ARRAY',
            'ARRAY_CONTAINS', 'EXPLODE', 'POSEXPLODE'
        ]
        
        for func in databricks_functions:
            if func in sql_upper:
                warnings.append(f"Databricks function '{func}' may need conversion")
        
        # Check for Databricks LIMIT syntax (LIMIT without ORDER BY)
        if 'LIMIT' in sql_upper and 'ORDER BY' not in sql_upper:
            warnings.append("LIMIT without ORDER BY may produce inconsistent results")
        
        # Check for Delta Lake specific syntax
        delta_keywords = ['MERGE INTO', 'VACUUM', 'OPTIMIZE', 'DESCRIBE HISTORY']
        for keyword in delta_keywords:
            if keyword in sql_upper:
                warnings.append(f"Delta Lake syntax '{keyword}' not supported in Postgres")
        
        return warnings
    
    def _check_postgres_compatibility(self, sql: str) -> List[str]:
        """Check for Postgres compatibility issues."""
        warnings = []
        sql_upper = sql.upper()
        
        # Check for missing table aliases in JOINs
        if 'JOIN' in sql_upper:
            join_pattern = r'JOIN\s+(\w+)\s+ON'
            joins = re.findall(join_pattern, sql, re.IGNORECASE)
            for join_table in joins:
                if not re.search(f'{join_table}\\s+AS\\s+\\w+', sql, re.IGNORECASE):
                    if not re.search(f'{join_table}\\s+\\w+\\s+ON', sql, re.IGNORECASE):
                        warnings.append(f"Consider adding alias for table '{join_table}' in JOIN")
        
        # Check for proper quoting of identifiers
        identifier_pattern = r'\\b[A-Z][A-Z_]+\\b'
        identifiers = re.findall(identifier_pattern, sql)
        for identifier in identifiers:
            if identifier in self.postgres_keywords:
                warnings.append(f"Reserved keyword '{identifier}' should be quoted")
        
        return warnings


class QueryConverter:
    """Converts Databricks SQL queries to Postgres SQL using LLM."""
    
    def __init__(self, databricks_token: str, databricks_endpoint: str, model_name: str = "databricks-meta-llama-3-1-70b-instruct"):
        if not OPENAI_AVAILABLE:
            raise ImportError("OpenAI package required for LLM conversion")
        
        self.client = OpenAI(
            api_key=databricks_token,
            base_url=databricks_endpoint
        )
        self.model_name = model_name
        self.validator = SQLSyntaxValidator()
        
        # Conversion statistics
        self.stats = {
            'total_files': 0,
            'successful_conversions': 0,
            'failed_conversions': 0,
            'warnings': 0
        }
    
    def convert_query(self, sql: str, query_description: str = "") -> Tuple[str, List[str], bool]:
        """Convert a single SQL query from Databricks to Postgres format."""
        
        # Create detailed conversion prompt
        conversion_prompt = self._create_conversion_prompt(sql, query_description)
        
        try:
            # Call LLM for conversion
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert SQL developer specializing in converting Databricks SQL to PostgreSQL. Provide only the converted SQL query without explanations unless there are critical issues."
                    },
                    {
                        "role": "user",
                        "content": conversion_prompt
                    }
                ],
                max_tokens=4000,
                temperature=0.1,  # Low temperature for consistent results
                top_p=0.9
            )
            
            converted_sql = response.choices[0].message.content.strip()
            
            # Clean up the response (remove markdown formatting if present)
            converted_sql = self._clean_sql_response(converted_sql)
            
            # Validate the converted SQL
            is_valid, validation_warnings = self.validator.validate_syntax(converted_sql)
            
            return converted_sql, validation_warnings, is_valid
            
        except Exception as e:
            error_msg = f"LLM conversion failed: {str(e)}"
            logger.error(error_msg)
            return sql, [error_msg], False
    
    def _create_conversion_prompt(self, sql: str, description: str = "") -> str:
        """Create a detailed prompt for SQL conversion."""
        
        prompt = f"""Convert the following Databricks SQL query to PostgreSQL-compatible SQL.

**Important conversion requirements:**

1. **Date/Time Functions:**
   - Convert DATE_FORMAT() to TO_CHAR()
   - Convert UNIX_TIMESTAMP() to EXTRACT(epoch FROM timestamp)
   - Convert FROM_UNIXTIME() to TO_TIMESTAMP()
   - Use CURRENT_TIMESTAMP instead of NOW() where appropriate

2. **String Functions:**
   - Convert CONCAT() to CONCAT() or || operator
   - Convert SUBSTRING() to SUBSTR() if needed
   - Use ILIKE for case-insensitive LIKE operations

3. **Array/Collection Functions:**
   - Convert COLLECT_LIST() to ARRAY_AGG()
   - Convert COLLECT_SET() to ARRAY_AGG(DISTINCT ...)
   - Convert SIZE() to ARRAY_LENGTH()
   - Convert EXPLODE() to unnest()

4. **Data Types:**
   - Convert BIGINT to INTEGER where appropriate
   - Use VARCHAR instead of STRING
   - Use BOOLEAN instead of BOOL

5. **Window Functions:**
   - Ensure proper PARTITION BY and ORDER BY clauses
   - Use standard SQL window function syntax

6. **Query Structure:**
   - Maintain proper JOIN syntax
   - Use explicit table aliases
   - Ensure LIMIT is used with ORDER BY for consistent results

7. **Postgres-Specific Optimizations:**
   - Use appropriate indexing hints in comments
   - Consider using WITH clauses for complex subqueries
   - Use proper casting with ::datatype syntax where beneficial

{f"**Query Description:** {description}" if description else ""}

**Original Databricks SQL:**
```sql
{sql}
```

**Requirements:**
- Provide only the converted PostgreSQL query
- Maintain the same logical functionality
- Optimize for PostgreSQL performance where possible
- Ensure the query is syntactically correct
- If any conversion is not possible, add a comment explaining why

**Converted PostgreSQL Query:**"""
        
        return prompt
    
    def _clean_sql_response(self, response: str) -> str:
        """Clean up LLM response to extract SQL only."""
        # Remove markdown code blocks
        response = re.sub(r'^```sql\\n', '', response, flags=re.MULTILINE)
        response = re.sub(r'^```\\n', '', response, flags=re.MULTILINE)
        response = re.sub(r'\\n```$', '', response, flags=re.MULTILINE)
        response = re.sub(r'^```', '', response, flags=re.MULTILINE)
        response = re.sub(r'```$', '', response, flags=re.MULTILINE)
        
        # Remove any leading/trailing explanatory text
        lines = response.split('\\n')
        sql_lines = []
        in_sql = False
        
        for line in lines:
            line = line.strip()
            # Start capturing from the first SQL-like line
            if not in_sql and (
                line.upper().startswith(('SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE', 'CREATE')) or
                line.startswith('--') or
                line.startswith('/*')
            ):
                in_sql = True
            
            if in_sql:
                sql_lines.append(line)
        
        return '\\n'.join(sql_lines).strip()
    
    def convert_file(self, source_file: str, target_file: str) -> bool:
        """Convert a single SQL file."""
        logger.info(f"Converting {source_file} -> {target_file}")
        
        try:
            # Read source file
            with open(source_file, 'r', encoding='utf-8') as f:
                original_sql = f.read()
            
            if not original_sql.strip():
                logger.warning(f"Empty file: {source_file}")
                return False
            
            # Extract query description from comments
            description = self._extract_description(original_sql)
            
            # Convert the query
            converted_sql, warnings, is_valid = self.convert_query(original_sql, description)
            
            # Update statistics
            if warnings:
                self.stats['warnings'] += len(warnings)
                for warning in warnings:
                    logger.warning(f"  Warning: {warning}")
            
            # Create output with metadata
            output_content = self._create_output_file(
                original_sql, converted_sql, source_file, warnings, is_valid
            )
            
            # Ensure target directory exists
            os.makedirs(os.path.dirname(target_file), exist_ok=True)
            
            # Write converted file
            with open(target_file, 'w', encoding='utf-8') as f:
                f.write(output_content)
            
            if is_valid:
                self.stats['successful_conversions'] += 1
                logger.info(f"  ✓ Successfully converted with {len(warnings)} warnings")
            else:
                self.stats['failed_conversions'] += 1
                logger.error(f"  ✗ Conversion completed but validation failed")
            
            return True
            
        except Exception as e:
            logger.error(f"  ✗ Error converting file: {str(e)}")
            self.stats['failed_conversions'] += 1
            return False
    
    def _extract_description(self, sql: str) -> str:
        """Extract description from SQL comments."""
        lines = sql.split('\\n')
        description_lines = []
        
        for line in lines:
            line = line.strip()
            if line.startswith('--'):
                comment = line[2:].strip()
                if comment and not comment.upper().startswith(('SELECT', 'FROM', 'WHERE')):
                    description_lines.append(comment)
            elif not line or line.upper().startswith(('SELECT', 'WITH', 'INSERT')):
                break
        
        return ' '.join(description_lines)
    
    def _create_output_file(self, original_sql: str, converted_sql: str, 
                           source_file: str, warnings: List[str], is_valid: bool) -> str:
        """Create formatted output file with metadata."""
        
        timestamp = datetime.now().isoformat()
        filename = os.path.basename(source_file)
        
        output = f"""/*
 * Converted from Databricks SQL to PostgreSQL
 * 
 * Source file: {filename}
 * Conversion timestamp: {timestamp}
 * Validation status: {'PASSED' if is_valid else 'FAILED'}
 * Warnings: {len(warnings)}
 */

"""
        
        if warnings:
            output += "/*\\n * CONVERSION WARNINGS:\\n"
            for i, warning in enumerate(warnings, 1):
                output += f" * {i}. {warning}\\n"
            output += " */\\n\\n"
        
        # Add original query as comment for reference
        output += "/*\\n * ORIGINAL DATABRICKS SQL:\\n"
        for line in original_sql.split('\\n'):
            output += f" * {line}\\n"
        output += " */\\n\\n"
        
        # Add converted query
        output += "-- CONVERTED POSTGRESQL QUERY:\\n"
        output += converted_sql
        
        if not converted_sql.endswith('\\n'):
            output += '\\n'
        
        return output
    
    def convert_directory(self, source_dir: str, target_dir: str, 
                         file_pattern: str = "*.sql") -> Dict[str, Any]:
        """Convert all SQL files in a directory."""
        
        source_path = Path(source_dir)
        target_path = Path(target_dir)
        
        if not source_path.exists():
            raise FileNotFoundError(f"Source directory not found: {source_dir}")
        
        # Find all SQL files
        sql_files = list(source_path.glob(file_pattern))
        
        if not sql_files:
            logger.warning(f"No SQL files found in {source_dir}")
            return self.stats
        
        logger.info(f"Found {len(sql_files)} SQL files to convert")
        
        # Convert each file
        for source_file in sql_files:
            self.stats['total_files'] += 1
            
            # Create target file path
            relative_path = source_file.relative_to(source_path)
            target_file = target_path / relative_path
            
            self.convert_file(str(source_file), str(target_file))
        
        return self.stats
    
    def generate_conversion_report(self, output_file: str = "conversion_report.json") -> None:
        """Generate a detailed conversion report."""
        
        report = {
            'conversion_summary': self.stats,
            'timestamp': datetime.now().isoformat(),
            'conversion_settings': {
                'model_name': self.model_name,
                'validation_enabled': SQLPARSE_AVAILABLE
            }
        }
        
        try:
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2)
            
            logger.info(f"Conversion report saved to {output_file}")
            
        except Exception as e:
            logger.error(f"Failed to save conversion report: {e}")


def load_config(config_file: str) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    if not YAML_AVAILABLE:
        return {}
    
    try:
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.warning(f"Config file not found: {config_file}")
        return {}
    except Exception as e:
        logger.warning(f"Error loading config: {e}")
        return {}


def main():
    parser = argparse.ArgumentParser(
        description='Convert Databricks SQL queries to PostgreSQL format using LLM'
    )
    
    # Input/Output options
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--source-dir',
        help='Source directory containing Databricks SQL files'
    )
    group.add_argument(
        '--file',
        help='Single SQL file to convert'
    )
    
    parser.add_argument(
        '--target-dir',
        help='Target directory for converted Postgres SQL files'
    )
    parser.add_argument(
        '--output',
        help='Output file for single file conversion'
    )
    
    # LLM Configuration (from environment variables only)
    # Required: DATABRICKS_ACCESS_TOKEN, DATABRICKS_ENDPOINT
    # Optional: MODEL_NAME (defaults set below)
    
    # Processing options
    parser.add_argument(
        '--config',
        help='Configuration file (YAML format)'
    )
    parser.add_argument(
        '--file-pattern',
        default="*.sql",
        help='File pattern for directory conversion (default: *.sql)'
    )
    parser.add_argument(
        '--report',
        default="conversion_report.json",
        help='Conversion report output file'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate setup without converting files'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Load configuration
    config = {}
    if args.config:
        config = load_config(args.config)
    
    # Read credentials from environment
    databricks_token = os.environ.get('DATABRICKS_ACCESS_TOKEN')
    databricks_endpoint = os.environ.get('DATABRICKS_ENDPOINT')
    model_name = os.environ.get('MODEL_NAME', "databricks-meta-llama-3-1-70b-instruct")

    # Validate required parameters
    if not databricks_token:
        logger.error("Databricks token required. Set DATABRICKS_ACCESS_TOKEN in your environment or .env file")
        sys.exit(1)
    if not databricks_endpoint:
        logger.error("Databricks endpoint required. Set DATABRICKS_ENDPOINT in your environment or .env file")
        sys.exit(1)
    
    if not OPENAI_AVAILABLE:
        logger.error("OpenAI package required. Install with: pip install openai")
        sys.exit(1)
    
    # Set up output paths
    if args.source_dir:
        if not args.target_dir:
            logger.error("--target-dir required when using --source-dir")
            sys.exit(1)
    elif args.file:
        if not args.output:
            # Default output file
            input_path = Path(args.file)
            args.output = str(input_path.parent / "target" / input_path.name)
    
    if args.dry_run:
        logger.info("Dry run mode - validating setup...")
        logger.info(f"Databricks endpoint: {databricks_endpoint}")
        logger.info(f"Model: {model_name}")
        logger.info(f"SQL validation: {'enabled' if SQLPARSE_AVAILABLE else 'disabled'}")
        logger.info("Setup validation completed successfully")
        return
    
    # Initialize converter
    try:
        converter = QueryConverter(
            databricks_token=databricks_token,
            databricks_endpoint=databricks_endpoint,
            model_name=model_name
        )
    except Exception as e:
        logger.error(f"Failed to initialize converter: {e}")
        sys.exit(1)
    
    # Perform conversion
    try:
        if args.source_dir:
            logger.info(f"Converting directory: {args.source_dir} -> {args.target_dir}")
            stats = converter.convert_directory(args.source_dir, args.target_dir, args.file_pattern)
            
            # Print summary
            logger.info("\\n" + "="*50)
            logger.info("CONVERSION SUMMARY")
            logger.info("="*50)
            logger.info(f"Total files processed: {stats['total_files']}")
            logger.info(f"Successful conversions: {stats['successful_conversions']}")
            logger.info(f"Failed conversions: {stats['failed_conversions']}")
            logger.info(f"Total warnings: {stats['warnings']}")
            
            if stats['successful_conversions'] > 0:
                success_rate = (stats['successful_conversions'] / stats['total_files']) * 100
                logger.info(f"Success rate: {success_rate:.1f}%")
            
        else:
            logger.info(f"Converting file: {args.file} -> {args.output}")
            success = converter.convert_file(args.file, args.output)
            if success:
                logger.info("File conversion completed successfully")
            else:
                logger.error("File conversion failed")
                sys.exit(1)
        
        # Generate report
        if args.report:
            converter.generate_conversion_report(args.report)
        
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
