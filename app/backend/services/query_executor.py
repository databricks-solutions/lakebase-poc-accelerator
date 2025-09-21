import logging
from typing import List, Any, Optional
import psycopg2
import psycopg2.extras
from models.query_models import QueryExecutionResult

logger = logging.getLogger(__name__)

class QueryExecutorService:
    """
    Handles secure query execution with parameterized queries
    to prevent SQL injection attacks.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    async def execute_parameterized_query(
        self,
        connection,
        query: str,
        parameters: List[Any],
        query_name: str
    ) -> QueryExecutionResult:
        """
        Execute a parameterized query safely.
        
        Args:
            connection: Database connection
            query: SQL query with %s placeholders
            parameters: List of parameter values
            query_name: Name for logging and metrics
            
        Returns:
            QueryExecutionResult with execution details
        """
        import time
        
        start_time = time.time()
        
        try:
            with connection.cursor() as cursor:
                # Execute parameterized query using psycopg2's safe parameter binding
                cursor.execute(query, parameters)
                
                # Get row count
                row_count = cursor.rowcount
                
                # Fetch results if it's a SELECT query
                if query.strip().upper().startswith('SELECT'):
                    results = cursor.fetchall()
                    row_count = len(results) if results else 0
                
                end_time = time.time()
                duration_ms = (end_time - start_time) * 1000
                
                self.logger.info(f"Query '{query_name}' executed successfully in {duration_ms:.2f}ms")
                
                return QueryExecutionResult(
                    query_identifier=query_name,
                    parameter_set_name="default",
                    execution_start_time=start_time,
                    execution_end_time=end_time,
                    duration_ms=duration_ms,
                    success=True,
                    rows_returned=row_count,
                    connection_id=str(id(connection))
                )
                
        except psycopg2.Error as e:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000
            
            self.logger.error(f"PostgreSQL error in query '{query_name}': {e}")
            
            return QueryExecutionResult(
                query_identifier=query_name,
                parameter_set_name="default",
                execution_start_time=start_time,
                execution_end_time=end_time,
                duration_ms=duration_ms,
                success=False,
                error_message=str(e),
                error_type="postgresql_error",
                connection_id=str(id(connection))
            )
            
        except Exception as e:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000
            
            self.logger.error(f"Unexpected error in query '{query_name}': {e}")
            
            return QueryExecutionResult(
                query_identifier=query_name,
                parameter_set_name="default",
                execution_start_time=start_time,
                execution_end_time=end_time,
                duration_ms=duration_ms,
                success=False,
                error_message=str(e),
                error_type="execution_error",
                connection_id=str(id(connection))
            )
    
    async def execute_stored_procedure(
        self,
        connection,
        procedure_name: str,
        parameters: List[Any],
        query_name: str
    ) -> QueryExecutionResult:
        """
        Execute stored procedure with proper cursor handling.
        
        Args:
            connection: Database connection
            procedure_name: Name of stored procedure
            parameters: List of parameter values
            query_name: Name for logging and metrics
            
        Returns:
            QueryExecutionResult with execution details
        """
        import time
        
        start_time = time.time()
        
        try:
            with connection.cursor() as cursor:
                # Execute stored procedure
                cursor.execute(f"CALL {procedure_name}(%s)", parameters)
                
                # Get row count
                row_count = cursor.rowcount
                
                # Handle multiple result sets if any
                results = []
                while True:
                    try:
                        result = cursor.fetchall()
                        if result:
                            results.append(result)
                        else:
                            break
                    except psycopg2.ProgrammingError:
                        # No more result sets
                        break
                
                end_time = time.time()
                duration_ms = (end_time - start_time) * 1000
                
                self.logger.info(f"Stored procedure '{procedure_name}' executed successfully in {duration_ms:.2f}ms")
                
                return QueryExecutionResult(
                    query_identifier=query_name,
                    parameter_set_name="default",
                    execution_start_time=start_time,
                    execution_end_time=end_time,
                    duration_ms=duration_ms,
                    success=True,
                    rows_returned=row_count,
                    connection_id=str(id(connection))
                )
                
        except psycopg2.Error as e:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000
            
            self.logger.error(f"PostgreSQL error in stored procedure '{procedure_name}': {e}")
            
            return QueryExecutionResult(
                query_identifier=query_name,
                parameter_set_name="default",
                execution_start_time=start_time,
                execution_end_time=end_time,
                duration_ms=duration_ms,
                success=False,
                error_message=str(e),
                error_type="postgresql_error",
                connection_id=str(id(connection))
            )
            
        except Exception as e:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000
            
            self.logger.error(f"Unexpected error in stored procedure '{procedure_name}': {e}")
            
            return QueryExecutionResult(
                query_identifier=query_name,
                parameter_set_name="default",
                execution_start_time=start_time,
                execution_end_time=end_time,
                duration_ms=duration_ms,
                success=False,
                error_message=str(e),
                error_type="execution_error",
                connection_id=str(id(connection))
            )
    
    def validate_query_safety(self, query: str) -> tuple[bool, str]:
        """
        Validate that a query is safe to execute (basic SQL injection prevention).
        
        Args:
            query: SQL query string
            
        Returns:
            Tuple of (is_safe, error_message)
        """
        # Convert to uppercase for checking
        query_upper = query.upper()
        
        # Check for dangerous SQL operations
        dangerous_operations = [
            'DROP', 'DELETE', 'UPDATE', 'INSERT', 'CREATE', 'ALTER',
            'TRUNCATE', 'EXEC', 'EXECUTE', 'UNION', '--', '/*', '*/'
        ]
        
        for operation in dangerous_operations:
            if operation in query_upper:
                return False, f"Dangerous SQL operation detected: {operation}"
        
        # Check for proper parameterization
        if '%' in query and '%s' not in query:
            return False, "Found % character but no %s placeholders. Use %s for parameters."
        
        return True, ""
    
    def get_query_type(self, query: str) -> str:
        """
        Determine the type of SQL query.
        
        Args:
            query: SQL query string
            
        Returns:
            Query type (SELECT, CALL, etc.)
        """
        query_upper = query.strip().upper()
        
        if query_upper.startswith('SELECT'):
            return 'SELECT'
        elif query_upper.startswith('CALL'):
            return 'CALL'
        elif query_upper.startswith('INSERT'):
            return 'INSERT'
        elif query_upper.startswith('UPDATE'):
            return 'UPDATE'
        elif query_upper.startswith('DELETE'):
            return 'DELETE'
        else:
            return 'OTHER'
