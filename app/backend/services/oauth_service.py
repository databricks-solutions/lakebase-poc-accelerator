import os
import uuid
from typing import Dict, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
import logging

logger = logging.getLogger(__name__)

class DatabricksOAuthService:
    """
    Handles OAuth authentication for Databricks Lakebase access
    following Databricks security best practices.
    """
    
    def __init__(self):
        self._workspace_client = None
    
    async def get_database_credentials(
        self,
        workspace_url: str,
        instance_name: str,
        auth_method: str = "user"  # or "service_principal"
    ) -> Dict[str, str]:
        """
        Obtain OAuth credentials for Lakebase access.
        
        Args:
            workspace_url: Databricks workspace URL
            instance_name: Lakebase instance name
            auth_method: Authentication method (user/service_principal)
            
        Returns:
            Dict containing connection credentials
        """
        try:
            # Initialize workspace client based on auth method
            if auth_method == "service_principal":
                self._workspace_client = WorkspaceClient(
                    host=workspace_url,
                    client_id=os.getenv('DATABRICKS_CLIENT_ID'),
                    client_secret=os.getenv('DATABRICKS_CLIENT_SECRET')
                )
            else:
                # User authentication - uses default profile or environment variables
                self._workspace_client = WorkspaceClient(host=workspace_url)
            
            # Get database instance information
            instance = self._workspace_client.database.get_database_instance(name=instance_name)
            
            # Generate database credentials
            request_id = str(uuid.uuid4())
            cred = self._workspace_client.database.generate_database_credential(
                request_id=request_id,
                instance_names=[instance_name]
            )
            
            return {
                "host": instance.read_write_dns,
                "port": "5432",
                "database": "databricks_postgres",
                "user": os.getenv('DATABRICKS_USER_EMAIL', 'user@databricks.com'),
                "password": cred.token,
                "ssl_mode": "require"
            }
            
        except DatabricksError as e:
            logger.error(f"Databricks API error: {e}")
            raise Exception(f"Failed to authenticate with Databricks: {e}")
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            raise Exception(f"Authentication failed: {e}")
    
    async def validate_instance_access(
        self,
        workspace_url: str,
        instance_name: str,
        auth_method: str = "user"
    ) -> bool:
        """
        Validate that the user has access to the specified Lakebase instance.
        
        Args:
            workspace_url: Databricks workspace URL
            instance_name: Lakebase instance name
            auth_method: Authentication method
            
        Returns:
            True if access is valid, False otherwise
        """
        try:
            # Initialize workspace client
            if auth_method == "service_principal":
                self._workspace_client = WorkspaceClient(
                    host=workspace_url,
                    client_id=os.getenv('DATABRICKS_CLIENT_ID'),
                    client_secret=os.getenv('DATABRICKS_CLIENT_SECRET')
                )
            else:
                self._workspace_client = WorkspaceClient(host=workspace_url)
            
            # Try to get instance information
            instance = self._workspace_client.database.get_database_instance(name=instance_name)
            
            # Check if instance is accessible
            if instance and instance.read_write_dns:
                return True
            else:
                return False
                
        except DatabricksError as e:
            logger.error(f"Instance validation error: {e}")
            return False
        except Exception as e:
            logger.error(f"Instance validation error: {e}")
            return False
    
    def get_workspace_client(self) -> Optional[WorkspaceClient]:
        """Get the current workspace client instance."""
        return self._workspace_client
