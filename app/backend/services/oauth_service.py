import os
import uuid
from urllib.parse import urlencode
from typing import Dict, Optional, Any
import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

logger = logging.getLogger(__name__)


def _is_resource_not_found(e: Exception) -> bool:
    """Return True if the error indicates resource not found."""
    msg = (getattr(e, "message", None) or str(e)).lower()
    return "resource not found" in msg or "not found" in msg


def get_postgres_token_credentials(
    access_token: str,
    endpoint_host: str,
    database: Optional[str] = None,
    postgres_user_name: Optional[str] = None,
) -> Dict[str, str]:
    """
    Lakebase OAuth: use only the Postgres token from Lakebase Connect (Copy OAuth token)
    and the endpoint host from the same dialog. No workspace or API calls.
    """
    token = (access_token or "").strip()
    if not token:
        raise Exception("Postgres OAuth token is required.")
    host = (endpoint_host or "").strip()
    if not host:
        raise Exception("Endpoint host is required (from Lakebase Connect dialog).")
    user_name = (postgres_user_name or "").strip()
    if not user_name:
        raise Exception(
            "Postgres user is required for OAuth. Use your Databricks email or username "
            "(the identity that has the Postgres role in the Lakebase project)."
        )
    return {
        "host": host,
        "port": "5432",
        "database": database or "databricks_postgres",
        "user": user_name,
        "password": token,
        "ssl_mode": "require",
    }


class DatabricksOAuthService:
    """
    Handles OAuth authentication for Databricks Lakebase access
    (both Provisioned and Autoscaling) following Databricks security best practices.
    """
    
    def __init__(self):
        self._workspace_client = None
    
    def _ensure_client(self, workspace_url: str, auth_method: str, profile: Optional[str]) -> None:
        """Initialize workspace client if not already set."""
        if self._workspace_client is not None:
            return
        if auth_method == "service_principal":
            self._workspace_client = WorkspaceClient(
                host=workspace_url,
                client_id=os.getenv('DATABRICKS_CLIENT_ID'),
                client_secret=os.getenv('DATABRICKS_CLIENT_SECRET')
            )
        else:
            self._workspace_client = (
                WorkspaceClient(profile=profile, host=workspace_url)
                if profile
                else WorkspaceClient(host=workspace_url)
            )
    
    def _get_credentials_provisioned(
        self, instance_name: str, database: Optional[str]
    ) -> Dict[str, str]:
        """Get credentials for Lakebase Provisioned (database instance)."""
        instance = self._workspace_client.database.get_database_instance(name=instance_name)
        request_id = str(uuid.uuid4())
        cred = self._workspace_client.database.generate_database_credential(
            request_id=request_id,
            instance_names=[instance_name]
        )
        user_name = self._workspace_client.current_user.me().user_name
        return {
            "host": instance.read_write_dns,
            "port": "5432",
            "database": database or "databricks_postgres",
            "user": user_name,
            "password": cred.token,
            "ssl_mode": "require"
        }
    
    def _get_credentials_autoscaling(
        self, instance_name: str, database: Optional[str]
    ) -> Dict[str, str]:
        """
        Get credentials for Lakebase Autoscaling (project/branch/endpoint).
        Uses SDK postgres API if available, otherwise falls back to REST via api_client.
        instance_name can be: project ID, projects/xxx, or display name.
        """
        if hasattr(self._workspace_client, "postgres"):
            return self._get_credentials_autoscaling_sdk(instance_name, database)
        return self._get_credentials_autoscaling_rest(instance_name, database)
    
    def _get_credentials_autoscaling_sdk(
        self, instance_name: str, database: Optional[str]
    ) -> Dict[str, str]:
        """Autoscaling credentials via w.postgres (requires databricks-sdk>=0.77)."""
        postgres = self._workspace_client.postgres
        project_name = f"projects/{instance_name}" if not instance_name.startswith("projects/") else instance_name
        
        try:
            project = postgres.get_project(name=project_name)
        except Exception as e:
            if _is_resource_not_found(e):
                for p in postgres.list_projects():
                    if getattr(getattr(p, "spec", None), "display_name", None) == instance_name:
                        project_name = p.name
                        project = p
                        break
                else:
                    raise Exception(
                        f"Lakebase project not found: '{instance_name}'. "
                        "Use the project ID from the URL or the project's display name."
                    ) from e
            else:
                raise
        
        if not project or not project.name:
            raise Exception(f"Could not resolve project for: {instance_name}")
        project_name = project.name
        
        branches = list(postgres.list_branches(parent=project_name))
        if not branches:
            raise Exception(f"No branches found in project {project_name}")
        branch = branches[0]
        branch_name = branch.name
        
        endpoints = list(postgres.list_endpoints(parent=branch_name))
        if not endpoints:
            raise Exception(f"No endpoints found in branch {branch_name}")
        endpoint = None
        for ep in endpoints:
            if getattr(ep, "status", None) and getattr(ep.status, "hosts", None) and getattr(ep.status.hosts, "host", None):
                state = getattr(ep.status, "current_state", None)
                if state and str(state).upper() in ("ACTIVE", "IDLE"):
                    endpoint = ep
                    break
        if endpoint is None:
            endpoint = endpoints[0]
        
        host = None
        if endpoint.status and endpoint.status.hosts:
            host = getattr(endpoint.status.hosts, "host", None) or getattr(endpoint.status.hosts, "read_only_host", None)
        if not host:
            raise Exception(f"Endpoint {endpoint.name} has no host; it may still be initializing.")
        
        cred = postgres.generate_database_credential(endpoint=endpoint.name)
        user_name = self._workspace_client.current_user.me().user_name
        token = getattr(cred, "token", None) or (cred.as_dict() if hasattr(cred, "as_dict") else {}).get("token")
        if not token:
            raise Exception("Failed to obtain OAuth token for Lakebase Autoscaling.")
        
        return {
            "host": host,
            "port": "5432",
            "database": database or "databricks_postgres",
            "user": user_name,
            "password": token,
            "ssl_mode": "require"
        }
    
    def _postgres_rest(self, method: str, path: str, body: Optional[Dict[str, Any]] = None, query: Optional[Dict[str, str]] = None) -> Any:
        """Call Postgres REST API; path is without query string. Query params appended when provided."""
        api = self._workspace_client.api_client
        full_path = path
        if query:
            full_path = f"{path}?{urlencode(query)}"
        if method == "GET":
            return api.do(method, full_path)
        return api.do(method, full_path, body=body or {})
    
    def _get_credentials_autoscaling_rest(
        self, instance_name: str, database: Optional[str]
    ) -> Dict[str, str]:
        """Autoscaling credentials via REST API (works when SDK has no postgres attribute)."""
        project_name = f"projects/{instance_name}" if not instance_name.startswith("projects/") else instance_name
        project: Any = None
        
        # Try get project: GET /api/2.0/postgres/projects/{project_id}
        project_id = project_name.replace("projects/", "", 1) if project_name.startswith("projects/") else project_name
        try:
            project = self._postgres_rest("GET", f"/api/2.0/postgres/projects/{project_id}")
        except Exception as e:
            if _is_resource_not_found(e):
                list_resp = self._postgres_rest("GET", "/api/2.0/postgres/projects", query={"page_size": "100"})
                projects = list_resp.get("projects") or []
                for p in projects:
                    if (p.get("spec") or {}).get("display_name") == instance_name:
                        project_name = p.get("name") or project_name
                        project = p
                        break
                else:
                    raise Exception(
                        f"Lakebase project not found: '{instance_name}'. "
                        "Use the project ID from the URL or the project's display name."
                    ) from e
            else:
                raise
        
        if isinstance(project, dict):
            project_name = project.get("name") or project_name
        else:
            project_name = getattr(project, "name", None) or project_name
        
        if not project_name:
            raise Exception(f"Could not resolve project for: {instance_name}")
        
        branches_resp = self._postgres_rest("GET", "/api/2.0/postgres/branches", query={"parent": project_name})
        branches = branches_resp.get("branches") or []
        if not branches:
            raise Exception(f"No branches found in project {project_name}")
        branch = branches[0]
        branch_name = branch.get("name") if isinstance(branch, dict) else getattr(branch, "name", None)
        if not branch_name:
            raise Exception("Branch has no name")
        
        endpoints_resp = self._postgres_rest("GET", "/api/2.0/postgres/endpoints", query={"parent": branch_name})
        endpoints = endpoints_resp.get("endpoints") or []
        if not endpoints:
            raise Exception(f"No endpoints found in branch {branch_name}")
        endpoint = endpoints[0]
        endpoint_name = endpoint.get("name") if isinstance(endpoint, dict) else getattr(endpoint, "name", None)
        if not endpoint_name:
            raise Exception("Endpoint has no name")
        
        status = endpoint.get("status") if isinstance(endpoint, dict) else getattr(endpoint, "status", None)
        hosts = (status or {}).get("hosts") if isinstance(status, dict) else getattr(status, "hosts", None) if status else None
        host = None
        if hosts:
            host = hosts.get("host") or hosts.get("read_only_host") if isinstance(hosts, dict) else getattr(hosts, "host", None) or getattr(hosts, "read_only_host", None)
        if not host:
            get_ep = self._postgres_rest("GET", f"/api/2.0/postgres/{endpoint_name}")
            status = get_ep.get("status") or {}
            hosts = status.get("hosts") or {}
            host = hosts.get("host") or hosts.get("read_only_host")
        if not host:
            raise Exception(f"Endpoint {endpoint_name} has no host; it may still be initializing.")
        
        cred_resp = self._postgres_rest("POST", "/api/2.0/postgres/credentials", body={"endpoint": endpoint_name})
        token = cred_resp.get("token") if isinstance(cred_resp, dict) else getattr(cred_resp, "token", None)
        if not token:
            raise Exception("Failed to obtain OAuth token for Lakebase Autoscaling.")
        
        user_name = self._workspace_client.current_user.me().user_name
        return {
            "host": host,
            "port": "5432",
            "database": database or "databricks_postgres",
            "user": user_name,
            "password": token,
            "ssl_mode": "require"
        }
    
    async def get_database_credentials(
        self,
        workspace_url: str,
        instance_name: str,
        auth_method: str = "user",  # or "service_principal"
        profile: Optional[str] = None,
        database: Optional[str] = None,
        access_token: Optional[str] = None,
        postgres_user_name: Optional[str] = None,
        endpoint_host: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Obtain OAuth credentials for Lakebase access.
        Supports both Provisioned (database instance) and Autoscaling (project) Lakebase.
        Returns connection params suitable for psycopg2 (host, port, user, password, ssl_mode).

        For OAuth (postgres token only): pass access_token (Lakebase Connect → Copy OAuth token)
        and endpoint_host from the same dialog. No workspace URL or instance name needed.
        
        Args:
            workspace_url: Databricks workspace URL (required for non-token flow)
            instance_name: For Provisioned: instance name. For Autoscaling: project ID or display name
            auth_method: Authentication method (user/service_principal)
            profile: Optional Databricks CLI profile name
            database: Optional database name (default: databricks_postgres)
            access_token: Postgres OAuth token from Lakebase Connect (Copy OAuth token)
            postgres_user_name: Optional Postgres user (default: databricks_user)
            endpoint_host: Endpoint host from Lakebase Connect dialog (required with access_token)
            
        Returns:
            Dict containing: host, port, database, user, password, ssl_mode
        """
        token = (access_token or "").strip()
        host = (endpoint_host or "").strip()
        if token and host:
            logger.info("Using Postgres OAuth token (endpoint host only).")
            return get_postgres_token_credentials(
                access_token=token,
                endpoint_host=host,
                database=database,
                postgres_user_name=(postgres_user_name or "").strip() or None,
            )
        instance_name = (instance_name or "").strip()
        if not instance_name:
            raise Exception("instance_name is required")

        try:
            self._ensure_client(workspace_url, auth_method, profile)
            try:
                return self._get_credentials_provisioned(instance_name, database)
            except (DatabricksError, Exception) as e:
                if _is_resource_not_found(e):
                    logger.info("Provisioned instance not found, trying Lakebase Autoscaling project: %s", instance_name)
                    return self._get_credentials_autoscaling(instance_name, database)
                if isinstance(e, DatabricksError):
                    raise Exception(f"Failed to authenticate with Databricks: {e}") from e
                raise
        except DatabricksError as e:
            logger.error(f"Databricks API error: {e}")
            raise Exception(f"Failed to authenticate with Databricks: {e}") from e
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            raise
    
    async def validate_instance_access(
        self,
        workspace_url: str,
        instance_name: str,
        auth_method: str = "user",
        profile: Optional[str] = None
    ) -> bool:
        """
        Validate that the user has access to the specified Lakebase instance or project.
        Supports both Provisioned (instance) and Autoscaling (project).
        """
        try:
            self._ensure_client(workspace_url, auth_method, profile)
            instance_name = (instance_name or "").strip()
            if not instance_name:
                return False
            # Provisioned: get instance
            try:
                instance = self._workspace_client.database.get_database_instance(name=instance_name)
                return bool(instance and getattr(instance, "read_write_dns", None))
            except Exception as e:
                if not _is_resource_not_found(e):
                    logger.error("Instance validation error: %s", e)
                    return False
            # Autoscaling: resolve project and ensure we can list branches/endpoints
            try:
                self._get_credentials_autoscaling(instance_name, database="databricks_postgres")
                return True
            except Exception as e:
                logger.error("Autoscaling project validation error: %s", e)
                return False
        except Exception as e:
            logger.error("Instance validation error: %s", e)
            return False
    
    def get_workspace_client(self) -> Optional[WorkspaceClient]:
        """Get the current workspace client instance."""
        return self._workspace_client
