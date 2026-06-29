from ._factory import create_app as create_app, create_router as create_router
from .dependencies import Dependencies as Dependencies
from ._config import logger as logger

# NOTE: The apx `lakebase` addon ships `core/lakebase.py`, a startup-bound
# LifespanDependency for a single *Provisioned* instance (ws.database + SQLModel).
# This app is autoscaling-only and connects on-demand to user-selected targets, so
# we intentionally do NOT import/register that dependency (importing it would auto-
# register its lifespan and crash boot without a bound instance). It is kept on disk
# only as a reference pattern for the credential-callback engine setup.
