"""Feature routers. Importing these modules registers their routes on the
singleton APIRouter returned by ``core.create_router()``."""

from . import lakebase as lakebase  # noqa: F401
from . import testing as testing  # noqa: F401
from . import optimize as optimize  # noqa: F401
from . import deployment as deployment  # noqa: F401
from . import history as history  # noqa: F401
