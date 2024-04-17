from . import _version
from prefect_duckdb.config import DuckConfig  # noqa
from prefect_duckdb.database import DuckConnector  # noqa

__version__ = _version.get_versions()["version"]
