from . import _version
from prefect_duckdb.config import DuckDBConfig  # noqa
from prefect_duckdb.database import DuckDBConnector  # noqa

__version__ = _version.get_versions()["version"]
