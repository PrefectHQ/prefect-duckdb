"""Module for querying against Snowflake databases."""

from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import duckdb
from duckdb import DuckDBPyConnection
from prefect.blocks.abstract import DatabaseBlock
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible
from pydantic import VERSION as PYDANTIC_VERSION

from .config import DuckConfig

if PYDANTIC_VERSION.startswith("2."):
    from pydantic.v1 import Field
else:
    from pydantic import Field


class DuckConnector(DatabaseBlock):
    """
    A block for connecting to a DuckDB database.

    Args:
        configuration: DuckConfig block to be used when creating connection.
        database: The name of the default database to use.
        read_only: Whether the connection should be read-only.
    """

    _block_type_name = "DuckDB connector"
    _logo_url = "https://placeholder.com"  # noqa
    _documentation_url = "https://placeholder.com"  # noqa
    _description = "Perform data operations against a DuckDb database."

    configuration: DuckConfig = Field(
        default=..., description="Configuration to be used when creating connection."
    )
    database: str = Field(
        default=":memory:", description="The name of the default database to use."
    )
    read_only: bool = Field(
        default=False,
        description="Whether the connection should be read-only.",
    )
    _connection: Optional[DuckDBPyConnection] = None

    def get_connection(
        self, read_only: Optional[bool] = None, config: Optional[DuckConfig] = None
    ) -> DuckDBPyConnection:
        """
        Returns an authenticated connection that can be
        used to query from Snowflake databases.

        Args:
            **connect_kwargs: Additional arguments to pass to
                `snowflake.connector.connect`.

        Returns:
            A `DuckDBPyConnection` object.
        """
        if self._connection is not None:
            return self._connection
        config = config or self.configuration.dict(
            exclude_none=True, exclude={"block_type_slug"}
        )
        read_only = read_only or self.read_only
        connection = duckdb.connect(
            database=self.database,
            read_only=read_only,
            config=config,
        )

        self._connection = connection
        self.logger.info(f"Started a new connection to {self.database}.")
        return connection

    @sync_compatible
    async def fetch_one(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = [],
    ) -> Tuple[Any]:
        """
        Fetch a single result from the database.

        Args:
            operation: The SQL operation to execute.
            parameters: The parameters to pass to the operation.
        """
        with self._connection.cursor() as cursor:
            await run_sync_in_worker_thread(cursor.execute, operation, parameters)
            self.logger.debug("Preparing to fetch a row.")
            result = await run_sync_in_worker_thread(cursor.fetchone)
            return result

    @sync_compatible
    async def fetch_many(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = [],
        size: Optional[int] = 1,
    ) -> List[Tuple[Any]]:
        """
        Fetch multiple results from the database.
        Args:
            operation: The SQL operation to execute.
            parameters: The parameters to pass to the operation.
            size: The number of rows to fetch.
        """
        with self._connection.cursor() as cursor:
            await run_sync_in_worker_thread(cursor.execute, operation, parameters)
            size = size
            self.logger.debug(f"Preparing to fetch {size} rows.")
            result = await run_sync_in_worker_thread(cursor.fetchmany, size=size)
            return result

    @sync_compatible
    async def fetch_all(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = [],
    ) -> List[Tuple[Any]]:
        """
        Fetch all results from the database.
        Args:
            operation: The SQL operation to execute.
            parameters: The parameters to pass to the operation.
        """
        with self._connection.cursor() as cursor:
            await run_sync_in_worker_thread(cursor.execute, operation, parameters)
            self.logger.debug("Preparing to fetch all rows.")
            result = await run_sync_in_worker_thread(cursor.fetchall)
            return result

    @sync_compatible
    async def execute(
        self,
        operation: str,
        parameters: Optional[Iterable[Any]] = [],
    ) -> DuckDBPyConnection:
        """
        Execute an operation against the database.
        Args:
            operation: The SQL operation to execute.
            parameters: The parameters to pass to the operation.
        """
        with self._connection.cursor() as cursor:
            cursor = await run_sync_in_worker_thread(
                cursor.execute, operation, parameters
            )
            self.logger.info(f"Executed the operation, {operation!r}.")
            return cursor

    @sync_compatible
    async def execute_many(
        self,
        operation: str,
        parameters: Iterable[Iterable[Any]] = [],
    ) -> DuckDBPyConnection:
        """
        Execute operations with using prepared statements.
        """
        with self._connection.cursor() as cursor:
            await run_sync_in_worker_thread(cursor.executemany, operation, parameters)
            self.logger.info(
                f"Executed {len(parameters)} operations off {operation!r}."
            )

    @sync_compatible
    async def fetch_numpy(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = [],
    ) -> Any:
        """
        Fetch all results of the query from the database as a numpy array.
        Args:
            operation: The SQL operation to execute.
            parameters: The parameters to pass to the operation.
        """
        with self._connection.cursor() as cursor:
            await run_sync_in_worker_thread(cursor.execute, operation, parameters)
            self.logger.debug("Preparing to fetch all rows.")
            result = await run_sync_in_worker_thread(cursor.fetchnumpy)
            return result

    @sync_compatible
    async def fetch_df(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = [],
    ) -> Any:
        """
        Fetch all results of the query from the database as a dataframe.
        Args:
            operation: The SQL operation to execute.
            parameters: The parameters to pass to the operation.
        """
        with self._connection.cursor() as cursor:
            await run_sync_in_worker_thread(cursor.execute, operation, parameters)
            self.logger.debug("Preparing to fetch all rows.")
            result = await run_sync_in_worker_thread(cursor.df)
            return result

    @sync_compatible
    async def fetch_arrow(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = [],
    ) -> Any:
        """
        Fetch all results of the query from the database as an Arrow table.
        Args:
            operation: The SQL operation to execute.
            parameters: The parameters to pass to the operation.
        """
        with self._connection.cursor() as cursor:
            await run_sync_in_worker_thread(cursor.execute, operation, parameters)
            self.logger.debug("Preparing to fetch all rows.")
            result = await run_sync_in_worker_thread(cursor.arrow)
            return result

    @sync_compatible
    async def create_function(
        self,
        name: str,
        func: Callable,
        parameters: Optional[Dict[str, Any]] = None,
        return_type: Optional[str] = None,
        side_effects: bool = False,
    ) -> None:
        """
        Create a function in the database.
        Args:
            name: string representing the unique name of the UDF within the catalog.
            func: The Python function you wish to register as a UDF.
            parameters: This parameter takes a list of column types used as input.
            return_type: Scalar functions return one element per row.
                         This parameter specifies the return type of the function.
            side_effects: Whether the function has side effects.
        """
        with self._connection.cursor() as cursor:
            await run_sync_in_worker_thread(
                cursor.create_function,
                name,
                func,
                parameters,
                return_type,
                side_effects=side_effects,
            )
            self.logger.info(f"Created function {name!r}.")

    def remove_function(self, name: str) -> None:
        """
        Remove a function from the database.
        Args:
            name: string representing the unique name of the UDF within the catalog.
        """
        self._connection.remove_function(name)

    def close(self):
        """
        Closes connection and its cursors.
        """
        if self._connection is None:
            self.logger.info("There was no connection open to be closed.")
            return
        self._connection.close()
        self._connection = None
        self.logger.info("Successfully closed the DuckDB connection.")

    def __enter__(self):
        """
        Start a connection upon entry.
        """
        return self

    def __exit__(self, *args):
        """
        Closes connection and its cursors upon exit.
        """
        self.close()

    def __getstate__(self):
        """Allows block to be pickled and dumped."""
        data = self.__dict__.copy()
        data.update({k: None for k in {"_connection"}})
        return data

    def __setstate__(self, data: dict):
        """Reset connection and cursors upon loading."""
        self.__dict__.update(data)
