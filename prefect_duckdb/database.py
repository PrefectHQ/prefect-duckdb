"""Module for querying against Snowflake databases."""

import asyncio
from typing import Any, Dict, List, Optional, Tuple, Iterable
import duckdb
from duckdb import DuckDBPyConnection
from prefect import task
from prefect.blocks.abstract import DatabaseBlock
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible, run_async_from_worker_thread
from prefect.utilities.hashing import hash_objects
from .config import DuckConfig
from pydantic import VERSION as PYDANTIC_VERSION

if PYDANTIC_VERSION.startswith("2."):
    from pydantic.v1 import Field
else:
    from pydantic import Field



class DuckConnector(DatabaseBlock):

    _block_type_name = "DuckDB connector"
    _logo_url = "https://placeholder.com"  # noqa
    _documentation_url = "https://placeholder.com"  # noqa
    _description = "Perform data operations against a DuckDb database."

    configuration: DuckConfig = Field(
        default=..., description="The credentials to authenticate with Snowflake."
    )
    database: str = Field(
        default=":memory:", description="The name of the default database to use."
    )
    read_only: bool = Field(
        default=False,
        description="Whether the connection should be read-only.",
    )

    _connection: Optional[DuckDBPyConnection] = None
    

    def get_connection(self, **connect_kwargs: Any) -> DuckDBPyConnection:
        if self._connection is not None:
            return self._connection
        
        connection = duckdb.connect(database=self.database, read_only=self.read_only)
       
        self._connection = connection
        self.logger.info(f"Started a new connection to {self.database}.")
        return connection

    def _get_cursor(
        self,
        inputs: Dict[str, Any],
    ) -> Tuple[bool, DuckDBPyConnection]:
        
        input_hash = hash_objects(inputs)
        if input_hash is None:
            raise RuntimeError(
                "We were not able to hash your inputs, "
                "which resulted in an unexpected data return; "
                "please open an issue with a reproducible example."
            )
        if input_hash not in self._unique_cursors.keys():
            new_cursor = self._connection.cursor()
            self._unique_cursors[input_hash] = new_cursor
            return True, new_cursor
        else:
            existing_cursor = self._unique_cursors[input_hash]
            return False, existing_cursor
    
    @sync_compatible
    async def fetch_one(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Any]:
        if parameters is None:
            parameters = []
        cursor = self._connection.cursor()
        await run_sync_in_worker_thread(cursor.execute, operation, parameters)
        self.logger.debug("Preparing to fetch a row.")
        result = await run_sync_in_worker_thread(cursor.fetchone)
        return result


    @sync_compatible
    async def fetch_many(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        size: Optional[int] = 1,
    ) -> List[Tuple[Any]]:
        if parameters is None:
            parameters = []
        cursor = self._connection.cursor()
        await run_sync_in_worker_thread(cursor.execute, operation, parameters)
        size = size
        self.logger.debug(f"Preparing to fetch {size} rows.")
        result = await run_sync_in_worker_thread(cursor.fetchmany, size=size)
        return result

    @sync_compatible
    async def fetch_all(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> List[Tuple[Any]]:
       
        if parameters is None:
            parameters = []
        cursor = self._connection.cursor()
        await run_sync_in_worker_thread(cursor.execute, operation, parameters)
        self.logger.debug("Preparing to fetch all rows.")
        result = await run_sync_in_worker_thread(cursor.fetchall)
        return result

    @sync_compatible
    async def execute(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> DuckDBPyConnection:
        
        if parameters is None:
            parameters = []

        with self._connection.cursor() as cursor:
            await run_sync_in_worker_thread(cursor.execute, operation, parameters)
        self.logger.info(f"Executed the operation, {operation!r}.")

    @sync_compatible
    async def execute_many(
        self,
        operation: str,
        parameters: Iterable[Iterable[Any]],
    ) -> DuckDBPyConnection:
        if parameters is None:
            parameters = []
        with self._connection.cursor() as cursor:
            await run_sync_in_worker_thread(cursor.executemany, operation, parameters)
        self.logger.info(
            f"Executed {len(parameters)} operations off {operation!r}."
        )

    def close(self):
        """
        Closes connection and its cursors.
        """
        try:
            self.reset_cursors()
        finally:
            if self._connection is None:
                self.logger.info("There was no connection open to be closed.")
                return
            self._connection.close()
            self._connection = None
            self.logger.info("Successfully closed the Snowflake connection.")

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



