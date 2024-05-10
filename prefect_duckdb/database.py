"""Module for querying against DuckDB databases."""

import os
import re
from typing import Any, Callable, Dict, Iterable, List, Literal, Optional, Tuple

import duckdb
import pandas
from duckdb import DuckDBPyConnection, DuckDBPyRelation
from prefect import get_client, task
from prefect.artifacts import create_markdown_artifact
from prefect.blocks.abstract import DatabaseBlock
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible
from pydantic import VERSION as PYDANTIC_VERSION

if PYDANTIC_VERSION.startswith("2."):
    from pydantic.v1 import Field, SecretStr
else:
    from pydantic import Field, SecretStr


class DuckDBConnector(DatabaseBlock):
    """
    A block for connecting to a DuckDB database.

    Args:
        configuration: DuckDBConfig block to be used when creating connection.
        database: The name of the default database to use.
        read_only: Whether the connection should be read-only.

    Examples:
        Load stored DuckDB connector as a context manager:
        ```python
        from prefect_duckdb.database import DuckDBConnector

        duckdb_connector = DuckDBConnector.load("BLOCK_NAME"):
        ```

        Insert data into database and fetch results.
        ```python
        from prefect_duckdb.database import DuckDBConnector

        with DuckDBConnector.load("BLOCK_NAME") as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS customers (name varchar, address varchar);"
            )
            conn.execute_many(
                "INSERT INTO customers (name, address) VALUES (%(name)s, %(address)s);",
                parameters=[
                    {"name": "Ford", "address": "Highway 42"},
                    {"name": "Unknown", "address": "Space"},
                    {"name": "Me", "address": "Myway 88"},
                ],
            )
            results = conn.fetch_all(
                "SELECT * FROM customers WHERE address = %(address)s",
                parameters={"address": "Space"}
            )
            print(results)
        ```
    """

    _block_type_name = "DuckDB Connector"
    _logo_url = "https://duckdb.org/images/logo-dl/DuckDB_Logo.png"  # noqa
    _documentation_url = "https://placeholder.com"  # noqa
    _description = "Perform data operations against a DuckDb database."

    configuration: Optional[dict] = Field(
        default=None, description="Configuration to be used when creating connection."
    )
    database: str = Field(
        default=":memory:", description="The name of the default database to use."
    )
    read_only: bool = Field(
        default=False,
        description="Whether the connection should be read-only.",
    )
    _connection: Optional[DuckDBPyConnection] = None
    _debug: bool = False

    def get_connection(
        self, read_only: Optional[bool] = None, config: Optional[dict] = None
    ) -> DuckDBPyConnection:
        """
        Returns a  DuckDB connection, if `mother_ducktoken` is found in enviroment
        or config, it will be passed in the connection.

        Args:
            read_only: Whether the connection should be read-only.
            config: Configuration to be used when creating connection.

        Returns:
            A `DuckDBPyConnection` object.

        Examples:
            ```python
            from prefect_duckdb.database import DuckDBConnector

            duckdb_connector = DuckDBConnector.load("BLOCK_NAME")

            with duckdb_connector as conn:
                conn.execute("CREATE TABLE test_table (i INTEGER, j STRING);")
                ...
            ```
        """
        if self._connection is not None:
            return self._connection

        config = config or self.configuration or {}
        read_only = read_only or self.read_only

        if os.environ.get("motherduck_token") and "motherduck_token" not in config:
            config["motherduck_token"] = os.environ.get("motherduck_token")

        connection = duckdb.connect(
            database=self.database,
            read_only=read_only,
            config=config,
        )

        self._connection = connection
        self.logger.info(f"Started a new connection to {self.database}.")
        return connection

    @sync_compatible
    async def execute(
        self,
        operation: str,
        parameters: Optional[Iterable[Any]] = [],
        multiple_parameter_sets: bool = False,
        debug: Optional[bool] = False,
    ) -> DuckDBPyConnection:
        """
        Execute the given SQL query, optionally using prepared statements
        with parameters set.

        Args:
            operation: The SQL operation to execute.
            parameters: The parameters to pass to the operation.
            multiple_parameter_sets: Whether to execute the operation multiple times.
            debug: Whether to run the operation in debug mode.
                   Sends the query plan to the logger.

        Examples:
            ```python
            from prefect_duckdb.database import DuckDBConnector

            with DuckDBConnector.load("BLOCK_NAME") as conn:
                conn.execute(
                    "CREATE TABLE test_table (i INTEGER, j STRING)"
                )
            ```
        """
        self.get_connection()
        cursor = self._connection.cursor()
        if self._debug or debug:
            await self.create_query_plan_markdown(operation, cursor, parameters)

        cursor = await run_sync_in_worker_thread(
            cursor.execute, operation, parameters, multiple_parameter_sets
        )
        self.logger.info(f"Executed the operation, {operation!r}.")
        return cursor

    @sync_compatible
    async def sql(
        self,
        operation: str,
        debug: Optional[bool] = False,
    ) -> DuckDBPyRelation:
        """
        Execute the given SQL query, optionally using prepared statements
        with parameters set.

        Args:
            operation: The SQL operation to execute.
            debug: Whether to run the operation in debug mode.
                   Sends the query plan to the logger.

        Examples:
            ```python
            from prefect_duckdb.database import DuckDBConnector

            with DuckDBConnector.load("BLOCK_NAME") as conn:
                conn.sql(
                    "CREATE TABLE test_table (i INTEGER, j STRING)"
                )
            ```
        """
        self.get_connection()
        cursor = self._connection.cursor()
        if self._debug or debug:
            await self.create_query_plan_markdown(operation=operation, cursor=cursor)
        cursor = await run_sync_in_worker_thread(cursor.sql, operation)
        self.logger.info(f"Executed the operation, {operation!r}.")
        return cursor

    @sync_compatible
    async def execute_many(
        self,
        operation: str,
        parameters: Iterable[Iterable[Any]] = [],
        debug: Optional[bool] = False,
    ) -> DuckDBPyConnection:
        """
        Execute the given prepared statement multiple times using the
        list of parameter sets in parameters

        Args:
            operation: The SQL operation to execute.
            parameters: The parameters to pass to the operation.
            debug: Whether to run the operation in debug mode.
                   Sends the query plan to the logger.

        Examples:
            ```python
                from prefect_duckdb.database import DuckDBConnector

                with DuckDBConnector.load("BLOCK_NAME") as conn:
                    conn.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
                    conn.execute_many(
                        "INSERT INTO test_table VALUES (?, ?)",
                        parameters=[[1, "one"], [2, "two"], [3, "three"]]
                    )
            ```
        """
        cursor = self._connection.cursor()
        if self._debug or debug:
            await self.create_query_plan_markdown(operation, cursor, parameters)
        await run_sync_in_worker_thread(cursor.executemany, operation, parameters)
        self.logger.info(f"Executed {len(parameters)} operations off {operation!r}.")
        return cursor

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

        Returns:
            A tuple representing the result.

        Examples:
            ```python
            from prefect_duckdb.database import DuckDBConnector

            with DuckDBConnector.load("BLOCK_NAME") as conn:
                conn.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
                conn.execute("INSERT INTO test_table VALUES (1, 'one')")
                result = conn.fetch_one("SELECT * FROM test_table")
                print(result)
            ```
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

        Returns:
            A list of tuples representing the results.

        Examples:
            ```python
            from prefect_duckdb.database import DuckDBConnector

            with DuckDBConnector.load("BLOCK_NAME") as conn:
                conn.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
                conn.execute_many(
                    "INSERT INTO test_table VALUES (?, ?)",
                    parameters=[[1, "one"], [2, "two"], [3, "three"]]
                )
                result = conn.fetch_many("SELECT * FROM test_table", size=2)
                print(result)
            ```
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

        Returns:
            A list of tuples representing the results.

        Examples:
            ```python
            from prefect_duckdb.database import DuckDBConnector

            with DuckDBConnector.load("BLOCK_NAME") as conn:
                conn.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
                conn.execute("INSERT INTO test_table VALUES (1, 'one')")
                result = conn.fetch_all("SELECT * FROM test_table")
                print(result)
            ```
        """
        with self._connection.cursor() as cursor:
            await run_sync_in_worker_thread(cursor.execute, operation, parameters)
            self.logger.debug("Preparing to fetch all rows.")
            result = await run_sync_in_worker_thread(cursor.fetchall)
            return result

    @sync_compatible
    async def fetch_numpy(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = [],
    ) -> dict:
        """
        Fetch all results of the query from the database as a numpy array.
        Args:
            operation: The SQL operation to execute.
            parameters: The parameters to pass to the operation.

        Returns:
            A dictionary representing a numpy array.

        Examples:
            ```python
            from prefect_duckdb.database import DuckDBConnector

            with DuckDBConnector.load("BLOCK_NAME") as conn:
                conn.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
                conn.execute("INSERT INTO test_table VALUES (1, 'one')")
                result = conn.fetch_numpy("SELECT * FROM test_table")
                print(result)
            ```
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
        date_as_object: bool = False,
    ) -> pandas.DataFrame:
        """
        Fetch all results of the query from the database as a dataframe.

        Args:
            operation: The SQL operation to execute.
            parameters: The parameters to pass to the operation.

        Returns:
            A pandas dataframe.

        Examples:
            ```python
            from prefect_duckdb.database import DuckDBConnector

            with DuckDBConnector.load("BLOCK_NAME") as conn:
                conn.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
                conn.execute("INSERT INTO test_table VALUES (1, 'one')")
                result = conn.fetch_df("SELECT * FROM test_table")
                print(result)
            ```
        """
        with self._connection.cursor() as cursor:
            await run_sync_in_worker_thread(
                cursor.execute, operation, parameters, date_as_object
            )
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

        Returns:
            An Arrow table.

        Examples:
            ```python
            from prefect_duckdb.database import DuckDBConnector

            with DuckDBConnector.load("BLOCK_NAME") as conn:
                conn.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
                conn.execute("INSERT INTO test_table VALUES (1, 'one')")
                result = conn.fetch_arrow("SELECT * FROM test_table")
                print(result)
            ```
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

    def create_secret(
        self,
        name: str,
        secret_type: Literal["S3", "AZURE"],
        key_id: Optional[str] = None,
        secret: Optional[str] = None,
        region: Optional[str] = None,
        scope: Optional[str] = None,
    ):
        """Create a secret in DuckDB.

        Args:
            name: The name of the secret.
            secret_type: The type of secret.
            key_id: The key ID.
            secret: The secret.
            region: The region.
            scope: The scope.

        Examples:
            ```python
            from prefect_duckdb.database import DuckDBConnector
            from prefect_aws import AwsCredentials

            aws_credentials_block = AwsCredentials.load("BLOCK_NAME")
            connector = DuckDBConnector().load("BLOCK_NAME")
            connector.get_connection()
            connector.create_secret(
                name="test_secret",
                secret_type="S3",
                key_id=aws_credentials_block.access_key,
                secret=aws_credentials_block.secret_access_key,
                region=aws_credentials_block.region_name
            )
            connector.execute("SELECT count(*) FROM 's3://<bucket>/<file>';")
            ```
        """
        if not self._connection:
            self.get_connection()

        args = []
        if type(secret) == SecretStr:
            secret = secret.get_secret_value()
        if key_id:
            args.append(f"KEY_ID '{key_id}'")
        if secret:
            args.append(f"SECRET '{secret}'")
        if region:
            args.append(f"REGION '{region}'")
        if scope:
            args.append(f"SCOPE '{scope}'")

        argstring = ", ".join(args)
        self._connection.execute(
            f"""CREATE SECRET {name} ( TYPE {secret_type}, {argstring});"""
        )

    @sync_compatible
    async def from_csv_auto(
        self,
        file_name: str,
    ) -> DuckDBPyRelation:
        """
        Create a table from a CSV file.

        Args:
            file_name: The name of the CSV file.
        """
        with self._connection.cursor() as cursor:
            return await run_sync_in_worker_thread(cursor.from_csv_auto, file_name)

    @sync_compatible
    async def from_df(
        self,
        df: pandas.DataFrame,
        table_name: Optional[str] = None,
    ) -> DuckDBPyRelation:
        """
        Create a table from a Pandas DataFrame.

        Args:
            df: The Pandas DataFrame.
            table_name: The name of the table.
        """
        cursor = self._connection.cursor()
        table = await run_sync_in_worker_thread(cursor.from_df, df)
        if table_name:
            await run_sync_in_worker_thread(cursor.register, table_name, table)
        return cursor

    @sync_compatible
    async def from_arrow(self, arrow_object) -> DuckDBPyRelation:
        """
        Create a table from an Arrow object.

        Args:
            arrow_object: The Arrow object.
        """
        with self._connection.cursor() as cursor:
            return await run_sync_in_worker_thread(cursor.from_arrow, arrow_object)

    @sync_compatible
    async def from_parquet(
        self,
        file_name: str,
    ) -> DuckDBPyRelation:
        """
        Create a table from a Parquet file.

        Args:
            file_name: The name of the Parquet file.
        """
        with self._connection.cursor() as cursor:
            return await run_sync_in_worker_thread(cursor.from_parquet, file_name)

    def remove_function(self, name: str) -> None:
        """
        Remove a function from the database.

        Args:
            name: string representing the unique name of the UDF within the catalog.
        """
        self._connection.remove_function(name)

    def set_debug(self, debug: bool) -> None:
        """
        Set the debug mode of the connector.

        Args:
            debug: Whether to enable debug mode.
        """
        self._debug = debug
        self.logger.info(f"Set debug mode to {debug}.")

    async def create_query_plan_markdown(
        self,
        operation: str,
        cursor: DuckDBPyConnection,
        parameters: Optional[list] = [],
    ):
        debug_operation = f"""EXPLAIN \
                            {operation}"""
        plan = cursor.execute(debug_operation, parameters)
        plan = plan.df()
        plan = plan.rename(columns={"explain_value": "Physical_Plan"})[
            "Physical_Plan"
        ].to_markdown(index=False)

        markdown = f"""
```
{plan}
```
"""
        artifact_key = re.sub("[^A-Za-z0-9 ]+", "", operation).lower().replace(" ", "-")

        self.logger.info(markdown)
        async with get_client():
            return await create_markdown_artifact(
                key=artifact_key,
                markdown=markdown,
                description="The query plan for the operation.",
            )

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


@task
def duckdb_query(
    query: str,
    duckdb_connector: DuckDBConnector,
    parameters: Optional[Iterable[Any]] = [],
    debug: Optional[bool] = False,
) -> List[Tuple[Any]]:
    """
    Execute a query against a DuckDB database.

    Args:
        query: The SQL query to execute.
        duckdb_connector: The DuckDBConnector block to use.
        parameters: The parameters to pass to the operation.

    Returns:
        A list of tuples representing the results.

    Examples:
        ```python
        from prefect import Flow
        from prefect_duckdb.database import DuckDBConnector, duckdb_query

        @flow
        def duckdb_query_flow():
            duckdb_connector = DuckDBConnector.load("BLOCK_NAME")

            result = duckdb_query("SELECT * FROM test_table", duckdb_connector)
            print(result)

        duckdb_query_flow()
        ```

    """
    result = duckdb_connector.execute(query, parameters, debug=debug)
    return result
