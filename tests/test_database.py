import multiprocessing

import pytest
from duckdb import DuckDBPyConnection

from prefect_duckdb.config import DuckConfig
from prefect_duckdb.database import DuckConnector


class TestDuckConnector:
    @pytest.fixture
    def duck_connector(self):
        connector = DuckConnector(configuration=DuckConfig(), read_only=False)
        return connector

    @pytest.fixture
    def duck_connection(self, duck_connector):
        return duck_connector.get_connection()

    def test_block_initialization(self, duck_connector):
        assert duck_connector._connection is None

    def test_get_connection(self, duck_connector: DuckConnector, caplog):
        connection = duck_connector.get_connection()
        assert duck_connector._connection is connection
        assert caplog.records[0].msg == "Started a new connection to :memory:."

    def test_execute(self, duck_connector: DuckConnector):
        duck_connector.get_connection()
        cursor = duck_connector.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
        assert type(cursor) is DuckDBPyConnection

    def test_fetch_one(self, duck_connector: DuckConnector):
        duck_connector.get_connection()
        duck_connector.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
        duck_connector.execute("INSERT INTO test_table VALUES (1, 'one')")
        result = duck_connector.fetch_one("SELECT * FROM test_table")
        assert result == (1, "one")

    def test_fetch_many(self, duck_connector: DuckConnector):
        duck_connector.get_connection()
        duck_connector.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
        duck_connector.execute_many(
            "INSERT INTO test_table VALUES (?, ?)",
            parameters=[[1, "one"], [2, "two"], [3, "three"]],
        )
        result = duck_connector.fetch_many("SELECT * FROM test_table", size=2)
        assert result == [(1, "one"), (2, "two")]
        result = duck_connector.fetch_many("SELECT * FROM test_table")
        assert result == [(1, "one")]

    def test_fetch_all(self, duck_connector: DuckConnector):
        duck_connector.get_connection()
        duck_connector.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
        duck_connector.execute("INSERT INTO test_table VALUES (1, 'one')")
        result = duck_connector.fetch_all("SELECT * FROM test_table")
        assert result == [(1, "one")]

    def test_fetch_pandas_all(self, duck_connector: DuckConnector):
        import pandas as pd

        duck_connector.get_connection()
        duck_connector.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
        duck_connector.execute("INSERT INTO test_table VALUES (1, 'one')")
        result = duck_connector.fetch_df("SELECT * FROM test_table")
        assert isinstance(result, pd.DataFrame)
        assert result.iloc[0, 0] == 1

    def test_create_function(self, duck_connector: DuckConnector):
        duck_connector.get_connection()

        def add_one(x: int) -> int:
            return x + 1

        duck_connector.create_function("add_one", add_one)
        result = duck_connector.fetch_one("SELECT add_one(1)")[0]
        assert result == 2

    def test_get_threads(self, duck_connection: DuckDBPyConnection):
        threads = duck_connection.sql(
            "SELECT current_setting('threads') AS threads;"
        ).fetchone()[0]
        assert threads == multiprocessing.cpu_count()
