
from unittest.mock import MagicMock
from pydantic import SecretBytes, SecretStr
from prefect_duckdb.database import DuckConnector
from prefect_duckdb.config import DuckConfig
import pytest
from duckdb import DuckDBPyConnection
from prefect import flow
import multiprocessing

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
        assert duck_connector.execute("CREATE TABLE test_table (i INTEGER, j STRING)", ) is None

    def test_fetch_one(self, duck_connector: DuckConnector):
        duck_connector.get_connection()
        duck_connector.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
        duck_connector.execute("INSERT INTO test_table VALUES (1, 'one')")
        result = duck_connector.fetch_one("SELECT * FROM test_table")
        assert result == (1, 'one')

    def test_fetch_many(self, duck_connector: DuckConnector):
        duck_connector.get_connection()
        duck_connector.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
        duck_connector.execute_many("INSERT INTO test_table VALUES (?, ?)" , parameters=[[1, 'one'],[2, 'two'], [3, 'three']])
        result = duck_connector.fetch_many("SELECT * FROM test_table", size=2)
        assert result == [(1, 'one'), (2, 'two')]
        result = duck_connector.fetch_many("SELECT * FROM test_table")
        assert result == [(1, 'one')]

    def test_fetch_all(self, duck_connector: DuckConnector):
        duck_connector.get_connection()
        duck_connector.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
        duck_connector.execute("INSERT INTO test_table VALUES (1, 'one')")
        result = duck_connector.fetch_all("SELECT * FROM test_table")

        assert result == [(1, 'one')]
   
    def test_get_threads(self, duck_connection: DuckDBPyConnection):
        threads = duck_connection.sql("SELECT current_setting('threads') AS threads;").fetchone()[0]
        assert threads == multiprocessing.cpu_count()