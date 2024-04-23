import pytest
from duckdb import DuckDBPyConnection

from prefect_duckdb.config import DuckDBConfig
from prefect_duckdb.database import DuckDBConnector

qplan = """The query plan for the operation is: 
┌───────────────────────────┐                             
│         PROJECTION        │                             
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                             
│            name           │                             
└─────────────┬─────────────┘                                                          
┌─────────────┴─────────────┐                             
│         HASH_JOIN         │                             
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                             
│           INNER           │                             
│         sid = sid         │                             
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ├──────────────┐              
│        Build Min: 1       │              │              
│        Build Max: 3       │              │              
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │              │              
│           EC: 1           │              │              
└─────────────┬─────────────┘              │                                           
┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│         SEQ_SCAN          ││           FILTER          │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│           exams           ││     prefix(name, 'Ma')    │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
│            sid            ││           EC: 1           │
│   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││                           │
│           EC: 3           ││                           │
└───────────────────────────┘└─────────────┬─────────────┘                             
                             ┌─────────────┴─────────────┐
                             │         SEQ_SCAN          │
                             │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
                             │          students         │
                             │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
                             │            sid            │
                             │            name           │
                             │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
                             │ Filters: name>=Ma AND name│
                             │  <Mb AND name IS NOT NULL │
                             │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
                             │           EC: 1           │a
                             └───────────────────────────┘                             
"""  # noqa: W291


class TestDuckDBConnector:
    @pytest.fixture
    def duck_connector(self):
        connector = DuckDBConnector(
            configuration=DuckDBConfig(), read_only=False, debug=False
        )
        return connector

    @pytest.fixture
    def duck_connection(self, duck_connector):
        return duck_connector.get_connection()

    def test_block_initialization(self, duck_connector):
        assert duck_connector._connection is None

    def test_get_connection(self, duck_connector: DuckDBConnector, caplog):
        connection = duck_connector.get_connection()
        assert duck_connector._connection is connection
        assert caplog.records[0].msg == "Started a new connection to :memory:."

    def test_execute(self, duck_connector: DuckDBConnector):
        duck_connector.get_connection()
        cursor = duck_connector.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
        assert type(cursor) is DuckDBPyConnection

    def test_execute_debug(self, duck_connector: DuckDBConnector, caplog):
        duck_connector.get_connection()
        duck_connector.execute("CREATE TABLE students (name VARCHAR, sid INTEGER);")
        duck_connector.execute(
            "CREATE TABLE exams (eid INTEGER, subject VARCHAR, sid INTEGER);"
        )
        duck_connector.execute(
            "INSERT INTO students VALUES ('Mark', 1), ('Joe', 2), ('Matthew', 3);"
        )
        duck_connector.execute(
            "INSERT INTO exams VALUES \n"
            "(10, 'Physics', 1), (20, 'Chemistry', 2), (30, 'Literature', 3);"
        )

        duck_connector.execute(
            "SELECT name FROM students JOIN exams USING (sid) WHERE name LIKE 'Ma%'",
            debug=True,
        )
        print(caplog.records[5])
        assert qplan == caplog.records[5].msg

    def test_fetch_one(self, duck_connector: DuckDBConnector):
        duck_connector.get_connection()
        cursor = duck_connector.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
        cursor.execute("INSERT INTO test_table VALUES (1, 'one')")
        result = duck_connector.fetch_one("SELECT * FROM test_table")
        assert result == (1, "one")

    def test_fetch_many(self, duck_connector: DuckDBConnector):
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

    def test_fetch_all(self, duck_connector: DuckDBConnector):
        duck_connector.get_connection()
        duck_connector.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
        duck_connector.execute("INSERT INTO test_table VALUES (1, 'one')")
        result = duck_connector.fetch_all("SELECT * FROM test_table")
        assert result == [(1, "one")]

    def test_fetch_numpy(self, duck_connector: DuckDBConnector):

        duck_connector.get_connection()
        duck_connector.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
        duck_connector.execute("INSERT INTO test_table VALUES (1, 'one')")
        result = duck_connector.fetch_numpy("SELECT * FROM test_table")
        assert isinstance(result, dict)
        assert result["i"] == 1

    def test_fetch_pandas(self, duck_connector: DuckDBConnector):
        import pandas as pd

        duck_connector.get_connection()
        duck_connector.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
        duck_connector.execute("INSERT INTO test_table VALUES (1, 'one')")
        result = duck_connector.fetch_df("SELECT * FROM test_table")
        assert isinstance(result, pd.DataFrame)
        assert result.iloc[0, 0] == 1

    def test_fetch_arrow(self, duck_connector: DuckDBConnector):
        import pyarrow as pa

        duck_connector.get_connection()
        duck_connector.execute("CREATE TABLE test_table (i INTEGER, j STRING)")
        duck_connector.execute("INSERT INTO test_table VALUES (1, 'one')")
        result = duck_connector.fetch_arrow("SELECT * FROM test_table")
        assert isinstance(result, pa.Table)
        assert result.to_pandas().iloc[0, 0] == 1

    def test_from_df(self, duck_connector: DuckDBConnector):
        import pandas as pd

        duck_connector.get_connection()
        df = pd.DataFrame.from_dict({"i": [1, 2, 3], "j": ["one", "two", "three"]})

        test_df = duck_connector.from_df(df, table_name="test_table")

        result = test_df.execute("SELECT * FROM test_table").fetchall()
        print(result)
        assert result == [(1, "one"), (2, "two"), (3, "three")]

    def test_create_function(self, duck_connector: DuckDBConnector):
        duck_connector.get_connection()

        def add_one(x: int) -> int:
            return x + 1

        duck_connector.create_function("add_one", add_one)
        result = duck_connector.fetch_one("SELECT add_one(1)")[0]
        assert result == 2
