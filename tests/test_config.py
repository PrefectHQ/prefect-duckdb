import pytest

from prefect_duckdb.config import DuckDBConfig
from prefect_duckdb.database import DuckDBConnector
from prefect_duckdb.secrets import DuckDBSecrets


class TestDuckDBConfig:
    @pytest.fixture
    def duck_connector(self):
        connector = DuckDBConnector(
            configuration=DuckDBConfig(), read_only=False, debug=False
        )
        return connector

    @pytest.fixture
    def duck_connection(self, duck_connector):
        return duck_connector.get_connection()

    def test_create_secret(self, duck_connection):

        secret = DuckDBSecrets(
            name="test_secret",
            type="S3",
            key_id="key",
            secret="secret",
            region="us-east-1",
        )
        secret.create_secret(connection=duck_connection)
        secrets = duck_connection.sql("FROM duckdb_secrets();").fetchall()[0]

        assert secrets[0] == "test_secret"
