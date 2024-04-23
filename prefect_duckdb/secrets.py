"""This module contains the DuckDB secrets block."""
from typing import Literal, Optional

from duckdb import DuckDBPyConnection
from prefect.blocks.core import Block
from pydantic import VERSION as PYDANTIC_VERSION

if PYDANTIC_VERSION.startswith("2."):
    from pydantic.v1 import Field, SecretStr
else:
    from pydantic import Field, SecretStr


class DuckDBSecrets(Block):
    """A block for storing DuckDB secrets."""

    _block_type_name = "DuckDB Secret"
    _logo_url = "https://duckdb.org/images/logo-dl/DuckDB_Logo.png"  # noqa
    _documentation_url = "https://duckdb.org/images/logo-dl/DuckDB_Logo.png"  # noqa
    name: str = Field(
        ...,
        description="The name of the secret",
    )
    secret_type: Literal["S3", "GCS", "R2", "AZURE"] = Field(
        "S3",
        description="The type of the secret",
    )
    key_id: Optional[str] = Field(
        None,
        description="The key ID of the secret",
    )
    secret: Optional[SecretStr] = Field(None, description="The secret")
    region: Optional[str] = Field(
        None,
        description="The region of the secret",
    )
    scope: Optional[str] = Field(
        None,
        description="The scope of the secret",
    )

    def create_secret(
        self,
        connection: DuckDBPyConnection,
    ):
        """Create a secret in DuckDB."""

        args = []
        if self.key_id:
            args.append(f"KEY_ID '{self.key_id}'")
        if self.secret:
            args.append(f"SECRET '{self.secret}'")
        if self.region:
            args.append(f"REGION '{self.region}'")
        if self.scope:
            args.append(f"SCOPE '{self.scope}'")

        argstring = ", ".join(args)
        connection.execute(
            f"""CREATE SECRET {self.name} ( TYPE {self.secret_type}, {argstring});"""
        )

    def drop_secret(connection: DuckDBPyConnection, name):
        """Drop a secret in DuckDB."""
        return connection.execute(f"""DROP SECRET {name}""")

    def list_secrets(connection: DuckDBPyConnection):
        """List all secrets in DuckDB."""
        return connection.execute("FROM duckdb_secrets();")
