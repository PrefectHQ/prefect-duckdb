from prefect.blocks import CredentialsBlock
from pydantic import Field
from typing import Literal
from duckdb import DuckDBPyConnection
class DuckSecrets(CredentialsBlock):
    _block_type_name = "DuckDB Secret"
    _logo_url = ""  # noqa
    _documentation_url = ""  # noqa

    name = Field(
        ...,
        description="The name of the secret",
    )
    type: Literal["S3", "GCS", "R2","AZURE"] = Field(
        "S3",
        description="The type of the secret",
    )
    key_id = Field(
        ...,
        description="The key ID of the secret",
    )
    secret = Field(
        ...,
        description="The secret"
    )
    region = Field(
        ...,
        description="The region of the secret",
    )
    scope = Field(
        ...,
        description="The scope of the secret",
    )   
    def create_secret(connection:DuckDBPyConnection, name, type, key_id, secret, region, scope):
        return connection.execute(f"""CREATE SECRET {name} 
                                    TYPE {type}
                                    KEY_ID {key_id}
                                    SECRET {secret} 
                                    REGION {region} 
                                    SCOPE {scope}'""")
    def drop_secret(connection:DuckDBPyConnection, name):
        return connection.execute(f"""DROP SECRET {name}""")
    def list_secrets(connection:DuckDBPyConnection):
        return connection.execute(f"""FROM duckdb_secrets();""")
    


