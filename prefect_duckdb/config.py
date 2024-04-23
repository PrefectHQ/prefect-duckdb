"""This is an example blocks module"""

from typing import Optional

from prefect.blocks.core import Block
from pydantic import VERSION as PYDANTIC_VERSION

if PYDANTIC_VERSION.startswith("2."):
    from pydantic.v1 import Field
else:
    from pydantic import Field


class DuckDBConfig(Block):
    """A block for configuring DuckDB."""

    _block_type_name = "DuckDB configuation"
    _logo_url = "https://placeholder.com"  # noqa
    _documentation_url = "https://placeholder.com"  # noqa

    calendar: Optional[str] = Field(
        None,
        description="The current calendar",
    )
    timezone: Optional[str] = Field(
        None,
        description="The current time zone",
    )
    access_mode: Optional[str] = Field(
        "automatic",
        description=(
            "Access mode of the database (AUTOMATIC, READ_ONLY or READ_WRITE)"
        ),
    )
    allocator_flush_threshold: Optional[str] = Field(
        "128.0 MiB",
        description=(
            "Peak allocation threshold at which to flush the allocator after"
            "completing a task."
        ),
    )

    allow_persistent_secrets: Optional[bool] = Field(
        True,
        description=(
            "Allow the creation of persistent secrets, that are stored"
            "and loaded on restarts"
        ),
    )
    allow_unredacted_secrets: Optional[bool] = Field(
        False,
        description=("Allow printing unredacted secrets"),
    )
    allow_unsigned_extensions: Optional[bool] = Field(
        False,
        description=("Allow to load extensions with invalid or missing signatures"),
    )
    arrow_large_buffer_size: Optional[bool] = Field(
        False,
        description=(
            "If arrow buffers for strings, blobs, uuids and bits"
            "should be exported using large buffers"
        ),
    )
    autoinstall_extension_repository: Optional[str] = Field(
        None,
        description=(
            "Overrides the custom endpoint for extension " "installation on autoloading"
        ),
    )
    autoinstall_known_extensions: Optional[bool] = Field(
        True,
        description=(
            "Whether known extensions are allowed to be automatically"
            " installed when a query depends on them"
        ),
    )
    autoload_known_extensions: Optional[bool] = Field(
        True,
        description=(
            "Whether known extensions are allowed to be automatically "
            "loaded when a query depends on them"
        ),
    )
    binary_as_string: Optional[bool] = Field(
        None, description=("In Parquet files, interpret binary data as a string.")
    )
    ca_cert_file: Optional[str] = Field(
        None,
        description=("Path to a custom certificate file for self-signed certificates."),
    )
    checkpoint_threshold: Optional[str] = Field(
        "16.0 MiB",
        description=(
            "The WAL size threshold at which to automatically "
            "trigger a checkpoint (e.g., 1GB)"
        ),
    )
    custom_extension_repository: Optional[str] = Field(
        None,
        description=("Overrides the custom endpoint for remote extension installation"),
    )
    custom_user_agent: Optional[str] = Field(
        None,
        description=("Metadata from DuckDB callers"),
    )
    default_collation: Optional[str] = Field(
        None,
        description=("The collation setting used when none is specified"),
    )
    default_null_order: Optional[str] = Field(
        "NULLS_LAST",
        description="Null ordering used when none is specified "
        "(NULLS_FIRST or NULLS_LAST)",
    )
    default_order: Optional[str] = Field(
        "ASC",
        description="The order type used when none is specified (ASC or DESC)",
    )
    default_secret_storage: Optional[str] = Field(
        "local_file",
        description="Allows switching the default storage for secrets",
    )
    disabled_filesystems: Optional[str] = Field(
        None,
        description=(
            "Disable specific file systems preventing access (e.g., LocalFileSystem)"
        ),
    )
    duckdb_api: Optional[str] = Field(
        "cli",
        description=("DuckDB API surface"),
    )
    enable_external_access: Optional[bool] = Field(
        True,
        description=(
            "Allow the database to access external state (e.g., loading/installing"
            " modules, COPY TO/FROM, CSV readers, pandas replacement scans, etc)"
        ),
    )
    enable_fsst_vectors: Optional[bool] = Field(
        False,
        description=(
            "Allow scans on FSST compressed segments to emit"
            " compressed vectors to utilize late decompression"
        ),
    )
    enable_http_metadata_cache: Optional[bool] = Field(
        False,
        description=(
            "Whether or not the global http metadata is used to cache HTTP metadata"
        ),
    )
    enable_object_cache: Optional[bool] = Field(
        False,
        description=(
            "Whether or not object cache is used to cache e.g., Parquet metadata"
        ),
    )

    extension_directory: Optional[str] = Field(
        None,
        description=("Set the directory to store extensions in"),
    )
    external_threads: Optional[int] = Field(
        1,
        description=("The number of external threads that work on DuckDB tasks."),
    )
    immediate_transaction_mode: Optional[bool] = Field(
        False,
        description=(
            "Whether transactions should be started lazily when needed"
            ", or immediately when BEGIN TRANSACTION is called"
        ),
    )
    lock_configuration: Optional[bool] = Field(
        False,
        description=("Whether or not the configuration can be altered"),
    )
    max_memory: Optional[str] = Field(
        None,
        description=("The maximum memory of the system (e.g., 1GB)"),
    )
    old_implicit_casting: Optional[bool] = Field(
        False,
        description=("Allow implicit casting to/from VARCHAR"),
    )
    password: Optional[str] = Field(
        None,
        description=("The password to use. Ignored for legacy compatibility."),
    )
    preserve_insertion_order: Optional[bool] = Field(
        True,
        description=(
            "Whether or not to preserve insertion order. "
            "If set to false the system is allowed to re-order any results "
            "that do not contain ORDER BY clauses."
        ),
    )
    secret_directory: Optional[str] = Field(
        "~/.duckdb/stored_secrets",
        description=("Set the directory to which persistent secrets are stored"),
    )
    temp_directory: Optional[str] = Field(
        None,
        description=("Set the directory to which to write temp files"),
    )
    threads: Optional[int] = Field(
        None,
        description=("The number of total threads used by the system."),
    )
    username: Optional[str] = Field(
        None,
        description=("The username to use. Ignored for legacy compatibility."),
    )
    # TODO: these are possibly attached to extensions
    # enable_server_cert_verification: Optional[bool] = Field(
    #     False,
    #     description="Enable server side certificate verification.",
    # )
    # allow_extensions_metadata_mismatch: Optional[bool] = Field(
    #     False,
    #     description="Allow to load extensions with not compatible metadata",
    # )
    # s3_access_key_id: Optional[str] = Field(
    #     None,
    #     description="S3 Access Key ID",
    # )
    # s3_endpoint: Optional[str] = Field(
    #     None,
    #     description="S3 Endpoint",
    # )
    # s3_region: Optional[str] = Field(
    #     "us-east-1",
    #     description="S3 Region",
    # )
    # s3_secret_access_key: Optional[str] = Field(
    #     None,
    #     description="S3 Access Key",
    # )
    # s3_session_token: Optional[str] = Field(
    #     None,
    #     description="S3 Session Token",
    # )
    # s3_uploader_max_filesize: Optional[str] = Field(
    #     "800GB",
    #     description="S3 Uploader max filesize (between 50GB and 5TB)",
    # )
    # s3_uploader_max_parts_per_file: Optional[int] = Field(
    #     10000,
    #     description="S3 Uploader max parts per file (between 1 and 10000)",
    # )
    # s3_uploader_thread_limit: Optional[int] = Field(
    #     50,
    #     description="S3 Uploader global thread limit",
    # )
    # s3_url_compatibility_mode: Optional[bool] = Field(
    #     False,
    #     description="Disable Globs and Query Parameters on S3 URLs",
    # )
    # s3_url_style: Optional[str] = Field(
    #     "vhost",
    #     description="S3 URL style",
    # )
    # s3_use_ssl: Optional[bool] = Field(
    #     True,
    #     description="S3 use SSL",
    # )
    # force_download: Optional[bool] = Field(
    #     False,
    #     description="Forces upfront download of file",
    # )
    # http_keep_alive: Optional[bool] = Field(
    #     True,
    #     description=("Keep alive connections. Setting this to false can help when "
    # "running into connection failures"),
    # )
    # http_retries: Optional[int] = Field(
    #     3,
    #     description="HTTP retries on I/O error",
    # )
    # http_retry_backoff: Optional[float] = Field(
    #     4,
    #     description="Backoff factor for exponentially increasing retry wait time",
    # )
    # http_retry_wait_ms: Optional[int] = Field(
    #     100,
    #     description="Time between retries",
    # )
    # http_timeout: Optional[int] = Field(
    #     30000,
    #     description="HTTP timeout read/write/connection/retry",
    # )
