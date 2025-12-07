"""Configuration management for Google Drive integration worker.

Uses Pydantic settings for validation and environment variable loading.
"""

from typing import Optional, Literal
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaConfig(BaseSettings):
    """Kafka-related configuration."""

    model_config = SettingsConfigDict(env_prefix="KAFKA_")

    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Comma-separated list of Kafka brokers",
    )
    consumer_group_id: str = Field(
        default="google-drive-worker",
        description="Consumer group ID for this worker",
    )
    auto_offset_reset: Literal["earliest", "latest"] = Field(
        default="latest",
        description="Where to start consuming if no offset exists",
    )
    max_poll_records: int = Field(
        default=100,
        description="Maximum records per poll",
    )
    session_timeout_ms: int = Field(
        default=30000,
        description="Consumer session timeout in milliseconds",
    )
    heartbeat_interval_ms: int = Field(
        default=3000,
        description="Consumer heartbeat interval in milliseconds",
    )
    enable_auto_commit: bool = Field(
        default=False,
        description="Auto-commit offsets (MUST be False for manual commits)",
    )
    producer_compression_type: str = Field(
        default="snappy",
        description="Compression type for producer",
    )
    producer_acks: Literal["all", "1", "0"] = Field(
        default="all",
        description="Producer acknowledgment level",
    )
    producer_enable_idempotence: bool = Field(
        default=True,
        description="Enable idempotent producer",
    )
    producer_max_in_flight_requests_per_connection: int = Field(
        default=5,
        description="Max in-flight requests per connection",
    )

    @field_validator("enable_auto_commit")
    def validate_auto_commit(cls, v: bool) -> bool:
        """Ensure auto-commit is disabled per anti-patterns."""
        if v:
            raise ValueError("Auto-commit MUST be disabled. Use manual offset commits.")
        return v


class WorkerConfig(BaseSettings):
    """Worker-specific configuration."""

    model_config = SettingsConfigDict(env_prefix="WORKER_")

    integration_id: str = Field(
        default="google-drive",
        description="Integration ID to filter messages",
    )
    max_concurrent_connections: int = Field(
        default=10,
        description="Maximum concurrent connections to process",
    )
    processing_timeout_seconds: int = Field(
        default=300,
        description="Timeout for processing a single trigger",
    )
    retry_max_attempts: int = Field(
        default=3,
        description="Maximum retry attempts for retriable errors",
    )
    retry_backoff_base: float = Field(
        default=2.0,
        description="Base for exponential backoff calculation",
    )
    retry_backoff_max: float = Field(
        default=60.0,
        description="Maximum backoff time in seconds",
    )
    s3_payload_threshold_bytes: int = Field(
        default=256 * 1024,  # 256 KB per architecture
        description="Threshold for S3 payload offloading",
    )
    idempotency_cache_size: int = Field(
        default=10000,
        description="Size of in-memory idempotency cache",
    )
    idempotency_cache_ttl_seconds: int = Field(
        default=86400,  # 24 hours
        description="TTL for idempotency cache entries",
    )


class GoogleDriveAPIConfig(BaseSettings):
    """Google Drive API-specific configuration."""

    model_config = SettingsConfigDict(env_prefix="GDRIVE_")

    api_base_url: str = Field(
        default="https://www.googleapis.com/drive/v3",
        description="Google Drive API v3 base URL",
    )
    api_timeout_seconds: int = Field(
        default=30,
        description="API request timeout in seconds",
    )
    max_retries: int = Field(
        default=3,
        description="Maximum API retry attempts",
    )
    page_size: int = Field(
        default=100,
        description="Files per page (max 1000)",
    )
    # Fields to fetch (reduces payload size)
    file_fields: str = Field(
        default="id,name,mimeType,createdTime,modifiedTime,size,parents,owners,permissions,webViewLink,thumbnailLink",
        description="Fields to fetch for file metadata",
    )
    # Export formats for Google Workspace files
    export_docs_as: str = Field(
        default="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        description="Export format for Google Docs (.docx)",
    )
    export_sheets_as: str = Field(
        default="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        description="Export format for Google Sheets (.xlsx)",
    )
    export_slides_as: str = Field(
        default="application/vnd.openxmlformats-officedocument.presentationml.presentation",
        description="Export format for Google Slides (.pptx)",
    )
    # Sync settings
    include_shared_drives: bool = Field(
        default=True,
        description="Include shared drives in sync",
    )
    include_trashed: bool = Field(
        default=False,
        description="Include trashed files in sync",
    )
    max_revision_history: int = Field(
        default=10,
        description="Fetch last N revisions per file",
    )
    # Rate limiting
    quota_user_identifier: Optional[str] = Field(
        default=None,
        description="For quota tracking per user",
    )


class S3Config(BaseSettings):
    """S3-compatible storage configuration for large payloads."""

    model_config = SettingsConfigDict(env_prefix="S3_")

    bucket_name: str = Field(
        default="clustera-integrations",
        description="S3 bucket for payload storage",
    )
    endpoint_url: Optional[str] = Field(
        default=None,
        description="S3 endpoint URL (for non-AWS S3)",
    )
    region: Optional[str] = Field(
        default="us-east-1",
        description="AWS region",
    )
    access_key_id: Optional[str] = Field(
        default=None,
        description="AWS access key ID",
    )
    secret_access_key: Optional[str] = Field(
        default=None,
        description="AWS secret access key",
    )


class ControlPlaneConfig(BaseSettings):
    """Control Plane configuration for secrets and config fetching with M2M token authentication."""

    model_config = SettingsConfigDict(env_prefix="CONTROL_PLANE_")

    base_url: str = Field(
        default="https://control-plane.clustera.io",
        description="Control Plane base URL",
    )
    m2m_token: Optional[str] = Field(
        default=None,
        description="M2M bearer token for authentication (format: m2m_live_* or m2m_test_*)",
    )
    timeout_seconds: int = Field(
        default=10,
        description="Request timeout for Control Plane calls",
    )


class LoggingConfig(BaseSettings):
    """Structured logging configuration."""

    model_config = SettingsConfigDict(env_prefix="LOG_")

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        description="Log level",
    )
    format: Literal["json", "text"] = Field(
        default="json",
        description="Log format",
    )
    include_traceback: bool = Field(
        default=True,
        description="Include traceback in error logs",
    )


class Settings(BaseSettings):
    """Root settings container."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8"
    )

    # Environment metadata
    environment: Literal["development", "staging", "production"] = Field(
        default="development",
        description="Environment name",
    )
    service_name: str = Field(
        default="google-drive-worker",
        description="Service name for observability",
    )
    service_version: str = Field(
        default="1.0.0",
        description="Service version",
    )

    # Sub-configurations
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    worker: WorkerConfig = Field(default_factory=WorkerConfig)
    google_drive: GoogleDriveAPIConfig = Field(default_factory=GoogleDriveAPIConfig)
    s3: S3Config = Field(default_factory=S3Config)
    control_plane: ControlPlaneConfig = Field(default_factory=ControlPlaneConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)


# Global settings instance
settings = Settings()