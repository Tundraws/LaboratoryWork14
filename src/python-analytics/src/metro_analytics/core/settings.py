from __future__ import annotations

from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded once by the app factory."""

    model_config = SettingsConfigDict(env_prefix="ANALYTICS_", env_file=".env", extra="ignore")

    arrow_url: HttpUrl = Field(default="http://localhost:8080/arrow")
    nats_url: str = Field(default="nats://localhost:4222")
    nats_subject: str = Field(default="metro.passenger.windows", min_length=1)
    parquet_path: str = Field(default="data/passenger_flow.parquet", min_length=1)
    request_timeout_seconds: float = Field(default=10.0, gt=0, le=60)
    dashboard_history_limit: int = Field(default=500, ge=10, le=10_000)

