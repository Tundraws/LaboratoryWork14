from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator


class WindowAggregate(BaseModel):
    """Validated aggregate produced by the Go collector."""

    model_config = ConfigDict(str_strip_whitespace=True)

    window_start: datetime
    window_end: datetime
    station_id: Annotated[str, Field(min_length=1, max_length=64)]
    line: Annotated[str, Field(min_length=1, max_length=32)]
    entries: Annotated[int, Field(ge=0)]
    exits: Annotated[int, Field(ge=0)]
    net_flow: int
    events_count: Annotated[int, Field(ge=0)]
    average_per_tick: Annotated[float, Field(ge=0)]

    @field_validator("window_start", "window_end")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        """Reject naive timestamps so DuckDB and Polars compare windows consistently."""
        if value.tzinfo is None or value.utcoffset() is None:
            msg = "timestamp must be timezone-aware"
            raise ValueError(msg)
        return value

    @field_validator("net_flow")
    @classmethod
    def net_flow_can_be_negative(cls, value: int) -> int:
        """Keep validation explicit because negative net flow is a valid station state."""
        return value


class StationSummary(BaseModel):
    station_id: str
    line: str
    total_entries: int
    total_exits: int
    max_events: int
    avg_net_flow: float


class DashboardState(BaseModel):
    records: list[WindowAggregate]
    summaries: list[StationSummary]

