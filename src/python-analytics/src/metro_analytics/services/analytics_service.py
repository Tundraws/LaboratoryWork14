from __future__ import annotations

import time

from metro_analytics.repositories.parquet_repository import ParquetAggregateRepository
from metro_analytics.schemas.passenger_flow import StationSummary, WindowAggregate


class AnalyticsService:
    """Coordinates persistence and aggregate analysis."""

    def __init__(self, repository: ParquetAggregateRepository) -> None:
        self._repository = repository

    def ingest(self, records: list[WindowAggregate]) -> dict[str, float | int]:
        """Persist records and return a small performance measurement."""
        started = time.perf_counter()
        if records:
            self._repository.save(records)
        elapsed_ms = (time.perf_counter() - started) * 1000
        return {"records": len(records), "elapsed_ms": round(elapsed_ms, 3)}

    def summarize(self) -> list[StationSummary]:
        """Return DuckDB station summaries from the current Parquet file."""
        return self._repository.summarize()

