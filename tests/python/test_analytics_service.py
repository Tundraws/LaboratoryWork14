from __future__ import annotations

from datetime import datetime, timezone

from metro_analytics.repositories.parquet_repository import ParquetAggregateRepository
from metro_analytics.schemas.passenger_flow import WindowAggregate
from metro_analytics.services.analytics_service import AnalyticsService


def test_ingest_and_summary_uses_duckdb(tmp_path) -> None:
    repository = ParquetAggregateRepository(str(tmp_path / "flow.parquet"))
    service = AnalyticsService(repository)
    record = WindowAggregate(
        window_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        window_end=datetime(2026, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
        station_id="central",
        line="red",
        entries=20,
        exits=8,
        net_flow=12,
        events_count=4,
        average_per_tick=7.0,
    )

    metrics = service.ingest([record])
    summary = service.summarize()

    assert metrics["records"] == 1
    assert summary[0].total_entries > summary[0].total_exits
    assert summary[0].avg_net_flow == record.net_flow

