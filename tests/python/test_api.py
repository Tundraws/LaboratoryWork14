from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from metro_analytics.api.app import create_app
from metro_analytics.api.dependencies import AppState, DashboardBuffer
from metro_analytics.repositories.parquet_repository import ParquetAggregateRepository
from metro_analytics.schemas.passenger_flow import WindowAggregate
from metro_analytics.services.analytics_service import AnalyticsService


@dataclass
class FakeArrowClient:
    records: list[WindowAggregate]

    async def fetch(self) -> list[WindowAggregate]:
        return self.records


def test_health_endpoint_returns_ok(tmp_path) -> None:
    app = create_app(state=build_state(tmp_path))
    client = TestClient(app)

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ingest_from_arrow_updates_dashboard(tmp_path) -> None:
    app = create_app(state=build_state(tmp_path))
    client = TestClient(app)

    response = client.post("/aggregates/from-arrow")
    dashboard = client.get("/dashboard/state")

    assert response.status_code == 200
    assert response.json()["records"] == 1
    assert len(dashboard.json()["records"]) == 1


def build_state(tmp_path) -> AppState:
    record = WindowAggregate(
        window_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        window_end=datetime(2026, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
        station_id="central",
        line="red",
        entries=10,
        exits=4,
        net_flow=6,
        events_count=2,
        average_per_tick=7.0,
    )
    repository = ParquetAggregateRepository(str(tmp_path / "flow.parquet"))
    return AppState(
        analytics=AnalyticsService(repository),
        arrow_client=FakeArrowClient([record]),  # type: ignore[arg-type]
        dashboard=DashboardBuffer(limit=10),
    )

