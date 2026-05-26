from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import Depends, FastAPI, WebSocket, WebSocketDisconnect

from metro_analytics.api.dependencies import AppState, DashboardBuffer
from metro_analytics.core.settings import Settings
from metro_analytics.repositories.parquet_repository import ParquetAggregateRepository
from metro_analytics.schemas.passenger_flow import DashboardState, StationSummary, WindowAggregate
from metro_analytics.services.analytics_service import AnalyticsService
from metro_analytics.services.arrow_client import ArrowAggregateClient
from metro_analytics.services.validation import build_validator


def create_app(settings: Settings | None = None, state: AppState | None = None) -> FastAPI:
    """Build the FastAPI app without module-level mutable state."""
    resolved_settings = settings or Settings()
    resolved_state = state or _build_state(resolved_settings)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield

    app = FastAPI(
        title="Metro Passenger Flow Analytics",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.container = resolved_state

    def get_state() -> AppState:
        return app.state.container

    @app.get("/healthz")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/aggregates/from-arrow")
    async def ingest_from_arrow(container: AppState = Depends(get_state)) -> dict[str, float | int]:
        records = await container.arrow_client.fetch()
        container.dashboard.extend(records)
        return container.analytics.ingest(records)

    @app.post("/aggregates")
    async def ingest_records(
        records: list[WindowAggregate],
        container: AppState = Depends(get_state),
    ) -> dict[str, float | int]:
        container.dashboard.extend(records)
        return container.analytics.ingest(records)

    @app.get("/summary")
    async def summary(container: AppState = Depends(get_state)) -> list[StationSummary]:
        return container.analytics.summarize()

    @app.get("/dashboard/state")
    async def dashboard_state(container: AppState = Depends(get_state)) -> DashboardState:
        return DashboardState(
            records=container.dashboard.records,
            summaries=container.analytics.summarize(),
        )

    @app.websocket("/ws/aggregates")
    async def aggregate_socket(websocket: WebSocket) -> None:
        await websocket.accept()
        try:
            while True:
                await websocket.receive_text()
                container = get_state()
                await websocket.send_json(
                    DashboardState(
                        records=container.dashboard.records,
                        summaries=container.analytics.summarize(),
                    ).model_dump(mode="json")
                )
        except WebSocketDisconnect:
            return

    return app


def _build_state(settings: Settings) -> AppState:
    validator = build_validator()
    repository = ParquetAggregateRepository(settings.parquet_path)
    analytics = AnalyticsService(repository)
    arrow_client = ArrowAggregateClient(
        arrow_url=str(settings.arrow_url),
        timeout_seconds=settings.request_timeout_seconds,
        validator=validator,
    )
    return AppState(
        analytics=analytics,
        arrow_client=arrow_client,
        dashboard=DashboardBuffer(limit=settings.dashboard_history_limit),
    )

