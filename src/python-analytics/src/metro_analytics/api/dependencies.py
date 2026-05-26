from __future__ import annotations

from dataclasses import dataclass, field

from metro_analytics.schemas.passenger_flow import WindowAggregate
from metro_analytics.services.analytics_service import AnalyticsService
from metro_analytics.services.arrow_client import ArrowAggregateClient
from metro_analytics.services.nats_consumer import NATSWindowConsumer


@dataclass
class DashboardBuffer:
    """Small in-memory buffer for realtime dashboard clients."""

    limit: int
    records: list[WindowAggregate] = field(default_factory=list)

    def extend(self, records: list[WindowAggregate]) -> None:
        self.records.extend(records)
        if len(self.records) > self.limit:
            self.records = self.records[-self.limit :]


@dataclass
class AppState:
    analytics: AnalyticsService
    arrow_client: ArrowAggregateClient
    dashboard: DashboardBuffer
    nats_consumer: NATSWindowConsumer | None = None
