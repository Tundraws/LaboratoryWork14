from __future__ import annotations

from io import BytesIO

import httpx
import pyarrow.ipc as ipc

from metro_analytics.schemas.passenger_flow import WindowAggregate
from metro_analytics.services.validation import AggregateValidator


class ArrowAggregateClient:
    """Reads Arrow RecordBatch streams produced by the Go collector."""

    def __init__(
        self,
        arrow_url: str,
        timeout_seconds: float,
        validator: AggregateValidator,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._arrow_url = arrow_url
        self._timeout_seconds = timeout_seconds
        self._validator = validator
        self._client = client

    async def fetch(self) -> list[WindowAggregate]:
        """Fetch and validate current aggregate snapshots."""
        close_client = self._client is None
        client = self._client or httpx.AsyncClient(timeout=self._timeout_seconds)
        try:
            response = await client.get(self._arrow_url)
            response.raise_for_status()
            return self._decode(response.content)
        finally:
            if close_client:
                await client.aclose()

    def _decode(self, content: bytes) -> list[WindowAggregate]:
        with ipc.open_stream(BytesIO(content)) as reader:
            table = reader.read_all()
        payloads = table.to_pylist()
        return [self._validator.validate(payload) for payload in payloads]

