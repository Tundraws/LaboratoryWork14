from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable

import nats

from metro_analytics.schemas.passenger_flow import WindowAggregate
from metro_analytics.services.validation import AggregateValidator

AggregateHandler = Callable[[list[WindowAggregate]], Awaitable[None]]


class NATSWindowConsumer:
    """Consumes realtime aggregate batches from the Go collector."""

    def __init__(
        self,
        url: str,
        subject: str,
        validator: AggregateValidator,
        handler: AggregateHandler,
    ) -> None:
        self._url = url
        self._subject = subject
        self._validator = validator
        self._handler = handler
        self._connection: nats.NATS | None = None
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        """Run until `stop()` is called or the task is cancelled."""
        self._connection = await nats.connect(self._url)

        async def on_message(message: nats.aio.msg.Msg) -> None:
            payload = json.loads(message.data.decode("utf-8"))
            records = [self._validator.validate(item) for item in payload]
            await self._handler(records)

        await self._connection.subscribe(self._subject, cb=on_message)
        await self._stop_event.wait()

    async def stop(self) -> None:
        """Close the NATS connection gracefully."""
        self._stop_event.set()
        if self._connection is not None:
            await self._connection.drain()

