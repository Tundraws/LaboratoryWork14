from __future__ import annotations

from datetime import datetime, timezone

import pytest

from metro_analytics.services.validation import PydanticAggregateValidator


def test_validator_rejects_inconsistent_net_flow() -> None:
    payload = {
        "window_start": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "window_end": datetime(2026, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
        "station_id": "central",
        "line": "red",
        "entries": 10,
        "exits": 2,
        "net_flow": 99,
        "events_count": 2,
        "average_per_tick": 6.0,
    }

    with pytest.raises(ValueError, match="net_flow"):
        PydanticAggregateValidator().validate(payload)


def test_validator_rejects_naive_datetime() -> None:
    payload = {
        "window_start": datetime(2026, 1, 1),
        "window_end": datetime(2026, 1, 1, 0, 0, 10),
        "station_id": "central",
        "line": "red",
        "entries": 10,
        "exits": 2,
        "net_flow": 8,
        "events_count": 2,
        "average_per_tick": 6.0,
    }

    with pytest.raises(ValueError, match="timezone-aware"):
        PydanticAggregateValidator().validate(payload)

