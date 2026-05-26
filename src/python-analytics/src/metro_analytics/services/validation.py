from __future__ import annotations

from importlib import import_module
from typing import Protocol

from metro_analytics.schemas.passenger_flow import WindowAggregate


class AggregateValidator(Protocol):
    """Validation contract used by transport and analytics services."""

    def validate(self, payload: dict[str, object]) -> WindowAggregate:
        """Validate a raw aggregate payload."""


class PydanticAggregateValidator:
    """Fallback validator used when the Rust extension is not installed."""

    def validate(self, payload: dict[str, object]) -> WindowAggregate:
        """Validate aggregate data with Pydantic constraints."""
        aggregate = WindowAggregate.model_validate(payload)
        expected_net_flow = aggregate.entries - aggregate.exits
        if aggregate.net_flow != expected_net_flow:
            msg = "net_flow must equal entries minus exits"
            raise ValueError(msg)
        return aggregate


class RustBackedAggregateValidator:
    """Validator that delegates range checks to the PyO3 Rust extension."""

    def __init__(self) -> None:
        self._module = import_module("metro_validator")
        self._fallback = PydanticAggregateValidator()

    def validate(self, payload: dict[str, object]) -> WindowAggregate:
        """Run Rust validation first, then materialize a typed Python schema."""
        self._module.validate_window_aggregate(payload)
        return self._fallback.validate(payload)


def build_validator(prefer_rust: bool = True) -> AggregateValidator:
    """Create the strongest available validator without making Rust mandatory in tests."""
    if prefer_rust:
        try:
            return RustBackedAggregateValidator()
        except (ImportError, ModuleNotFoundError):
            return PydanticAggregateValidator()
    return PydanticAggregateValidator()

