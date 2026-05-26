from __future__ import annotations

from pathlib import Path

import plotly.express as px
import polars as pl

from metro_analytics.schemas.passenger_flow import WindowAggregate


class PlotlyVisualizer:
    """Creates two required visualizations for the passenger-flow pipeline."""

    def __init__(self, output_dir: str) -> None:
        self._output_dir = Path(output_dir)

    def render(self, records: list[WindowAggregate]) -> list[Path]:
        """Render a time series and station balance chart as HTML files."""
        self._output_dir.mkdir(parents=True, exist_ok=True)
        if not records:
            return []
        frame = pl.DataFrame([record.model_dump(mode="json") for record in records])
        paths = [
            self._render_time_series(frame),
            self._render_station_balance(frame),
        ]
        return paths

    def _render_time_series(self, frame: pl.DataFrame) -> Path:
        grouped = (
            frame.group_by(["window_start", "station_id"])
            .agg(pl.sum("entries").alias("entries"))
            .sort("window_start")
        )
        figure = px.line(
            grouped.to_pandas(),
            x="window_start",
            y="entries",
            color="station_id",
            title="Passenger entries by station",
        )
        path = self._output_dir / "entries_time_series.html"
        figure.write_html(path)
        return path

    def _render_station_balance(self, frame: pl.DataFrame) -> Path:
        grouped = frame.group_by("station_id").agg(
            pl.sum("entries").alias("entries"),
            pl.sum("exits").alias("exits"),
        )
        figure = px.bar(
            grouped.to_pandas(),
            x="station_id",
            y=["entries", "exits"],
            barmode="group",
            title="Entries and exits by station",
        )
        path = self._output_dir / "station_balance.html"
        figure.write_html(path)
        return path

