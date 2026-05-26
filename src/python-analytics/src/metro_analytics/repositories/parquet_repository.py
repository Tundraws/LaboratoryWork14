from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

from metro_analytics.schemas.passenger_flow import StationSummary, WindowAggregate


class ParquetAggregateRepository:
    """Persists validated aggregates and exposes analytical SQL queries."""

    def __init__(self, parquet_path: str) -> None:
        self._path = Path(parquet_path)

    @property
    def path(self) -> Path:
        return self._path

    def save(self, records: list[WindowAggregate]) -> None:
        """Append records to a Parquet dataset represented by one compact file."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        new_frame = pl.DataFrame([record.model_dump(mode="json") for record in records])
        if self._path.exists():
            existing = pl.read_parquet(self._path)
            new_frame = pl.concat([existing, new_frame], how="vertical_relaxed")
        cleaned = (
            new_frame.unique()
            .with_columns(
                pl.col("window_start").str.to_datetime(strict=False, time_zone="UTC"),
                pl.col("window_end").str.to_datetime(strict=False, time_zone="UTC"),
            )
            .sort(["window_start", "station_id"])
        )
        cleaned.write_parquet(self._path)

    def summarize(self) -> list[StationSummary]:
        """Run a DuckDB analytical query over Parquet data."""
        if not self._path.exists():
            return []
        query = """
            SELECT
                station_id,
                line,
                SUM(entries)::BIGINT AS total_entries,
                SUM(exits)::BIGINT AS total_exits,
                MAX(events_count)::BIGINT AS max_events,
                AVG(net_flow)::DOUBLE AS avg_net_flow
            FROM read_parquet(?)
            GROUP BY station_id, line
            ORDER BY total_entries DESC
        """
        rows = duckdb.connect(database=":memory:").execute(query, [str(self._path)]).fetchall()
        return [
            StationSummary(
                station_id=row[0],
                line=row[1],
                total_entries=row[2],
                total_exits=row[3],
                max_events=row[4],
                avg_net_flow=row[5],
            )
            for row in rows
        ]
