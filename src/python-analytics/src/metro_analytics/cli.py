from __future__ import annotations

import argparse
import asyncio

from metro_analytics.api.app import _build_state
from metro_analytics.core.settings import Settings


async def run_once(settings: Settings) -> None:
    """Fetch Arrow data once, store it in Parquet, and print a station summary."""
    state = _build_state(settings)
    records = await state.arrow_client.fetch()
    metrics = state.analytics.ingest(records)
    print(f"Импортировано записей: {metrics['records']} за {metrics['elapsed_ms']} мс")
    for row in state.analytics.summarize():
        print(
            f"{row.station_id} ({row.line}): входы={row.total_entries}, "
            f"выходы={row.total_exits}, средний net_flow={row.avg_net_flow:.2f}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Metro passenger-flow Arrow importer")
    parser.add_argument("--arrow-url", default=None)
    parser.add_argument("--parquet-path", default=None)
    args = parser.parse_args()

    settings = Settings(
        **{
            key: value
            for key, value in {
                "arrow_url": args.arrow_url,
                "parquet_path": args.parquet_path,
            }.items()
            if value is not None
        }
    )
    asyncio.run(run_once(settings))


if __name__ == "__main__":
    main()

