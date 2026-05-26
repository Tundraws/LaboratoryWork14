from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

import aiohttp
import psutil


@dataclass(frozen=True)
class PassengerEvent:
    sensor_id: str
    station_id: str
    line: str
    event_type: str
    passengers: int
    timestamp: str


SENSORS = (
    ("S-001", "central", "red"),
    ("S-002", "park", "green"),
    ("S-003", "river", "blue"),
)


async def produce_events(events: int, workers: int) -> list[PassengerEvent]:
    """Generate passenger events using asyncio workers for Go/Python comparison."""
    queue: asyncio.Queue[int] = asyncio.Queue()
    for index in range(events):
        queue.put_nowait(index)
    results: list[PassengerEvent] = []
    lock = asyncio.Lock()

    async def worker(seed: int) -> None:
        randomizer = random.Random(seed)
        while not queue.empty():
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            sensor_id, station_id, line = randomizer.choice(SENSORS)
            event = PassengerEvent(
                sensor_id=sensor_id,
                station_id=station_id,
                line=line,
                event_type="entry" if randomizer.random() > 0.45 else "exit",
                passengers=randomizer.randint(1, 20),
                timestamp=datetime.now(UTC).isoformat(),
            )
            async with lock:
                results.append(event)
            queue.task_done()
            await asyncio.sleep(0)

    await asyncio.gather(*(worker(seed) for seed in range(workers)))
    return results


async def post_events(url: str, events: list[PassengerEvent]) -> None:
    """Optionally post generated events to a compatible HTTP endpoint."""
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        async with session.post(url, json=[asdict(event) for event in events]) as response:
            response.raise_for_status()


async def run(args: argparse.Namespace) -> dict[str, float | int]:
    process = psutil.Process()
    started = time.perf_counter()
    events = await produce_events(args.events, args.workers)
    if args.post_url:
        await post_events(args.post_url, events)
    elapsed = time.perf_counter() - started
    return {
        "events": len(events),
        "workers": args.workers,
        "elapsed_seconds": round(elapsed, 4),
        "events_per_second": round(len(events) / elapsed, 2),
        "rss_mb": round(process.memory_info().rss / 1024 / 1024, 2),
        "cpu_percent": process.cpu_percent(interval=0.1),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Async Python collector benchmark")
    parser.add_argument("--events", type=int, default=10_000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--post-url", default=None)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run(args)), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

