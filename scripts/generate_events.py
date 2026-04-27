#!/usr/bin/env python3
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
RECORDS_PER_PARTITION = 1000
PARTITIONS = ["2026-04-27", "2026-04-28", "2026-05-01"]

BASE_COLUMNS = [
    "event_id",
    "customer_name",
    "amount",
    "partition_date",
    "partition_year",
    "partition_month",
    "partition_day",
]

EVOLVED_COLUMNS = BASE_COLUMNS + ["event_category", "source_system"]


def partition_parts(partition_date: str) -> tuple[str, str, str]:
    return partition_date[0:4], partition_date[5:7], partition_date[8:10]


def row(event_id: int, partition_date: str, evolved: bool) -> dict[str, str]:
    year, month, day = partition_parts(partition_date)
    data = {
        "event_id": str(event_id),
        "customer_name": f"customer-{event_id:05d}",
        "amount": f"{(event_id % 997) * 1.17:.2f}",
        "partition_date": partition_date,
        "partition_year": year,
        "partition_month": month,
        "partition_day": day,
    }
    if evolved:
        data["event_category"] = ["purchase", "refund", "adjustment"][event_id % 3]
        data["source_system"] = ["web", "mobile", "batch"][event_id % 3]
    return data


def write_csv(path: Path, columns: list[str], start_id: int, evolved: bool) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        event_id = start_id
        for partition_date in PARTITIONS:
            for _ in range(RECORDS_PER_PARTITION):
                writer.writerow(row(event_id, partition_date, evolved))
                event_id += 1


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    write_csv(DATA_DIR / "events_base.csv", BASE_COLUMNS, 1, evolved=False)
    write_csv(DATA_DIR / "events_evolved.csv", EVOLVED_COLUMNS, 100_001, evolved=True)
    print(f"generated CSV files in {DATA_DIR}")


if __name__ == "__main__":
    main()
