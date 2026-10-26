"""Convert a TLC Yellow Taxi parquet file into the replay CSV the service streams into Kafka.

Usage:
    python tools/prepare_replay.py ../data/raw/yellow_tripdata_2025-01.parquet data/trips_2025_01.csv.gz

Applies the same basic validity rules as the analysis notebooks: pickups inside the file's
month, known pickup zones (1-263; 264/265 are "unknown"/"outside NYC"), and non-negative
fares. Output is sorted by pickup time so the replayer can pace events realistically.
"""
import sys
from pathlib import Path

import pandas as pd

COLUMNS = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "trip_distance": "trip_distance",
    "fare_amount": "fare_amount",
    "total_amount": "total_amount",
}


def main(src: str, dst: str) -> None:
    df = pd.read_parquet(src, columns=list(COLUMNS)).rename(columns=COLUMNS)
    raw = len(df)

    month_start = df["pickup_datetime"].median().to_period("M").to_timestamp()
    month_end = month_start + pd.offsets.MonthBegin(1)
    df = df[
        (df["pickup_datetime"] >= month_start)
        & (df["pickup_datetime"] < month_end)
        & df["pu_location_id"].between(1, 263)
        & df["do_location_id"].between(1, 265)
        & (df["fare_amount"] >= 0)
    ]
    df = df.sort_values("pickup_datetime", kind="stable")

    df["vendor_id"] = df["vendor_id"].astype("Int64")
    for col in ("pickup_datetime", "dropoff_datetime"):
        df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(dst, index=False, float_format="%.2f")
    print(f"{raw:,} raw trips -> {len(df):,} valid trips written to {dst}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
