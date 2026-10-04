import argparse
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as ds


INTEGER_COLUMNS = ["VendorID", "PULocationID", "DOLocationID", "payment_type", "pickup_hour"]
FLOAT_COLUMNS = [
    "passenger_count", "trip_distance", "fare_amount", "tip_amount",
    "total_amount", "average_speed_mph",
]
DATETIME_COLUMNS = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
STRING_COLUMNS = ["pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone"]
PARTITION_COUNT = 256


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--spark-output", default="output/spark")
    parser.add_argument("--ray-output", default="output/ray")
    parser.add_argument("--batch-size", type=int, default=250_000)
    parser.add_argument("--temp-dir", default="output")
    return parser.parse_args()


def normalize(data, columns):
    """Give equivalent Spark and Ray values the same pandas dtypes."""
    for column in INTEGER_COLUMNS:
        data[column] = data[column].astype("int64")
    for column in FLOAT_COLUMNS:
        data[column] = data[column].astype("float64")
    for column in DATETIME_COLUMNS:
        data[column] = pd.to_datetime(data[column])
    for column in STRING_COLUMNS:
        data[column] = data[column].astype("string")
    return data[columns]


def partition_hashes(output_path, destination, columns, batch_size):
    """Hash Parquet batches into small on-disk partitions and return row count."""
    dataset = ds.dataset(output_path, format="parquet")
    files = {}
    row_count = 0

    try:
        for record_batch in dataset.scanner(columns=columns, batch_size=batch_size).to_batches():
            data = normalize(record_batch.to_pandas(), columns)
            hashes = pd.util.hash_pandas_object(data, index=False).to_numpy(dtype=np.uint64)
            row_count += len(hashes)
            if len(hashes) == 0:
                continue

            # The top byte assigns each hash to one of 256 manageable files.
            partition_ids = (hashes >> np.uint64(56)).astype(np.uint8)
            order = np.argsort(partition_ids, kind="stable")
            hashes = hashes[order]
            partition_ids = partition_ids[order]
            starts = np.flatnonzero(np.r_[True, partition_ids[1:] != partition_ids[:-1]])
            ends = np.r_[starts[1:], len(hashes)]

            for start, end in zip(starts, ends):
                partition_id = int(partition_ids[start])
                if partition_id not in files:
                    path = destination / f"{partition_id:03d}.bin"
                    files[partition_id] = path.open("ab")
                hashes[start:end].tofile(files[partition_id])
    finally:
        for file in files.values():
            file.close()

    return row_count


def partitions_match(spark_dir, ray_dir):
    """Compare each small hash partition as an unordered multiset."""
    empty = np.array([], dtype=np.uint64)
    for partition_id in range(PARTITION_COUNT):
        spark_path = spark_dir / f"{partition_id:03d}.bin"
        ray_path = ray_dir / f"{partition_id:03d}.bin"
        spark_hashes = np.fromfile(spark_path, dtype=np.uint64) if spark_path.exists() else empty
        ray_hashes = np.fromfile(ray_path, dtype=np.uint64) if ray_path.exists() else empty

        if len(spark_hashes) != len(ray_hashes):
            return False
        spark_hashes.sort()
        ray_hashes.sort()
        if not np.array_equal(spark_hashes, ray_hashes):
            return False
    return True


def main():
    args = parse_args()
    spark_dataset = ds.dataset(args.spark_output, format="parquet")
    ray_dataset = ds.dataset(args.ray_output, format="parquet")
    spark_columns = sorted(spark_dataset.schema.names)
    ray_columns = sorted(ray_dataset.schema.names)
    columns_match = spark_columns == ray_columns

    temp_parent = Path(args.temp_dir)
    temp_parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="parity-", dir=temp_parent) as temp:
        temp = Path(temp)
        spark_dir = temp / "spark"
        ray_dir = temp / "ray"
        spark_dir.mkdir()
        ray_dir.mkdir()

        if columns_match:
            spark_rows = partition_hashes(
                args.spark_output, spark_dir, spark_columns, args.batch_size
            )
            ray_rows = partition_hashes(
                args.ray_output, ray_dir, spark_columns, args.batch_size
            )
            rows_match = spark_rows == ray_rows
            values_match = rows_match and partitions_match(spark_dir, ray_dir)
        else:
            spark_rows = spark_dataset.count_rows()
            ray_rows = ray_dataset.count_rows()
            rows_match = spark_rows == ray_rows
            values_match = False

    print(f"COLUMNS_MATCH={columns_match}")
    print(f"SPARK_ROWS={spark_rows}")
    print(f"RAY_ROWS={ray_rows}")
    print(f"ROW_COUNTS_MATCH={rows_match}")
    print(f"ROW_HASHES_MATCH={values_match}")
    print(f"PARITY={'PASS' if values_match else 'FAIL'}")

    if not values_match:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
