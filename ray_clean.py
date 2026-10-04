import argparse
import glob
import time
import pandas as pd
import ray
from ray.data import ActorPoolStrategy
from ray.data.context import DataContext, ShuffleStrategy


SHUFFLE_PARTITIONS = 64
SHUFFLE_INPUT_BATCH_BYTES = 256 * 1024 * 1024
UDF_BATCH_SIZE = 50_000


INPUT_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "total_amount",
]

OUTPUT_COLUMNS = INPUT_COLUMNS + [
    "pickup_borough",
    "pickup_zone",
    "dropoff_borough",
    "dropoff_zone",
    "average_speed_mph",
    "pickup_hour",
]


def calculate_average_speed(distance_miles, pickup, dropoff):
    # Returns in miles per hour
    hours = (dropoff - pickup).total_seconds() / 3600.0
    return round(float(distance_miles) / hours, 6)


def clean_data(data: pd.DataFrame) -> pd.DataFrame:
    data = data[INPUT_COLUMNS].dropna(subset=INPUT_COLUMNS).copy()
    # The Parquet IDs are int32, while the CSV lookup infers LocationID as int64.
    # PyArrow joins require both key columns to have exactly the same type.
    data["PULocationID"] = data["PULocationID"].astype("int64")
    data["DOLocationID"] = data["DOLocationID"].astype("int64")
    data["tpep_pickup_datetime"] = pd.to_datetime(data["tpep_pickup_datetime"])
    data["tpep_dropoff_datetime"] = pd.to_datetime(data["tpep_dropoff_datetime"])
    data["duration_seconds"] = (
        data["tpep_dropoff_datetime"] - data["tpep_pickup_datetime"]
    ).dt.total_seconds()
    return data[(data["duration_seconds"] > 0) & (data["trip_distance"] > 0)]


def prepare_zones(zones, location_column, borough_column, zone_column):
    return zones.select_columns(["LocationID", "Borough", "Zone"]).rename_columns({
        "LocationID": location_column,
        "Borough": borough_column,
        "Zone": zone_column,
    })


def add_features(data: pd.DataFrame) -> pd.DataFrame:
    data = data.copy()
    data["average_speed_mph"] = [
        calculate_average_speed(distance, pickup, dropoff)
        for distance, pickup, dropoff in zip(
            data["trip_distance"],
            data["tpep_pickup_datetime"],
            data["tpep_dropoff_datetime"],
        )
    ]
    data["pickup_hour"] = data["tpep_pickup_datetime"].dt.hour
    return data[OUTPUT_COLUMNS]


class FeatureWorker:
    """Callable used by Ray to create two persistent UDF actor workers."""

    def __call__(self, data: pd.DataFrame) -> pd.DataFrame:
        return add_features(data)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trips", default="data/yellow_tripdata_*.parquet")
    parser.add_argument("--zones", default="data/taxi_zone_lookup.csv")
    parser.add_argument("--output", default="output/ray")
    return parser.parse_args()


def process_data(trips_path, zones_path, output_path):
    total_start = time.perf_counter()

    # Ingest
    trip_files = sorted(glob.glob(trips_path))
    trips = ray.data.read_parquet(trip_files).select_columns(INPUT_COLUMNS)
    input_rows = trips.count()

    # Clean
    cleaned = (
        trips.map_batches(clean_data, batch_format="pandas")
        .groupby(INPUT_COLUMNS, num_partitions=SHUFFLE_PARTITIONS)
        .count()
        .drop_columns(["count()"])
    )

    # Join pickup and drop off locations
    zones = ray.data.read_csv(zones_path)
    pickup_zones = prepare_zones(
        zones, "PULocationID", "pickup_borough", "pickup_zone"
    )
    dropoff_zones = prepare_zones(
        zones, "DOLocationID", "dropoff_borough", "dropoff_zone"
    )
    before_udf = (
        cleaned.join(
            pickup_zones,
            join_type="inner",
            num_partitions=SHUFFLE_PARTITIONS,
            on=("PULocationID",),
        )
        .join(
            dropoff_zones,
            join_type="inner",
            num_partitions=SHUFFLE_PARTITIONS,
            on=("DOLocationID",),
        )
        .materialize()
    )

    # Add features and measure the Python UDF stage
    udf_start = time.perf_counter()
    result = before_udf.map_batches(
        FeatureWorker,
        batch_format="pandas",
        batch_size=UDF_BATCH_SIZE,
        compute=ActorPoolStrategy(size=2),
        num_cpus=1,
    ).materialize()
    output_rows = result.count()
    udf_seconds = time.perf_counter() - udf_start

    # Export
    result.write_parquet(output_path, mode="overwrite")
    total_seconds = time.perf_counter() - total_start

    return input_rows, output_rows, udf_seconds, total_seconds


def main():
    args = parse_args()
    ray.init(address="auto")

    # Use smaller shuffle units and partitions that fit comfortably in memory.
    context = DataContext.get_current()
    context.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE_V2
    context.shuffle_input_batch_bytes = SHUFFLE_INPUT_BATCH_BYTES

    input_rows, output_rows, udf_seconds, total_seconds = process_data(
        args.trips, args.zones, args.output
    )

    print(f"INPUT_ROWS={input_rows}")
    print(f"OUTPUT_ROWS={output_rows}")
    print(f"UDF_TIME_SECONDS={udf_seconds:.3f}")
    print(f"TOTAL_TIME_SECONDS={total_seconds:.3f}")
    print(f"OUTPUT_PATH={args.output}")
    ray.shutdown()


if __name__ == "__main__":
    main()
