import argparse
import time
from pyspark.sql import SparkSession, functions as F, types as T


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


def clean_data(data):
    return (
        data.dropna(subset=INPUT_COLUMNS)
        .dropDuplicates(INPUT_COLUMNS)
        .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp"))
        .withColumn(
            "duration_seconds",
            F.col("tpep_dropoff_datetime").cast("long")
            - F.col("tpep_pickup_datetime").cast("long"),
        )
        .filter((F.col("duration_seconds") > 0) & (F.col("trip_distance") > 0))
    )


def prepare_zones(zones, location_column, borough_column, zone_column):
    return zones.select(
        F.col("LocationID").alias(location_column),
        F.col("Borough").alias(borough_column),
        F.col("Zone").alias(zone_column),
    )


def add_features(data):
    speed_udf = F.udf(calculate_average_speed, T.DoubleType())
    return (
        data.withColumn(
            "average_speed_mph",
            speed_udf("trip_distance", "tpep_pickup_datetime", "tpep_dropoff_datetime"),
        )
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .select(*OUTPUT_COLUMNS)
    )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trips", default="data/yellow_tripdata_*.parquet")
    parser.add_argument("--zones", default="data/taxi_zone_lookup.csv")
    parser.add_argument("--output", default="output/spark")
    return parser.parse_args()


def process_data(spark, trips_path, zones_path, output_path):
    total_start = time.perf_counter()

    # Ingest
    trips = spark.read.parquet(trips_path).select(*INPUT_COLUMNS)
    input_rows = trips.count()

    # Clean
    cleaned = clean_data(trips)

    # Join pickup and drop off locations
    zones = spark.read.option("header", True).option("inferSchema", True).csv(zones_path)
    pickup_zones = prepare_zones(
        zones, "PULocationID", "pickup_borough", "pickup_zone"
    )
    dropoff_zones = prepare_zones(
        zones, "DOLocationID", "dropoff_borough", "dropoff_zone"
    )
    before_udf = (
        cleaned.join(pickup_zones, "PULocationID", "inner")
        .join(dropoff_zones, "DOLocationID", "inner")
        .cache()
    )
    before_udf.count()

    # Add features and measure the Python UDF stage
    udf_start = time.perf_counter()
    result = add_features(before_udf).cache()
    output_rows = result.count()
    udf_seconds = time.perf_counter() - udf_start

    # Export
    result.write.mode("overwrite").parquet(output_path)
    total_seconds = time.perf_counter() - total_start

    return input_rows, output_rows, udf_seconds, total_seconds


def main():
    args = parse_args()
    spark = (
        SparkSession.builder.appName("AIOps-A3-Spark-Clean")
        # Taxi timestamps are timezone-naive local values. UTC prevents Spark from
        # shifting them according to the laptop's Asia/Kolkata timezone.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    input_rows, output_rows, udf_seconds, total_seconds = process_data(
        spark, args.trips, args.zones, args.output
    )

    print(f"INPUT_ROWS={input_rows}")
    print(f"OUTPUT_ROWS={output_rows}")
    print(f"UDF_TIME_SECONDS={udf_seconds:.3f}")
    print(f"TOTAL_TIME_SECONDS={total_seconds:.3f}")
    print(f"OUTPUT_PATH={args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
