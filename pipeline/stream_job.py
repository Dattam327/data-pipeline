"""
Reads telemetry JSON off Kafka, checks it, and writes parquet.
Good records go to CLEAN_PATH, bad ones go to DLQ_PATH with a reason.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "telemetry")
CLEAN_PATH = os.getenv("CLEAN_PATH", "file:///tmp/telemetry/clean")
DLQ_PATH = os.getenv("DLQ_PATH", "file:///tmp/telemetry/dead_letter")
CHECKPOINT = os.getenv("CHECKPOINT_PATH", "file:///tmp/telemetry/_chk")

ABSOLUTE_ZERO_C = -273.15

# timestamp is read as a string first, then converted.
# from_json didn't like the ISO timestamps when I used TimestampType here.
schema = StructType([
    StructField("event_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("device_id", StringType()),
    StructField("status", StringType()),
    StructField("temperature", DoubleType()),
])


def parse(raw):
    """Turn the raw kafka value into columns we can work with."""
    df = raw.selectExpr("CAST(value AS STRING) as json_str", "partition", "offset")
    df = df.withColumn("data", F.from_json("json_str", schema))
    df = df.withColumn("event_ts", F.to_timestamp(F.col("data.timestamp")))
    return df


def add_reject_reason(df):
    """Label anything that fails a check. Null reason means the record is fine."""
    reason = (
        F.when(F.col("data").isNull(), "unparseable_json")
        .when(F.col("data.event_id").isNull(), "missing_event_id")
        .when(F.col("data.device_id").isNull(), "missing_device_id")
        .when(F.col("event_ts").isNull(), "bad_timestamp")
        # a missing temperature is fine, a physically impossible one is not
        .when(F.col("data.temperature") < ABSOLUTE_ZERO_C, "impossible_temperature")
    )
    return df.withColumn("reject_reason", reason)


def clean(df):
    """Pick out the columns we want and fill in a default status."""
    out = df.select(
        F.col("data.event_id").alias("event_id"),
        F.col("event_ts").alias("timestamp"),
        F.col("data.device_id").alias("device_id"),
        F.coalesce(F.col("data.status"), F.lit("UNKNOWN")).alias("status"),
        F.col("data.temperature").alias("temperature"),
    )
    out = out.withColumn("event_date", F.to_date("timestamp"))
    return out


def main():
    spark = (
        SparkSession.builder
        .appName("telemetry-etl")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        # keep dates in UTC so partitions don't change on a different machine
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        # without this the first batch tries to read the entire backlog at once
        .option("maxOffsetsPerTrigger", 10000)
        .load()
    )

    tagged = add_reject_reason(parse(raw))

    good = tagged.filter(F.col("reject_reason").isNull())
    bad = tagged.filter(F.col("reject_reason").isNotNull()) \
                .select("json_str", "reject_reason", "partition", "offset")

    clean_query = (
        clean(good).writeStream
        .format("parquet")
        .option("path", CLEAN_PATH)
        .option("checkpointLocation", CHECKPOINT + "/clean")
        .partitionBy("event_date", "device_id")
        .trigger(processingTime="30 seconds")
        .outputMode("append")
        .start()
    )

    dlq_query = (
        bad.writeStream
        .format("parquet")
        .option("path", DLQ_PATH)
        .option("checkpointLocation", CHECKPOINT + "/dlq")
        .trigger(processingTime="30 seconds")
        .outputMode("append")
        .start()
    )

    print("clean ->", CLEAN_PATH)
    print("dlq   ->", DLQ_PATH)
    print("ctrl+c to stop")

    # If one of the two queries dies, stop the other one instead of leaving
    # half the pipeline running. Learned this the hard way when the DLQ writer
    # failed and the job looked healthy for 20 minutes.
    try:
        while clean_query.isActive and dlq_query.isActive:
            spark.streams.awaitAnyTermination(timeout=10)
            spark.streams.resetTerminated()
    except KeyboardInterrupt:
        print("\nshutting down")
    finally:
        for query in [clean_query, dlq_query]:
            if query.exception() is not None:
                print("query failed:", query.exception())
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
