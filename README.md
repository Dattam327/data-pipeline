# Telemetry Pipeline

![tests](https://github.com/dattam327/telemetry-pipeline/actions/workflows/tests.yml/badge.svg)

A streaming ETL pipeline that reads IoT device telemetry from Kafka, validates it with PySpark Structured Streaming, and writes parquet files partitioned by date and device.

Records that fail validation are not thrown away. They go to a dead letter queue with a reason attached so you can see what went wrong.

```
producer.py  ->  Kafka topic 'telemetry'  ->  Spark Streaming
                                                    |
                                                    +-- valid   -> /clean (partitioned by event_date, device_id)
                                                    +-- invalid -> /dead_letter (with reject_reason)
```

## What each piece does

**`pipeline/producer.py`**
Simulates 10 devices (`device-001` through `device-010`) sending readings. Each record has an event id, a UTC timestamp, a device id, a status (`OK`, `WARN`, `FAULT`), and a temperature in Celsius.

About 10% of the messages are broken on purpose. Some are missing a required field, some have temperatures below absolute zero, and a few are not valid JSON at all. This is so the dead letter queue actually has something to catch.

**`pipeline/stream_job.py`**
Reads the topic, parses the JSON against a fixed schema, and labels anything that fails a check:

| reject_reason | what happened |
| --- | --- |
| `unparseable_json` | the message was not valid JSON |
| `missing_event_id` | no event id |
| `missing_device_id` | no device id |
| `bad_timestamp` | the timestamp would not parse |
| `impossible_temperature` | below -273.15 C |

Everything that passes gets a status of `UNKNOWN` if the status field was empty, plus an `event_date` column used for partitioning. Both outputs write parquet on a 30 second trigger with checkpointing, so restarting the job picks up where it left off.

## Running it

You need Python 3.10 or newer, Java 11 or 17 for Spark, and Docker for the local Kafka broker.

**1. Start Kafka**

```bash
docker compose up -d
```

**2. Set up Python**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**3. Run the pipeline**

Output goes to `/tmp/telemetry` by default. In one terminal:

```bash
python pipeline/stream_job.py
```

In another terminal:

```bash
python pipeline/producer.py
```

The first run downloads the Spark Kafka connector, so give it a minute.

**4. Check that it worked**

Wait about a minute, then look at what landed:

```bash
find /tmp/telemetry/clean -name "*.parquet" | head
```

You should see folders like `event_date=2026-07-23/device_id=device-004/`. To see the rejects and why they were rejected:

```python
import pandas as pd, glob
files = glob.glob("/tmp/telemetry/dead_letter/*.parquet")
df = pd.concat([pd.read_parquet(f) for f in files])
print(df["reject_reason"].value_counts())
```

**5. Run the tests**

```bash
pytest tests/ -v
```

## Writing to S3 instead

Set the paths to `s3a://` URLs and add the Hadoop AWS jar. The version has to match the Hadoop version Spark was built against, which is 3.3.4 for Spark 3.5.1:

```bash
export CLEAN_PATH=s3a://your-bucket/clean
export DLQ_PATH=s3a://your-bucket/dead_letter
export CHECKPOINT_PATH=s3a://your-bucket/_chk
```

Then add `org.apache.hadoop:hadoop-aws:3.3.4` to the `spark.jars.packages` list in `stream_job.py` and make sure your AWS credentials are available.

## Things I would change

**Duplicates are possible.** Checkpointing means no records get lost when the job restarts, but a record can be written twice. Whatever reads the parquet needs to handle that. Delta Lake or Iceberg would fix it properly.

**One Kafka broker.** The Docker setup runs a single broker with no replication, which is fine for testing and wrong for anything real.

**Lots of small files.** Partitioning by date and device with a 30 second trigger means 10 devices times 120 batches an hour, so over a thousand files per hour. A compaction job would clean that up.

**No load testing.** I ran this at 5 events per second on a laptop. I have no idea where it breaks.
