import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.stream_job import parse, add_reject_reason, clean


@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
            .master("local[2]")
            .appName("tests")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


def fake_kafka_df(spark, messages):
    """Build a dataframe that looks like what we get from Kafka."""
    rows = []
    for i, text in enumerate(messages):
        rows.append((text.encode(), 0, i))
    return spark.createDataFrame(rows, ["value", "partition", "offset"])


def run_checks(spark, messages):
    return add_reject_reason(parse(fake_kafka_df(spark, messages)))


GOOD = ('{"event_id":"a","timestamp":"2026-01-01T00:00:00+00:00",'
        '"device_id":"d1","temperature":20.0}')


def test_good_record_passes(spark):
    df = run_checks(spark, [GOOD])
    assert df.collect()[0].reject_reason is None


def test_missing_device_id(spark):
    bad = '{"event_id":"b","timestamp":"2026-01-01T00:00:00+00:00","temperature":20.0}'
    df = run_checks(spark, [bad])
    assert df.collect()[0].reject_reason == "missing_device_id"


def test_garbage_json(spark):
    df = run_checks(spark, ["not json at all"])
    assert df.collect()[0].reject_reason == "unparseable_json"


def test_temperature_below_absolute_zero(spark):
    bad = ('{"event_id":"a","timestamp":"2026-01-01T00:00:00+00:00",'
           '"device_id":"d1","temperature":-400.0}')
    df = run_checks(spark, [bad])
    assert df.collect()[0].reject_reason == "impossible_temperature"


def test_missing_status_becomes_unknown(spark):
    df = run_checks(spark, [GOOD])
    good = df.filter(F.col("reject_reason").isNull())
    assert clean(good).collect()[0].status == "UNKNOWN"


def test_missing_temperature_is_ok(spark):
    text = ('{"event_id":"a","timestamp":"2026-01-01T00:00:00+00:00",'
            '"device_id":"d1"}')
    df = run_checks(spark, [text])
    assert df.collect()[0].reject_reason is None


def test_event_date_is_added(spark):
    df = run_checks(spark, [GOOD])
    good = df.filter(F.col("reject_reason").isNull())
    assert str(clean(good).collect()[0].event_date) == "2026-01-01"
