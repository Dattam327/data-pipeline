"""
Fake IoT devices sending readings to Kafka.
On purpose, some of the messages are broken so the DLQ has something to catch.
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "telemetry")
RATE = float(os.getenv("EVENTS_PER_SEC", "5"))

devices = ["device-%03d" % i for i in range(1, 11)]


def make_record():
    record = {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device_id": random.choice(devices),
        "status": random.choice(["OK", "OK", "OK", "WARN", "FAULT", None]),
        "temperature": round(random.uniform(-300, 120), 2),
    }

    # about 10% of the time, drop a field the pipeline needs
    if random.random() < 0.1:
        field = random.choice(["event_id", "device_id", "timestamp"])
        del record[field]

    return record


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks="all",      # wait for the broker to confirm, don't fire and forget
        linger_ms=50,    # let it batch a little instead of a send per message
    )
    print("sending to", TOPIC, "at about", RATE, "per second")

    count = 0
    try:
        while True:
            # every so often send text that isn't valid JSON at all
            if random.random() < 0.02:
                message = b"{this is not json"
            else:
                message = json.dumps(make_record()).encode()

            producer.send(TOPIC, message)
            count += 1

            if count % 100 == 0:
                print(count, "sent")

            time.sleep(1 / RATE)
    except KeyboardInterrupt:
        print("")
        print("stopping")
    finally:
        producer.flush()
        producer.close()
        print("sent", count, "messages total")


if __name__ == "__main__":
    main()
