import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers = "localhost:9093",
    value_serializer = lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "behavior-events"

event_types = ["login", "view", "click", "logout"]

print("Starting behavior producer...")

while True:
    event = {
        "event_id": fake.uuid4(),
        "user_id": f"user_{random.randint(1,100)}",
        "event_type": random.choice(event_types),
        "page": random.choice(["home", "product", "profile"]),
        "event_time": datetime.now(timezone.utc).isformat(),
        "ip": fake.ipv4();
    }

    producer.send(TOPIC, event)
    print('Sent:', event)

    time.sleep(1)