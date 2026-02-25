import json
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9093",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "risk-rules"

rule = {
    "op": "UPSERT",
    "rule": {
        "rule_id": "R001",
        "enabled": True,
        "rule_type": "count_in_window",
        "event_type": "login",
        "window_minutes": 1,
        "action": "alert",
        "severity": "high",
        "updated_at": datetime.now(timezone.utc).isformat()
    }
}

producer.send(TOPIC, rule)
producer.flush()

print("Rule sent:", rule)