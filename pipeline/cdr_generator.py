"""
cdr_generator.py: High-throughput Call Detail Record (CDR) streaming simulator with CDC.
Produces synthetic Uganda cellular events into Redpanda topic 'cdr-events'.
CDC distribution: 94% INSERT, 5% UPDATE, 1% DELETE.
"""

import argparse
import collections
import json
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from faker import Faker
from kafka import KafkaProducer
import structlog

# 1. Logging Setup
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer()
    ]
)
logger = structlog.get_logger()

# 2. Telecom Domain Constants
UGANDA_OPERATOR_PREFIXES = [
    # MTN Uganda
    "+25677", "+25678", "+25676",
    # Airtel Uganda
    "+25670", "+25675", "+25674"
]

EVENT_TYPES = ["VOICE", "SMS", "DATA"]
EVENT_TYPE_WEIGHTS = [0.55, 0.25, 0.20]

TERMINATION_CODES = ["NORMAL", "HANDOVER_DROP", "SIGNAL_LOSS", "CONGESTION_DROP"]
TERMINATION_WEIGHTS = [0.88, 0.06, 0.04, 0.02]

MAX_TOWER_ID = 33051  # Matches our oltp_tower_inventory surrogate range
BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "cdr-events"

fake = Faker()


def generate_uganda_msisdn() -> str:
    """Generates a realistic 12-digit Uganda MSISDN (e.g. +256772123456)."""
    prefix = random.choice(UGANDA_OPERATOR_PREFIXES)
    suffix = "".join([str(random.randint(0, 9)) for _ in range(7)])
    return f"{prefix}{suffix}"


def generate_new_cdr() -> dict:
    """Generates a brand new INSERT CDR event."""
    call_id = str(uuid.uuid4())
    event_type = random.choices(EVENT_TYPES, weights=EVENT_TYPE_WEIGHTS)[0]
    term_code = random.choices(TERMINATION_CODES, weights=TERMINATION_WEIGHTS)[0]

    duration = random.randint(5, 1800) if event_type == "VOICE" else (0 if event_type == "SMS" else random.randint(30, 7200))
    rsrp = round(random.uniform(-130.0, -65.0), 2)
    rsrq = round(random.uniform(-19.5, -4.0), 2)
    latency = random.randint(15, 350)

    return {
        "call_id": call_id,
        "caller_msisdn": generate_uganda_msisdn(),
        "callee_msisdn": generate_uganda_msisdn(),
        "tower_id": random.randint(1, MAX_TOWER_ID),
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "duration_sec": duration,
        "termination_code": term_code,
        "rsrp_dbm": rsrp,
        "rsrq_db": rsrq,
        "latency_ms": latency,
        "op": "INSERT"
    }


def produce_stream(total_events: int, rate_per_sec: int, topic: str):
    logger.info("cdr_generator_started", target_events=total_events, rate=rate_per_sec, topic=topic)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
        linger_ms=10
    )

    recent_active_calls = collections.deque(maxlen=1000)
    counts = {"INSERT": 0, "UPDATE": 0, "DELETE": 0}
    start_time = time.time()

    for i in range(total_events):
        # CDC Roll: 94% INSERT, 5% UPDATE, 1% DELETE
        roll = random.random()

        if roll < 0.94 or len(recent_active_calls) == 0:
            # 94%: INSERT
            record = generate_new_cdr()
            recent_active_calls.append(record.copy())
            counts["INSERT"] += 1

        elif roll < 0.99:
            # 5%: UPDATE existing call
            target = random.choice(recent_active_calls)
            record = target.copy()
            record["op"] = "UPDATE"
            record["timestamp"] = datetime.now(timezone.utc).isoformat()
            # Simulate call duration progress or radio signal shift
            record["duration_sec"] += random.randint(10, 120)
            record["rsrp_dbm"] = round(max(-140.0, record["rsrp_dbm"] + random.uniform(-10.0, 5.0)), 2)
            counts["UPDATE"] += 1

        else:
            # 1%: DELETE
            target = recent_active_calls.popleft()
            record = target.copy()
            record["op"] = "DELETE"
            record["timestamp"] = datetime.now(timezone.utc).isoformat()
            counts["DELETE"] += 1

        producer.send(topic, value=record)

        # Rate throttle
        if rate_per_sec > 0:
            time.sleep(1.0 / rate_per_sec)

        if (i + 1) % 500 == 0 or (i + 1) == total_events:
            logger.info("generation_progress", generated=i + 1, counts=counts)

    producer.flush()
    producer.close()
    duration = time.time() - start_time

    logger.info(
        "cdr_generation_completed",
        total_events=total_events,
        counts=counts,
        duration_sec=round(duration, 3),
        events_per_sec=round(total_events / max(duration, 0.001), 1)
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic CDR stream with CDC into Redpanda.")
    parser.add_argument("--events", type=int, default=2500, help="Total events to generate (default: 2500)")
    parser.add_argument("--rate", type=int, default=250, help="Events per second throttle (0 for unthrottled)")
    parser.add_argument("--topic", type=str, default=DEFAULT_TOPIC, help="Target Kafka/Redpanda topic")
    args = parser.parse_args()

    produce_stream(total_events=args.events, rate_per_sec=args.rate, topic=args.topic)
