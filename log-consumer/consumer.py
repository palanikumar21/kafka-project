import os
import json
import logging
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import start_http_server, Counter
from datetime import datetime

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "app-logs")
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "8000"))
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "alerts")  # optional

# Prometheus metrics
total_logs = Counter("logs_processed_total", "Total logs processed")
severity_counter = Counter("logs_by_severity_total",
                           "Logs by severity", ["severity"])
service_counter = Counter("logs_by_service_total",
                          "Logs by service", ["service"])
error_counter = Counter("logs_errors_total", "Total ERROR logs")


def handle_message(msg):
    try:
        payload = json.loads(msg.value.decode("utf-8"))
    except Exception as e:
        logging.error("Invalid JSON: %s", e)
        return

    total_logs.inc()
    sev = payload.get("severity", "UNKNOWN")
    severity_counter.labels(sev).inc()
    service_counter.labels(payload.get("service", "unknown")).inc()

    if sev == "ERROR":
        error_counter.inc()
        # write to a file (could be shipped to ELK or other store)
        with open("/tmp/error_logs.txt", "a") as f:
            f.write(f"{datetime.utcnow().isoformat()} {json.dumps(payload)}\n")
        # optionally forward to an alerts topic
        if "producer" in globals():
            try:
                producer.send(ALERT_TOPIC, payload)
            except Exception as e:
                logging.error("Failed to send to alerts topic: %s", e)


if __name__ == "__main__":
    logging.info("Starting consumer. Prometheus on port %s", PROMETHEUS_PORT)
    start_http_server(PROMETHEUS_PORT)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="log-consumers-group",
        consumer_timeout_ms=1000,
    )

    # Create a producer only if ALERT_TOPIC is defined
    producer = None
    try:
        if ALERT_TOPIC:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )

        while True:
            for msg in consumer:
                handle_message(msg)
    except KeyboardInterrupt:
        logging.info("Shutting down consumer")
    finally:
        try:
            consumer.close()
        except:
            pass
        if producer:
            producer.flush()
            producer.close()
