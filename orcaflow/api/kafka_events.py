"""
Kafka Event Streaming for OrcaFlow

Produces and consumes job lifecycle events through Kafka topics.
Events track: job_submitted, job_classified, job_running, job_completed, job_failed.
"""

import json
import logging
import threading
import time
from datetime import datetime
from typing import Dict, Any, Optional, List, Callable

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_JOB_EVENTS = "orcaflow-job-events"
TOPIC_CLUSTER_METRICS = "orcaflow-cluster-metrics"


class KafkaEventProducer:
    """Publishes job lifecycle events to Kafka."""

    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.enabled = False
        self._connect()

    def _connect(self):
        """Attempt to connect to Kafka broker."""
        try:
            from kafka import KafkaProducer
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=5000,
                max_block_ms=5000,
            )
            self.enabled = True
            logger.info(f"Kafka producer connected to {self.bootstrap_servers}")
        except Exception as e:
            self.enabled = False
            logger.warning(f"Kafka not available ({e}) — events will be logged only")

    def emit(self, event_type: str, job_id: str, data: Dict[str, Any] = None,
             topic: str = TOPIC_JOB_EVENTS):
        """
        Emit a job event to Kafka.

        Args:
            event_type: Event type (job_submitted, job_classified, etc.)
            job_id: Job identifier
            data: Additional event data
            topic: Kafka topic (default: orcaflow-job-events)
        """
        event = {
            "event_type": event_type,
            "job_id": job_id,
            "timestamp": datetime.now().isoformat(),
            "data": data or {},
        }

        # Always log
        logger.info(f"[EVENT] {event_type} | job={job_id} | {data}")

        if self.enabled and self.producer:
            try:
                self.producer.send(topic, value=event)
                self.producer.flush(timeout=2)
            except Exception as e:
                logger.warning(f"Failed to send Kafka event: {e}")

    def emit_job_submitted(self, job_id: str, job_data: Dict[str, Any]):
        self.emit("job_submitted", job_id, {
            "job_type": job_data.get("job_type"),
            "dataset_size_mb": job_data.get("dataset_size_mb"),
        })

    def emit_job_classified(self, job_id: str, classification: Dict[str, Any]):
        self.emit("job_classified", job_id, {
            "job_class": classification.get("job_class"),
            "confidence": classification.get("confidence"),
        })

    def emit_job_routed(self, job_id: str, target: str, job_class: str):
        self.emit("job_routed", job_id, {
            "execution_target": target,
            "job_class": job_class,
        })

    def emit_job_running(self, job_id: str, target: str):
        self.emit("job_running", job_id, {"execution_target": target})

    def emit_job_completed(self, job_id: str, execution_time: float = None,
                           records: int = None):
        self.emit("job_completed", job_id, {
            "execution_time_sec": execution_time,
            "records_processed": records,
        })

    def emit_job_failed(self, job_id: str, error: str):
        self.emit("job_failed", job_id, {"error": error})

    def emit_cluster_metrics(self, metrics: Dict[str, Any]):
        self.emit("cluster_metrics", "system", metrics,
                  topic=TOPIC_CLUSTER_METRICS)


class KafkaEventConsumer:
    """Consumes job events from Kafka and stores recent history."""

    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP,
                 topic: str = TOPIC_JOB_EVENTS, max_events: int = 200):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.max_events = max_events
        self.events: List[Dict[str, Any]] = []
        self.enabled = False
        self._lock = threading.Lock()
        self._running = False

    def start(self):
        """Start consuming events in a background thread."""
        try:
            from kafka import KafkaConsumer
            self.enabled = True
            self._running = True
            thread = threading.Thread(target=self._consume_loop, daemon=True)
            thread.start()
            logger.info(f"Kafka consumer started for topic '{self.topic}'")
        except ImportError:
            logger.warning("kafka-python not installed — event consumer disabled")
        except Exception as e:
            logger.warning(f"Kafka consumer failed to start: {e}")

    def _consume_loop(self):
        """Background consumer loop."""
        from kafka import KafkaConsumer
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",
                consumer_timeout_ms=1000,
                group_id="orcaflow-dashboard",
            )

            while self._running:
                for message in consumer:
                    with self._lock:
                        self.events.append(message.value)
                        if len(self.events) > self.max_events:
                            self.events = self.events[-self.max_events:]
                time.sleep(0.5)
        except Exception as e:
            logger.warning(f"Kafka consumer loop error: {e}")
            self._running = False

    def get_recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent events."""
        with self._lock:
            return list(reversed(self.events[-limit:]))

    def stop(self):
        """Stop the consumer."""
        self._running = False


# Global instances
event_producer = KafkaEventProducer()
event_consumer = KafkaEventConsumer()
