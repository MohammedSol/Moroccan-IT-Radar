from __future__ import annotations

import json
import logging
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError


LOGGER = logging.getLogger(__name__)


class MarketDataProducer:
    def __init__(self, bootstrap_servers: list[str] | None = None) -> None:
        servers = bootstrap_servers or ["localhost:9093"]
        self.producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=lambda value: json.dumps(
                value, ensure_ascii=False
            ).encode("utf-8"),
        )

    def send_event(self, topic_name: str, event_data: dict[str, Any]) -> None:
        try:
            future = self.producer.send(topic_name, event_data)
            future.get(timeout=10)
            title = str(event_data.get("titre", event_data.get("title", "")))
            LOGGER.info("Événement envoyé avec succès: %s", title[:30])
        except KafkaError as exc:
            LOGGER.error("Échec d'envoi Kafka sur %s: %s", topic_name, exc)
            raise

    def close(self) -> None:
        self.producer.flush()
        self.producer.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    producer = MarketDataProducer()
    test_event = {
        "titre": "Croissance du marché IT au Maroc en 2026",
        "source": "Test Local",
        "date_publication": "2026-05-06",
        "contenu_texte": "Ceci est un événement fictif envoyé vers Kafka pour validation.",
        "url": "https://example.com/test-it-market-event",
    }

    try:
        producer.send_event("it_market_events", test_event)
    finally:
        producer.close()
