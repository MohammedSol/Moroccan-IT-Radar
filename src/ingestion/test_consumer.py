from __future__ import annotations

import json
from kafka import KafkaConsumer


def main() -> None:
    consumer = KafkaConsumer(
        "it_market_events",
        bootstrap_servers=["localhost:9093"],
        auto_offset_reset="latest",
        value_deserializer=lambda msg: json.loads(msg.decode("utf-8")),
    )
    print("Consumer démarré. En attente de messages...")
    for message in consumer:
        event = message.value
        print("\n" + "=" * 80)
        print(f"[NOUVEAU ÉVÉNEMENT] Topic: {message.topic}")
        print(f"  Titre: {event.get('titre', 'N/A')}")
        print(f"  Source: {event.get('source', 'N/A')}")
        print(f"  Date: {event.get('date_publication', 'N/A')}")
        print(f"  URL: {event.get('url', 'N/A')}")
        print(f"  Texte (100 char): {event.get('contenu_texte', 'N/A')[:100]}...")
        print("=" * 80)


if __name__ == "__main__":
    main()
