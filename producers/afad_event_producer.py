"""
AFAD canlı deprem verisini periyodik olarak çeker (polling) ve
`earthquake.events.raw` Kafka topic'ine yayınlar.

AFAD gerçek zamanlı bir WebSocket sunmuyor; bu yüzden kısa aralıklı polling +
event_id bazlı de-duplication kullanıyoruz. Üretimde bu servisi bir
Kubernetes Deployment (replicas=1, çünkü tek producer yeterli) olarak çalıştırın.

Test/demo modu: --simulate bayrağı ile 07_event_scenarios.py'deki senaryoları
gerçek zamanlıymış gibi belirli aralıklarla yayınlar (AFAD'a bağımlı olmadan
pipeline'ı uçtan uca test etmek için).
"""

import argparse
import json
import time
import logging
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("afad_producer")

AFAD_LIVE_ENDPOINT = "https://deprem.afad.gov.tr/apiv2/event/filter"
TOPIC = "earthquake.events.raw"

MIN_MAGNITUDE = 3.0  # gürültüyü azaltmak için eşik altı mikro-depremleri süz


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Kafka'ya iletilemedi: {err}")
    else:
        logger.debug(f"İletildi -> {msg.topic()} [{msg.partition()}]")


def build_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "afad-event-producer",
            # yeniden başlatmalarda mükerrer yayını azaltmak için idempotent producer
            "enable.idempotence": True,
            "acks": "all",
            "retries": 5,
        }
    )


def normalize_afad_event(raw: dict) -> dict:
    """AFAD API şemasını iç event şemamıza dönüştürür."""
    return {
        "event_id": raw.get("eventID") or raw.get("id"),
        "event_name": raw.get("location", "Bilinmeyen Konum"),
        "latitude": float(raw["latitude"]),
        "longitude": float(raw["longitude"]),
        "magnitude": float(raw.get("magnitude") or raw.get("ml") or 0),
        "depth_km": float(raw.get("depth", 10.0)),
        "event_time": raw.get("date") or datetime.now(timezone.utc).isoformat(),
        "source": "AFAD",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def poll_afad(seen_event_ids: set):
    """Son deprem listesini çeker, daha önce görülmemiş olanları döner."""
    try:
        resp = requests.get(AFAD_LIVE_ENDPOINT, timeout=10)
        resp.raise_for_status()
        events = resp.json()
    except requests.RequestException as exc:
        logger.warning(f"AFAD isteği başarısız: {exc}")
        return []

    new_events = []
    for raw in events:
        event = normalize_afad_event(raw)
        if event["event_id"] in seen_event_ids:
            continue
        if event["magnitude"] < MIN_MAGNITUDE:
            continue
        seen_event_ids.add(event["event_id"])
        new_events.append(event)

    return new_events


def simulate_events():
    """Test amaçlı: 07_event_scenarios.py'deki senaryoları tek tek üretir."""
    scenarios = [
        {"event_id": "EVT001", "event_name": "Silivri Açıkları", "latitude": 40.84,
         "longitude": 28.18, "magnitude": 7.2, "depth_km": 12},
        {"event_id": "EVT002", "event_name": "İzmit Körfezi", "latitude": 40.72,
         "longitude": 29.78, "magnitude": 7.1, "depth_km": 15},
        {"event_id": "EVT003", "event_name": "Gemlik Körfezi", "latitude": 40.43,
         "longitude": 28.90, "magnitude": 7.0, "depth_km": 10},
    ]
    for scenario in scenarios:
        scenario["event_time"] = datetime.now(timezone.utc).isoformat()
        scenario["source"] = "SIMULATED"
        scenario["ingested_at"] = datetime.now(timezone.utc).isoformat()
        yield scenario


def run(bootstrap_servers: str, poll_interval: int, simulate: bool):
    producer = build_producer(bootstrap_servers)
    seen_event_ids = set()

    logger.info(f"Producer başladı. simulate={simulate}, interval={poll_interval}s")

    try:
        if simulate:
            for event in simulate_events():
                key = f"{event['event_id']}"
                producer.produce(
                    TOPIC, key=key, value=json.dumps(event), callback=delivery_report
                )
                producer.poll(0)
                logger.info(f"Yayınlandı (simüle): {event['event_id']} - {event['event_name']}")
                time.sleep(poll_interval)
        else:
            while True:
                new_events = poll_afad(seen_event_ids)
                for event in new_events:
                    key = f"{event['event_id']}"
                    producer.produce(
                        TOPIC, key=key, value=json.dumps(event), callback=delivery_report
                    )
                    logger.info(f"Yayınlandı: {event['event_id']} - M{event['magnitude']}")
                producer.flush(timeout=5)
                time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Producer durduruldu.")
    finally:
        producer.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--poll-interval", type=int, default=15, help="saniye")
    parser.add_argument("--simulate", action="store_true", help="AFAD yerine test senaryoları yayınla")
    args = parser.parse_args()

    run(args.bootstrap_servers, args.poll_interval, args.simulate)
