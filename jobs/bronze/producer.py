from __future__ import annotations

import logging
from pathlib import Path

LOGGER = logging.getLogger(__name__)
BROKER = "localhost:19092"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "raw"


def delivery_report(err, msg) -> None:
    if err is not None:
        LOGGER.error(
            "Falha ao enviar mensagem para %s [partition=%s]: %s",
            msg.topic(),
            msg.partition(),
            err,
        )


def produce_jsonl_file(file_path: Path, topic: str) -> None:
    try:
        from confluent_kafka import Producer
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "Dependência ausente: instale confluent-kafka antes de executar producer.py"
        ) from exc

    producer = Producer({"bootstrap.servers": BROKER})

    LOGGER.info("Iniciando envio de %s para o tópico %s", file_path.name, topic)

    sent = 0
    with file_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            payload = line.strip()
            if not payload:
                continue

            producer.produce(
                topic=topic,
                value=payload.encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)

            sent += 1
            if sent % 10000 == 0:
                LOGGER.info(
                    "%s: %d mensagens enviadas (linha %d)",
                    file_path.name,
                    sent,
                    line_number,
                )
                producer.flush()

    producer.flush()
    LOGGER.info("Concluído: %s mensagens enviadas para %s", sent, topic)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    jobs = [
        (DATA_DIR / "player_events.jsonl", "topic_player_events"),
        (DATA_DIR / "scte35_markers.jsonl", "topic_scte35_markers"),
    ]

    for file_path, topic in jobs:
        if not file_path.exists():
            LOGGER.error("Arquivo não encontrado: %s", file_path)
            return 1

        produce_jsonl_file(file_path, topic)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
