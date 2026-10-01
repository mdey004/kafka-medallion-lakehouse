"""Environment-driven pipeline configuration.

Every setting has a local-dev default so the pipeline runs out of the box
against the docker-compose Kafka broker and a local ``./lakehouse`` folder.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# Logical entity name -> Kafka topic name.
DEFAULT_TOPICS: dict[str, str] = {
    "customers": "customers",
    "products": "products",
    "orders": "orders",
}


KAFKA_OPTION_PREFIX = "KAFKA_OPT_"


def _kafka_options_from_env() -> dict[str, str]:
    """``KAFKA_OPT_SECURITY_PROTOCOL=SASL_SSL`` -> ``{"kafka.security.protocol": "SASL_SSL"}``.

    Credentials stay in the environment / secret store, never in code.
    """
    return {
        "kafka." + name[len(KAFKA_OPTION_PREFIX) :].lower().replace("_", "."): value
        for name, value in os.environ.items()
        if name.startswith(KAFKA_OPTION_PREFIX)
    }


@dataclass(frozen=True)
class PipelineConfig:
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_starting_offsets: str = "earliest"
    lakehouse_root: str = "./lakehouse"
    topics: dict[str, str] = field(default_factory=lambda: dict(DEFAULT_TOPICS))
    # Extra Kafka client options, e.g. security settings for Confluent Cloud / MSK.
    kafka_options: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> PipelineConfig:
        return cls(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", cls.kafka_bootstrap_servers),
            kafka_starting_offsets=os.getenv("KAFKA_STARTING_OFFSETS", cls.kafka_starting_offsets),
            lakehouse_root=os.getenv("LAKEHOUSE_ROOT", cls.lakehouse_root),
            topics={
                entity: os.getenv(f"KAFKA_TOPIC_{entity.upper()}", topic)
                for entity, topic in DEFAULT_TOPICS.items()
            },
            kafka_options=_kafka_options_from_env(),
        )

    # ---- path helpers -------------------------------------------------
    def _path(self, *parts: str) -> str:
        # Object-store URIs (s3a://, abfss://, dbfs:/) must not go through pathlib.
        if ":/" in self.lakehouse_root:
            return "/".join([self.lakehouse_root.rstrip("/"), *parts])
        return Path(self.lakehouse_root, *parts).resolve().as_posix()

    def bronze_path(self, entity: str) -> str:
        return self._path("bronze", entity)

    def silver_path(self, table: str) -> str:
        return self._path("silver", table)

    def gold_path(self, table: str) -> str:
        return self._path("gold", table)

    def checkpoint_path(self, layer: str, name: str) -> str:
        return self._path("_checkpoints", layer, name)
