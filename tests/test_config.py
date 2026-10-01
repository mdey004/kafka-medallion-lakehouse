from medallion.config import PipelineConfig


def test_from_env_reads_overrides_and_kafka_options(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    monkeypatch.setenv("KAFKA_TOPIC_ORDERS", "orders.v2")
    monkeypatch.setenv("KAFKA_OPT_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_OPT_SASL_MECHANISM", "PLAIN")

    cfg = PipelineConfig.from_env()

    assert cfg.kafka_bootstrap_servers == "broker:29092"
    assert cfg.topics["orders"] == "orders.v2"
    assert cfg.kafka_options == {
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
    }


def test_object_store_paths_are_not_mangled():
    cfg = PipelineConfig(lakehouse_root="abfss://lake@acct.dfs.core.windows.net/medallion/")
    assert cfg.silver_path("orders") == "abfss://lake@acct.dfs.core.windows.net/medallion/silver/orders"
    assert cfg.checkpoint_path("bronze", "orders").endswith("/medallion/_checkpoints/bronze/orders")
