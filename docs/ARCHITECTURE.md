# Architecture & Design Decisions

## Context and goals

Ingest operational events (customers, products, orders) from Kafka into an analytics-ready lakehouse:

* **No data loss, full replayability.** Any downstream table can be rebuilt.
* **Correct history.** Customer and product attribute changes are tracked (SCD Type 2), and facts join to the version that was valid when the order happened.
* **Idempotent processing.** Restarts, replays and producer retries must not create duplicates.
* **Bad data is isolated, not fatal.** One malformed event must not stop the pipeline.

Assumptions: moderate volume (thousands to millions of events per day), minute-level freshness is acceptable for Silver, and Gold is refreshed in batch.

## High-level design

```
Producer ──► Kafka ──readStream──► Bronze (Delta, append-only)
                                      │  readStream (Delta source) + foreachBatch
                                      ▼
                                   Silver (Delta, MERGE)  ──► _quarantine
                                      │  batch
                                      ▼
                                   Gold (Delta, overwrite): star schema + aggregates
```

| Concern | Choice |
|---|---|
| Compute | PySpark 3.5 Structured Streaming |
| Storage | Delta Lake 3.2 (ACID, MERGE, time travel). Path-based tables, so no metastore is required |
| Exactly-once into Bronze | Kafka offsets tracked in the Spark checkpoint, Delta transactional sink |
| Exactly-once into Silver | Delta streaming source + checkpoint, and an **idempotent MERGE** in `foreachBatch` |
| Scheduling | `trigger(availableNow=True)` drains new data and stops, so it is cheap to run from cron, Airflow or Databricks Jobs. `--continuous` runs on a processing-time trigger |

## Key decisions and trade-offs

### 1. Bronze stores the raw payload as a string
* **Pro:** schema changes upstream never break ingestion, and every byte can be replayed.
* **Con:** parsing cost is paid in Silver. That is acceptable, because Silver needs a strict contract anyway.

### 2. Parse as strings, then `try_cast`
`from_json` with a typed schema silently NULLs an *entire* record on a type mismatch. Parsing to strings and casting column by column yields a NULL only in the bad column. The required-column check then routes the row to `_quarantine` with a reason and the original payload.

### 3. SCD2 via the "staged updates" MERGE
One atomic Delta `MERGE` both closes the old version and inserts the new one. Readers never see two current rows. Rules per batch:

* Deduplicate on `(key, event_ts)`, latest Kafka offset wins.
* Union with the current target rows and drop **late events**: `event_ts <= current.valid_from`.
  * This makes replays idempotent.
  * Trade-off: a genuinely late correction is ignored rather than spliced into history. Splicing would need a full history rewrite per key. It's in the "revisit" list below.
* Drop versions whose attribute hash equals the previous version's (no-op updates).
* Chain the remaining versions with `lead(valid_from)`, so several changes in one micro-batch are all preserved.
* Surrogate key = `xxhash64(business_key, valid_from)`. It is deterministic, so a rebuild reproduces the same keys and needs no sequence or identity column.

### 4. Orders: latest-event upsert
Order status changes over time (placed → shipped → delivered). Silver keeps one row per `order_id`. The MERGE only updates when the incoming event is newer (Kafka timestamp, then offset), so out-of-order delivery cannot regress a status.

### 5. Gold is a full refresh
Point-in-time joins depend on the whole SCD2 history, and the data volume is small, so an overwrite is the simplest correct option. Delta keeps the previous versions for time travel and rollback.

### 6. Unknown member (`-1`)
Facts whose customer or product has no valid version at `order_ts` (late-arriving dimension) get `-1` instead of NULL. Inner joins then never drop revenue, and data gaps show up in reports.

## Failure handling

| Failure | Behaviour |
|---|---|
| Malformed JSON or values | Row goes to `silver/_quarantine` and the batch continues |
| Job crash mid-batch | Checkpoint not committed, the batch is re-processed, and the MERGE is idempotent |
| Kafka retention expired before ingest | `failOnDataLoss=false` logs and continues. Alert on it in production |
| Duplicate producer sends | Removed by dedup (Silver) and the SCD2 no-op rule |

## What to revisit as the system grows

* **Gold incremental builds:** MERGE only the affected date partitions of `fct_orders` and the aggregates.
* **Late-arriving dimension corrections:** rewrite the history window for affected keys, then re-key the impacted facts.
* **Schema Registry / Avro or Protobuf:** replace JSON string contracts with registry-enforced schemas.
* **Table maintenance:** schedule `OPTIMIZE` + `ZORDER BY (customer_id)` and `VACUUM`. Use liquid clustering on Delta 3.x.
* **Observability:** publish `StreamingQueryListener` metrics (input rows, batch duration, lag) and quarantine counts to monitoring, and alert on growth.
* **Unity Catalog / Hive metastore:** register tables by name instead of by path.
