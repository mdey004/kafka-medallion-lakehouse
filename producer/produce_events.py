"""Synthetic event producer for the customers / products / orders topics.

The data is deliberately messy so every Silver rule has something to do:
  * exact duplicate sends (producer retries)        -> dedup
  * attribute changes over time                     -> SCD Type 2 versions
  * re-sent unchanged records                       -> SCD2 no-op
  * untrimmed strings / mixed-case emails           -> cleaning
  * malformed values (non-numeric price, bad dates) -> try_cast + quarantine
  * order status transitions                        -> latest-event upsert

    python producer/produce_events.py --bootstrap localhost:9092 --customers 50 --orders 500
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

CITIES = [("London", "UK"), ("Paris", "FR"), ("Berlin", "DE"), ("Mumbai", "IN"), ("Austin", "US")]
SEGMENTS = ["consumer", "corporate", "small_business"]
CATEGORIES = {
    "electronics": ["Headphones", "Keyboard", "Monitor", "Webcam"],
    "home": ["Lamp", "Kettle", "Blender"],
    "outdoor": ["Tent", "Backpack", "Bottle"],
}
FIRST = ["Asha", "Ben", "Chen", "Dana", "Eli", "Farah", "Gus", "Hana", "Ivan", "Jo"]
LAST = ["Patel", "Smith", "Li", "Garcia", "Okafor", "Novak", "Rossi", "Kim"]


def iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


class EventSender:
    def __init__(self, bootstrap: str) -> None:
        self.producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
        self.sent = 0

    def send(self, topic: str, key: object, payload: dict) -> None:
        self.producer.produce(topic, key=str(key), value=json.dumps(payload))
        self.sent += 1
        self.producer.poll(0)

    def flush(self) -> None:
        self.producer.flush(30)


def generate(sender: EventSender, n_customers: int, n_orders: int, seed: int) -> None:
    rng = random.Random(seed)
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)

    # ---- products (initial load + price changes)
    products: dict[int, dict] = {}
    pid = 100
    for category, names in CATEGORIES.items():
        for name in names:
            pid += 1
            products[pid] = {
                "product_id": pid,
                "product_name": name,
                "category": category,
                "unit_price": f"{rng.uniform(5, 400):.2f}",
                "status": "active",
                "updated_at": iso(t0),
            }
            sender.send("products", pid, products[pid])

    for pid in rng.sample(list(products), k=max(1, len(products) // 3)):
        changed = dict(products[pid])
        changed["unit_price"] = f"{float(changed['unit_price']) * rng.uniform(0.8, 1.2):.2f}"
        changed["updated_at"] = iso(t0 + timedelta(days=rng.randint(20, 60)))
        products[pid] = changed
        sender.send("products", pid, changed)

    # ---- customers (initial load, duplicates, attribute changes)
    customers: dict[int, dict] = {}
    for cid in range(1, n_customers + 1):
        city, country = rng.choice(CITIES)
        first, last = rng.choice(FIRST), rng.choice(LAST)
        customers[cid] = {
            "customer_id": cid,
            "first_name": f"  {first} ",  # untrimmed on purpose
            "last_name": last,
            "email": f"{first}.{last}{cid}@Example.COM",  # mixed case on purpose
            "city": city,
            "country": country,
            "segment": rng.choice(SEGMENTS),
            "updated_at": iso(t0),
        }
        sender.send("customers", cid, customers[cid])
        if rng.random() < 0.15:  # producer retry -> exact duplicate
            sender.send("customers", cid, customers[cid])

    for cid in rng.sample(list(customers), k=max(1, n_customers // 4)):
        moved = dict(customers[cid])
        moved["city"], moved["country"] = rng.choice(CITIES)
        moved["segment"] = rng.choice(SEGMENTS)
        moved["updated_at"] = iso(t0 + timedelta(days=rng.randint(30, 80)))
        customers[cid] = moved
        sender.send("customers", cid, moved)

    # Re-send an unchanged record with a newer timestamp -> SCD2 must ignore it.
    noop = dict(customers[1])
    noop["updated_at"] = iso(t0 + timedelta(days=90))
    sender.send("customers", 1, noop)

    # Malformed records -> quarantine.
    sender.send(
        "customers", "bad", {"customer_id": "abc", "first_name": "Broken", "updated_at": "not-a-date"}
    )
    sender.send(
        "products",
        "bad",
        {"product_id": "999", "product_name": "Mystery", "unit_price": "N/A", "updated_at": iso(t0)},
    )

    # ---- orders (with status transitions and duplicates)
    for i in range(1, n_orders + 1):
        oid = f"ORD-{i:06d}"
        pid = rng.choice(list(products))
        order = {
            "order_id": oid,
            "customer_id": rng.randint(1, n_customers),
            "product_id": pid,
            "quantity": rng.randint(1, 5),
            "unit_price": products[pid]["unit_price"],
            "order_status": "placed",
            "order_ts": iso(t0 + timedelta(days=rng.randint(0, 120), minutes=rng.randint(0, 1439))),
        }
        sender.send("orders", oid, order)
        if rng.random() < 0.1:
            sender.send("orders", oid, order)  # duplicate
        final_status = rng.choices(["shipped", "delivered", "cancelled", "returned"], [3, 5, 1, 1])[0]
        sender.send("orders", oid, {**order, "order_status": final_status})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--customers", type=int, default=50)
    parser.add_argument("--orders", type=int, default=500)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    sender = EventSender(args.bootstrap)
    generate(sender, args.customers, args.orders, args.seed)
    sender.flush()
    print(f"Produced {sender.sent} events to {args.bootstrap}")


if __name__ == "__main__":
    main()
