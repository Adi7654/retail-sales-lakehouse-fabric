# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Order event generator
# MAGIC Simulates a live order feed: writes small **JSON files of order events** into a landing Volume,
# MAGIC the way an app or message queue would drop files during the day.
# MAGIC
# MAGIC - Each event = one order item (`order_id`, `order_item_id`, customer, product, seller, price, payment type, timestamp)
# MAGIC - IDs are real customers, products and sellers from `silver`, so events join cleanly to the gold dimensions
# MAGIC - About **2% of events are sent twice** (same `event_id`), like a real at-least-once feed, to test deduplication
# MAGIC
# MAGIC Run it, then run `07_stream_orders`. Run both again to see Auto Loader pick up **only the new files**.

# COMMAND ----------

import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F

CATALOG = "workspace"
EVENTS_PATH = f"/Volumes/{CATALOG}/bronze/landing/order_events"

N_FILES = 5             # files written per run
ORDERS_PER_FILE = 30    # each order has 1-3 items → ~60 events per file
DUPLICATE_RATE = 0.02   # share of events re-sent with the same event_id

dbutils.fs.mkdirs(EVENTS_PATH)

# COMMAND ----------

# MAGIC %md ## Real IDs to build realistic events

# COMMAND ----------

customer_ids = [r.customer_id for r in
                spark.table(f"{CATALOG}.silver.customers").select("customer_id")
                     .orderBy(F.rand()).limit(1000).collect()]

# Product + seller + realistic price/freight taken from real order items
catalog_items = [r.asDict() for r in
                 spark.table(f"{CATALOG}.silver.order_items")
                      .select("product_id", "seller_id", "price", "freight_value")
                      .orderBy(F.rand()).limit(1000).collect()]

PAYMENT_TYPES = ["credit_card"] * 7 + ["boleto"] * 2 + ["voucher", "debit_card"]
print(f"Using {len(customer_ids):,} customers and {len(catalog_items):,} product/seller combinations")

# COMMAND ----------

# MAGIC %md ## Write the event files

# COMMAND ----------

now = datetime.now(timezone.utc)
batch = now.strftime("%Y%m%dT%H%M%S")
total_events, total_dupes, all_events = 0, 0, []

for f in range(N_FILES):
    events = []
    for _ in range(ORDERS_PER_FILE):
        order_id = uuid.uuid4().hex
        customer_id = random.choice(customer_ids)
        payment_type = random.choice(PAYMENT_TYPES)
        order_ts = now - timedelta(seconds=random.randint(0, 600))
        for item_no in range(1, random.randint(1, 3) + 1):
            item = random.choice(catalog_items)
            events.append({
                "event_id": uuid.uuid4().hex,
                "order_id": order_id,
                "order_item_id": item_no,
                "customer_id": customer_id,
                "product_id": item["product_id"],
                "seller_id": item["seller_id"],
                "price": round(item["price"], 2),
                "freight_value": round(item["freight_value"], 2),
                "payment_type": payment_type,
                "order_status": "created",
                "event_ts": order_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            })

    originals = list(events)

    # Re-send a few events from earlier files (same event_id) to simulate duplicates
    if all_events:
        dupes = random.sample(all_events, max(1, int(len(events) * DUPLICATE_RATE)))
        events.extend(dupes)
        total_dupes += len(dupes)

    path = f"{EVENTS_PATH}/orders_{batch}_{f:02d}.json"
    dbutils.fs.put(path, "\n".join(json.dumps(e) for e in events), overwrite=True)
    all_events.extend(originals)
    total_events += len(events)
    print(f"wrote {path}  ({len(events)} events)")

print(f"\n{N_FILES} files, {total_events:,} events ({total_dupes} duplicates included)")
