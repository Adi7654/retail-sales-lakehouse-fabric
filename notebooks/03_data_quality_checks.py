# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Data quality checks (silver)
# MAGIC Runs automated checks on the `silver` tables and appends every result to `silver.dq_results`,
# MAGIC so quality is tracked over time.
# MAGIC
# MAGIC Each check is a SQL query that **counts bad rows**: 0 means pass.
# MAGIC - `error` checks **fail the job** if they find bad rows, stopping bad data from reaching gold.
# MAGIC - `warn` checks are logged only, for known source-data quirks worth monitoring.

# COMMAND ----------

import uuid
from datetime import datetime, timezone

CATALOG = "workspace"
S = f"{CATALOG}.silver"

# COMMAND ----------

# MAGIC %md ## Check definitions

# COMMAND ----------

def not_null(table, col):
    return (f"{table}_{col}_not_null", table, "error",
            f"SELECT COUNT(*) FROM {S}.{table} WHERE {col} IS NULL")

def unique(table, cols):
    key = ", ".join(cols)
    name = "_".join(cols)
    return (f"{table}_{name}_unique", table, "error",
            f"SELECT COUNT(*) FROM (SELECT {key} FROM {S}.{table} GROUP BY {key} HAVING COUNT(*) > 1)")

def foreign_key(child, col, parent, parent_col=None, severity="error"):
    parent_col = parent_col or col
    return (f"{child}_{col}_exists_in_{parent}", child, severity,
            f"SELECT COUNT(*) FROM {S}.{child} c LEFT ANTI JOIN {S}.{parent} p "
            f"ON c.{col} = p.{parent_col} WHERE c.{col} IS NOT NULL")

def rule(name, table, condition, severity="error"):
    """condition describes a BAD row."""
    return (name, table, severity, f"SELECT COUNT(*) FROM {S}.{table} WHERE {condition}")

def not_empty(table):
    return (f"{table}_not_empty", table, "error",
            f"SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM {S}.{table}")


checks = [
    # Completeness
    *[not_empty(t) for t in ["customers", "orders", "order_items", "products", "sellers"]],
    # Primary keys
    not_null("orders", "order_id"),           unique("orders", ["order_id"]),
    not_null("customers", "customer_id"),     unique("customers", ["customer_id"]),
    not_null("products", "product_id"),       unique("products", ["product_id"]),
    not_null("sellers", "seller_id"),         unique("sellers", ["seller_id"]),
    unique("order_items", ["order_id", "order_item_id"]),
    unique("order_payments", ["order_id", "payment_sequential"]),
    unique("order_reviews", ["review_id", "order_id"]),
    # Referential integrity
    foreign_key("orders", "customer_id", "customers"),
    foreign_key("order_items", "order_id", "orders"),
    foreign_key("order_items", "product_id", "products"),
    foreign_key("order_items", "seller_id", "sellers"),
    foreign_key("order_payments", "order_id", "orders"),
    foreign_key("order_reviews", "order_id", "orders"),
    # Valid values and ranges
    rule("order_items_price_positive", "order_items", "price IS NULL OR price <= 0"),
    rule("order_items_freight_non_negative", "order_items", "freight_value IS NULL OR freight_value < 0"),
    rule("order_payments_value_non_negative", "order_payments", "payment_value IS NULL OR payment_value < 0"),
    rule("order_payments_installments_non_negative", "order_payments", "payment_installments < 0"),
    rule("order_reviews_score_1_to_5", "order_reviews", "review_score NOT BETWEEN 1 AND 5"),
    rule("orders_status_valid", "orders",
         "order_status NOT IN ('created','approved','invoiced','processing','shipped',"
         "'delivered','canceled','unavailable')"),
    rule("orders_purchase_ts_not_null", "orders", "order_purchase_ts IS NULL"),
    rule("products_category_not_null", "products", "product_category_name_english IS NULL"),
    # Business logic (known source quirks: monitor, don't block)
    rule("orders_delivered_after_purchase", "orders",
         "order_delivered_customer_ts < order_purchase_ts", severity="warn"),
    rule("orders_delivered_status_has_delivery_date", "orders",
         "order_status = 'delivered' AND order_delivered_customer_ts IS NULL", severity="warn"),
]
print(f"{len(checks)} checks defined")

# COMMAND ----------

# MAGIC %md ## Run checks

# COMMAND ----------

run_id = str(uuid.uuid4())
run_ts = datetime.now(timezone.utc).replace(tzinfo=None)
rows = []

for name, table, severity, sql in checks:
    failed = int(spark.sql(sql).collect()[0][0] or 0)
    status = "PASS" if failed == 0 else ("FAIL" if severity == "error" else "WARN")
    rows.append((run_id, run_ts, name, table, severity, failed, status))
    print(f"[{status:4}] {name}: {failed:,} bad rows")

results = spark.createDataFrame(
    rows,
    "run_id STRING, run_ts TIMESTAMP, check_name STRING, table_name STRING, "
    "severity STRING, failed_rows LONG, status STRING",
)
results.write.mode("append").format("delta").saveAsTable(f"{S}.dq_results")

# COMMAND ----------

# MAGIC %md ## Summary (fails the job on any `error` check)

# COMMAND ----------

passed = sum(r[6] == "PASS" for r in rows)
warned = sum(r[6] == "WARN" for r in rows)
failed = [r[2] for r in rows if r[6] == "FAIL"]
print(f"\n{passed} passed, {warned} warnings, {len(failed)} failed (out of {len(rows)} checks)")

if failed:
    raise Exception(f"Data quality checks failed: {failed}")
