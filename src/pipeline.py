"""
Day22 - E-commerce Daily Sales Batch Pipeline (PySpark)

Stages:  1) Read raw  2) Clean  3) Join  4) Aggregate  5) Write partitioned Parquet
Run:     python src/pipeline.py
"""
import os
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA = os.path.join(BASE, "data")
OUT = os.path.join(BASE, "output")

# Make workers use the same Python as the driver (avoids common local errors)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# Explicit schema: no inferSchema pass over the file, and unparseable numbers become null.
TXN_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("transaction_date", StringType()),   # parsed later so bad dates can be caught
    StructField("payment_status", StringType()),
])


def build_spark():
    return (SparkSession.builder
            .appName("Day22-Batch-Mini-Project")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")   # default 200 is overkill for tiny data
            .getOrCreate())


def read_raw(spark):
    txns = (spark.read.option("header", True).schema(TXN_SCHEMA)
            .option("mode", "PERMISSIVE")
            .csv(os.path.join(DATA, "transactions.csv")))
    customers = spark.read.option("header", True).csv(os.path.join(DATA, "customers.csv"))
    products = spark.read.option("header", True).csv(os.path.join(DATA, "products.csv"))
    return txns, customers, products


def clean(txns):
    """Remove invalid records. Returns (valid_df, rejected_df)."""
    parsed = txns.withColumn("txn_date", F.to_date("transaction_date", "yyyy-MM-dd"))

    # Each rule is a boolean column; rows failing any rule are rejected.
    is_valid = (
        F.col("transaction_id").isNotNull()
        & F.col("customer_id").isNotNull() & (F.trim("customer_id") != "")
        & F.col("product_id").isNotNull()
        & (F.col("quantity") > 0)
        & (F.col("unit_price") > 0)
        & F.col("txn_date").isNotNull()
        & (F.upper("payment_status") == "PAID")
    )

    valid = parsed.filter(is_valid).dropDuplicates(["transaction_id"])
    rejected = parsed.filter(~is_valid)
    return valid, rejected


def enrich(valid, customers, products):
    """Inner joins drop rows with unknown customers/products (referential integrity).
    The small dimension tables are broadcast to avoid shuffling the big fact table."""
    return (valid
            .join(F.broadcast(customers), "customer_id", "inner")
            .join(F.broadcast(products), "product_id", "inner")
            .withColumn("revenue", F.round(F.col("quantity") * F.col("unit_price"), 2))
            .withColumn("year", F.year("txn_date"))
            .withColumn("month", F.month("txn_date"))
            .withColumn("day", F.dayofmonth("txn_date")))


def aggregate(enriched):
    daily_category = (enriched
                      .groupBy("year", "month", "day", "txn_date", "category")
                      .agg(F.countDistinct("transaction_id").alias("total_orders"),
                           F.sum("quantity").alias("units_sold"),
                           F.round(F.sum("revenue"), 2).alias("total_revenue"),
                           F.round(F.avg("revenue"), 2).alias("avg_order_value")))
    by_city = (enriched.groupBy("city")
               .agg(F.round(F.sum("revenue"), 2).alias("total_revenue"))
               .orderBy(F.desc("total_revenue")))
    overall = enriched.agg(F.round(F.sum("revenue"), 2).alias("grand_total_revenue"))
    return daily_category, by_city, overall


def write_output(daily_category):
    # repartition by the partition columns -> one file per day folder instead of many small files
    (daily_category
     .repartition("year", "month", "day")
     .write.mode("overwrite")
     .partitionBy("year", "month", "day")
     .parquet(os.path.join(OUT, "daily_sales")))


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    txns, customers, products = read_raw(spark)
    print(f"Raw transactions: {txns.count()}")

    valid, rejected = clean(txns)
    valid.cache()   # reused by several actions below -> avoid re-reading/re-parsing the CSV
    print(f"Valid: {valid.count()} | Rejected: {rejected.count()}")
    print("--- Rejected records ---")
    rejected.select("transaction_id", "customer_id", "product_id", "quantity",
                    "transaction_date", "payment_status").show(truncate=False)

    enriched = enrich(valid, customers, products)
    print(f"After joins (unknown customer/product removed): {enriched.count()}")

    daily_category, by_city, overall = aggregate(enriched)
    print("--- Daily revenue by category ---")
    daily_category.orderBy("txn_date", "category").show(truncate=False)
    print("--- Revenue by city ---")
    by_city.show()
    print("--- Grand total revenue ---")
    overall.show()

    print("--- Physical plan (note Exchange = shuffle; dimension joins are broadcast) ---")
    daily_category.explain()

    write_output(daily_category)

    print("--- Verify: read back Parquet, partition pruning on day=30 ---")
    check = spark.read.parquet(os.path.join(OUT, "daily_sales"))
    check.printSchema()
    check.filter((F.col("year") == 2026) & (F.col("month") == 9) & (F.col("day") == 30)) \
         .show(truncate=False)

    valid.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
