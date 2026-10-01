# Day22 - Batch Mini Project: E-commerce Daily Sales Pipeline (PySpark)

An end-to-end Spark **batch** pipeline: read raw transactions, clean bad records, enrich with
customer and product data, aggregate revenue, and write **partitioned Parquet**.

```
transactions.csv ─► clean ─► join customers + products ─► aggregate ─► Parquet (year/month/day)
 customers.csv  ──────────────┘          (broadcast)
 products.csv   ──────────────┘
```

## Project Structure

```
Day22-Batch-Mini-Project/
├── data/
│   ├── transactions.csv     # raw fact data (17 rows, includes bad records)
│   ├── customers.csv        # customer dimension
│   └── products.csv         # product dimension
├── src/
│   └── pipeline.py          # the whole pipeline
├── output/                  # generated (git-ignored)
├── requirements.txt
├── .gitignore
└── README.md
```

## Setup

Prerequisites: **Python 3.9+** and **Java 8/11/17** (Spark needs a JVM; check with `java -version`).

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Run

```bash
python src/pipeline.py
```

Output is written to `output/daily_sales/year=YYYY/month=M/day=D/part-*.parquet`.
Spark UI is available at http://localhost:4040 while the job runs.

## Pipeline Steps

| Step | What happens |
|------|--------------|
| 1. Read | CSVs read with an explicit `StructType` schema (no extra inference pass) |
| 2. Clean | Rejects: null/blank `customer_id`, `quantity <= 0`, `unit_price <= 0`, unparseable date, `payment_status != PAID`; duplicates removed by `transaction_id` |
| 3. Join | Inner join with customers and products (both **broadcast**); unknown customer/product IDs are dropped |
| 4. Aggregate | `revenue = quantity * unit_price`; `groupBy` day + category with `sum`, `countDistinct`, `avg`; plus revenue by city and grand total |
| 5. Write | `repartition(year, month, day)` then `partitionBy(year, month, day)` Parquet |

### Intentionally bad sample records

| Transaction | Problem | Removed at |
|-------------|---------|-----------|
| T1008 | negative quantity | clean |
| T1011 | invalid date | clean |
| T1012 | payment FAILED | clean |
| T1013 | quantity 0 | clean |
| T1015 | missing customer_id | clean |
| T1006 (2nd copy) | duplicate | dedup |
| T1009 | customer C999 not in customers | join |
| T1010 | product P999 not in products | join |

## Expected Results (hand-computed from the sample data)

Counts: 17 raw → 5 rejected, 1 duplicate dropped → **11 valid** → 2 lost in joins → **9 enriched**.

**Grand total revenue: 3600.21**

| Date | Category | Orders | Units | Revenue |
|------|----------|-------:|------:|--------:|
| 2026-09-28 | Electronics | 1 | 2 | 1599.98 |
| 2026-09-28 | Accessories | 1 | 1 | 49.50 |
| 2026-09-28 | Stationery | 1 | 3 | 45.00 |
| 2026-09-29 | Electronics | 2 | 2 | 919.99 |
| 2026-09-29 | Home | 1 | 4 | 89.00 |
| 2026-09-30 | Stationery | 1 | 2 | 30.00 |
| 2026-09-30 | Electronics | 1 | 1 | 799.99 |
| 2026-09-30 | Home | 1 | 3 | 66.75 |

Revenue by city: Hyderabad 1719.98, Mumbai 866.74, Chennai 844.99, Delhi 89.00, Bengaluru 79.50.

## Basic Spark Execution Concepts Used

- **Transformations vs actions**: `filter`, `join`, `groupBy`, `withColumn` are lazy and only build a plan;
  `count()`, `show()`, and `write` are actions that trigger a job.
- **Narrow vs wide**: `filter`/`withColumn` are narrow (no data movement). `groupBy`, `dropDuplicates`, and
  `repartition` are wide and cause a **shuffle** (`Exchange` in the plan).
- **Broadcast join**: `F.broadcast()` ships the small customer/product tables to every executor,
  so the large transaction table is not shuffled for the joins.
- **Caching**: `valid.cache()` keeps the cleaned data in memory because several actions reuse it;
  otherwise Spark would re-read and re-clean the CSV each time.
- **Shuffle partitions**: `spark.sql.shuffle.partitions` is set to 4 (default 200) because the data is tiny.
- **Partitioned output**: `partitionBy` creates one folder per year/month/day. Filtering on those columns
  reads only matching folders (**partition pruning**). `repartition` first avoids many tiny files per folder.
- **Explain plan**: `df.explain()` prints the physical plan so you can verify the broadcast and shuffles.
- **Lazy + lineage**: Spark builds a DAG; stages split at shuffle boundaries (visible in the Spark UI).

## Things to Try

1. Change `.master("local[*]")` to `local[1]` and compare run time in the Spark UI.
2. Remove `F.broadcast(...)` and compare `explain()` (SortMergeJoin appears if tables are large enough).
3. Remove `.repartition(...)` before the write and count the part files per folder.
4. Add a new bad record and confirm it appears in the rejected list.
5. Write the rejected records to `output/rejected/` as CSV for auditing.

## Push to GitHub

```bash
git init
git add .
git commit -m "Day22: PySpark batch mini project - e-commerce daily sales pipeline"
git branch -M main
git remote add origin <your-repo-url>
git push -u origin main
```
