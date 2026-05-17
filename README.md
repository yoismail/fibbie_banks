---
# 🏦 FIBBIEBANKS CASE STUDY — A 1M‑Row PySpark ETL Pipeline & Star Schema Warehouse  
*A production‑grade distributed ETL system I designed and built using PySpark, PostgreSQL, and SQLAlchemy, processing 1 million synthetic banking transactions into a fully‑validated dimensional warehouse.*

---

## 🏷️ Badges  
![Python](https://img.shields.io/badge/Python-3.10+-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.5+-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14+-blue)
![ETL Pipeline](https://img.shields.io/badge/ETL-Production--Grade-green)
![Star Schema](https://img.shields.io/badge/Schema-Star-yellow)
![Idempotent](https://img.shields.io/badge/Loads-Idempotent-success)
![Rows](https://img.shields.io/badge/Rows-1%2C000%2C000-red)

---

# 🌟 Why This Project Matters  

I built FibbieBanks to demonstrate how I approach **distributed data engineering at production scale** — not just moving data, but designing a warehouse that can be re‑run safely, validated at every stage, and trusted by downstream consumers.

My goals were clear:

- Process **1 million banking transactions** through a full ETL lifecycle  
- Build a **distributed pipeline** using PySpark that can scale beyond a single machine  
- Guarantee **idempotency** — running the pipeline twice never corrupts the warehouse  
- Validate the schema at **every transformation stage**, not just at load time  
- Maintain **type discipline end‑to‑end** — money stays NUMERIC, dates stay DATE, surrogate keys stay deterministic  
- Own the schema declaratively through SQL DDL, not through Spark auto‑inference  

This project reflects how I think as a data engineer:  
**deliberate about contracts, defensive about types, and disciplined about the boundary between source data and the warehouse.**

---

# 🧠 What I Demonstrate in This Project

### 🔹 Distributed ETL Engineering  
I designed a 5‑stage PySpark pipeline (extract → explore → clean → transform → load) that processes 1M rows end‑to‑end in ~25 minutes on a single local Spark instance, with the same code portable to a managed cluster.

### 🔹 Kimball Dimensional Modeling  
I built a complete star schema with 4 conformed dimensions (`dim_customer`, `dim_transaction`, `dim_employee`, `dim_date`) and 1 fact table (`fact_transactions`), with full referential integrity enforced through PRIMARY KEYs and FOREIGN KEYs at the DDL layer.

### 🔹 Deterministic Surrogate Keys  
I replaced Spark's `monotonically_increasing_id()` (non‑deterministic across runs) with `SHA‑256(concat_ws('|', ...natural keys))` so the same input row always produces the same surrogate key — making the entire pipeline idempotent.

### 🔹 Validation at Every Stage  
I wrote a `validate_schema()` utility called before and after every transformation, so missing columns trigger a hard failure with a clear error message rather than silently producing broken downstream tables.

### 🔹 Type Discipline End‑to‑End  
Money flows through the pipeline as `DecimalType(18,2)` in Spark and lands as `NUMERIC(18,2)` in Postgres. Timestamps are converted to `DATE` for `date_key` joins but preserved as `TIMESTAMP` in `dim_transaction` for audit detail. No silent type drift between layers.

### 🔹 Schema‑Owned‑By‑SQL  
DDL lives in a versioned `.sql` file with PRIMARY KEYs, FOREIGN KEYs, and 7 indexes for read performance. The Python ETL hard‑fails with a clear error if the schema isn't present, rather than auto‑creating tables with inferred types that would discard constraints.

### 🔹 Production‑Style Logging  
I built a custom `logger.py` with a `@timed` decorator that wraps every pipeline stage and a `section()` utility that prints visual banners. Every transformation logs its before/after row counts so silent corruption is impossible.

### 🔹 Environment‑Driven Configuration  
All credentials live in `.env` (gitignored) and are validated at import time — the pipeline refuses to start if any required environment variable is missing, with a clear error listing exactly which ones.

### 🔹 Transactional Postgres Merges  
Loads use a temp‑table + LEFT JOIN merge pattern inside `engine.begin()` so partial failures roll back cleanly. JDBC tuning (batchsize 100k, isolation level `READ_COMMITTED`) keeps bulk inserts fast without compromising consistency.

---

# 🌐 High‑Level Architecture Diagram  

```
                ┌──────────────────────────────┐
                │   fibbie_bank_transactions   │
                │       (1M rows · 23 cols)    │
                └──────────────┬───────────────┘
                               ▼
                ┌──────────────────────────────┐
                │          EXTRACT             │
                │   PySpark CSV reader (lazy)  │
                │   • Schema inference         │
                │   • 14g driver/executor      │
                └──────────────┬───────────────┘
                               ▼
                ┌──────────────────────────────┐
                │          EXPLORE             │
                │   Profile every column       │
                │   • Schema overview          │
                │   • Null counts              │
                │   • Distinct cardinality     │
                └──────────────┬───────────────┘
                               ▼
                ┌──────────────────────────────┐
                │           CLEAN              │
                │   • Snake_case column names  │
                │   • Type‑aware fillna        │
                │   • Drop rows w/ no audit ts │
                │   • Amount → DECIMAL(18,2)   │
                │   • Deduplicate              │
                └──────────────┬───────────────┘
                               ▼
                ┌──────────────────────────────┐
                │         TRANSFORM            │
                │   Build 4 dims + 1 fact      │
                │   • SHA‑256 surrogate keys   │
                │   • Date dim (YYYYMMDD key)  │
                │   • Validate every stage     │
                │   • Join‑on‑natural‑key      │
                └──────────────┬───────────────┘
                               ▼
                ┌──────────────────────────────┐
                │            LOAD              │
                │   PostgreSQL via JDBC        │
                │   • Temp table + LEFT JOIN   │
                │   • Idempotent merge         │
                │   • Transactional commits    │
                └──────────────┬───────────────┘
                               ▼
                ┌──────────────────────────────┐
                │      ANALYTICS WAREHOUSE     │
                │   4 dimensions + 1 fact      │
                │   FK enforced · 7 indexes    │
                └──────────────┬───────────────┘
                               ▼
                ┌──────────────────────────────┐
                │            BI TOOLS          │
                │   (Tableau, Power BI, dbt)   │
                └──────────────────────────────┘
```

---

# 🧩 ERD — Star Schema Warehouse

```
                              +----------------------+
                              |     dim_customer     |
                              +----------------------+
                              | customer_id (PK)     |
                              | customer_name        |
                              | customer_address     |
                              | customer_city        |
                              | customer_state       |
                              | customer_country     |
                              | email                |
                              | phone_number         |
                              +----------▲-----------+
                                         │
                                         │ FK
                                         │
+----------------------+      +----------+-----------+      +----------------------+
|   dim_transaction    |◀─FK──│   fact_transactions  │──FK─▶│     dim_employee     |
+----------------------+      +----------------------+      +----------------------+
| transaction_id (PK)  |      | transaction_id (PK)  |      | employee_id (PK)     |
| transaction_date     |      | customer_id (FK)     |      | company              |
| date_key (FK)        |      | employee_id (FK)     |      | job_title            |
| transaction_type     |      | date_key (FK)        |      | gender               |
| amount NUMERIC(18,2) |      | credit_card_number   |      | marital_status       |
+----------------------+      | iban                 |      +----------------------+
                              | currency_code        |
                              | random_number        |
                              | category             |
                              | "group"              |
                              | is_active            |
                              | last_updated         |
                              | description          |
                              +----------▲-----------+
                                         │
                                         │ FK
                                         │
                              +----------+-----------+
                              |       dim_date       |
                              +----------------------+
                              | date_key (PK)        |
                              | full_date            |
                              | year                 |
                              | month                |
                              | month_name           |
                              | quarter              |
                              | day_of_month         |
                              | day_name             |
                              | is_weekend           |
                              +----------------------+
```

**Why this schema works:**
- The fact has **four FKs** linking to every conformed dimension — every analytic query joins through `fact_transactions`
- The **degenerate dimensions** (credit_card_number, iban, currency_code, etc.) live in the fact rather than being broken out, because they have one‑to‑one cardinality with transactions — splitting them would inflate the schema without analytic benefit
- **`dim_date` uses an integer `YYYYMMDD` primary key** (the Kimball standard) rather than a hash, because date dimensions are tiny, naturally sortable, and benefit from human‑readable keys
- **Other dimensions use 64‑char SHA‑256 hash surrogate keys**, derived from each dimension's natural‑key tuple, so re‑runs produce identical IDs and the warehouse stays idempotent

---

# 🗂 Project Structure 

```
fibbie_banks/
├── data_model/
│   └── fibbie_bank_data_model.jpg   # Visual ER diagram
│
├── dataset/
│   └── fibbie_bank_transactions.csv # 1M‑row source dataset
│
├── etl/
│   ├── __init__.py
│   ├── extract.py                   # PySpark CSV reader + session config
│   ├── explore.py                   # Schema, nulls, cardinality profiling
│   ├── clean.py                     # Normalize, fillna, amount cast, dedupe
│   ├── transform.py                 # Build 4 dims + 1 fact star schema
│   ├── load.py                      # JDBC temp + LEFT JOIN merge
│   ├── logger.py                    # Custom logger + @timed + section()
│   └── db_config.py                 # Env‑var loader with validation
│
├── logs/
│   └── pipeline.log                 # Rotating run logs
│
├── sql/
│   └── create_tables.sql            # Schema DDL (PKs, FKs, indexes)
│
├── venv/                            # Virtual environment (gitignored)
├── .env                             # Credentials (gitignored)
├── .gitattributes
├── .gitignore
├── README.md
└── requirements.txt
```

---

# 🔄 Pipeline Flow 

### 1️⃣ Extract  
I create a Spark session tuned for the workload (14g driver/executor, KryoSerializer, custom Postgres JDBC jar) and lazily read the source CSV with schema inference. The DataFrame and session are returned together so downstream stages can reuse the same Spark context without re‑initialising.

### 2️⃣ Explore  
I profile the entire dataset — schema overview, sample rows, total row count, total column count, null count per column, and distinct values for categorical columns like `transaction_type`. This is its own module rather than buried in extract, because **knowing your data is a first‑class engineering concern**, not an afterthought.

### 3️⃣ Clean  
I normalise column names to `snake_case`, fill NULLs with **type‑aware defaults** (`"Unknown"` for strings, `0` for ints, `0.0` for floats), drop rows where `last_updated` is NULL (no audit timestamp = unreliable row), cast `amount` from string to `DECIMAL(18,2)` after stripping currency symbols and thousands separators, and deduplicate. I log before/after null counts at every step so silent corruption is impossible.

### 4️⃣ Transform  
I build the star schema:  
- **`dim_date`** generated first from unique transaction dates with pre‑computed attributes (year, month, quarter, day_name, is_weekend)  
- **`dim_customer`**, **`dim_transaction`**, **`dim_employee`** each get a deterministic `SHA‑256` surrogate key derived from their natural‑key tuple  
- **`fact_transactions`** joins back to all four dimensions on natural keys, then projects only the surrogate keys + degenerate dimensions  
- `validate_schema()` runs at every stage to fail loudly if a column is missing

### 5️⃣ Load  
I load each dimension and the fact table into PostgreSQL via JDBC using a temp‑table + LEFT JOIN merge:  
1. Write the Spark DataFrame to `analytics.temp_<table>` (overwrite mode)  
2. Inside a single transaction, `INSERT INTO target SELECT src.* FROM temp src LEFT JOIN target tgt ON keys WHERE tgt.id IS NULL`  
3. Drop the temp table

This means **only rows with surrogate keys not already in the target get appended.** Re‑runs on unchanged data insert zero rows.

The pipeline hard‑fails with a clear error if any target table is missing, telling the operator to run `create_tables.sql` first — the schema is declaratively owned by SQL, not by Spark inference.

### 6️⃣ Orchestrate  
The entire pipeline can be run end‑to‑end with `python -m etl.load`, which calls into `transform.main()`, which calls into `clean.main()`, which calls into `extract.main()` — composable modules where each can also be run independently for debugging.

---

# 🧩 Custom Logging & Observability

I built a custom `logger.py` because real pipelines need more than `print()` statements. The system includes:

- **`@timed` decorator** that wraps any function and logs its execution time (e.g. `⏱️ Step completed in 1m 16s`)  
- **`section(name)` helper** that prints visual banners between pipeline stages so log files are scannable  
- **`setup_logging()`** that configures a deterministic handler set with consistent formatting across modules  
- **UTF‑8 safe file logging** with emoji support for visual log scanning  
- **Before/after counts** at every cleaning and validation step

This gives me full observability — a single `pipeline.log` file tells the whole story of any run, with timestamps, row counts, and stage durations.

---

# 🔑 Key Engineering Decisions  

### 1️⃣ Why SHA‑256 surrogate keys instead of `monotonically_increasing_id()`?  
Spark's built‑in monotonic ID is **not deterministic across runs** — it depends on partition layout. Same input data produces different IDs on each run, which would break the LEFT JOIN merge in `load.py` (every row would look "new"). Switching to `sha2(concat_ws('|', ...natural_key_cols), 256)` makes the surrogate key a deterministic function of the row's content — same row in, same key out, forever.

### 2️⃣ Why DECIMAL(18,2) for money, not FLOAT or VARCHAR?  
Floats lose precision at scale (`0.1 + 0.2 != 0.3`). Strings prevent aggregation. DECIMAL preserves exactness AND enables `SUM(amount)`, `AVG(amount)`, etc. directly in SQL without casts. The same type flows from clean.py (`DecimalType(18,2)`) through transform.py and lands in Postgres as `NUMERIC(18,2)` — money stays money through the entire pipeline.

### 3️⃣ Why VARCHAR(64) for surrogate keys?  
SHA‑256 in hex form is always exactly 64 characters. `VARCHAR(64)` documents this contract to anyone reading the schema and constrains the column to its real shape, rather than allowing arbitrary string lengths.

### 4️⃣ Why is DDL owned by SQL, not auto‑created by Spark?  
Spark's JDBC writer can auto‑create tables, but it auto‑infers types (often suboptimally) and **discards PRIMARY KEYs, FOREIGN KEYs, and indexes entirely**. By owning the schema in `create_tables.sql`, I get full control over types, constraints, and read‑performance indexes — and the ETL becomes a pure data‑movement layer with no DDL concerns mixed in.

### 5️⃣ Why a separate `explore.py` stage?  
Knowing your data is part of the job. Profiling row counts, null distributions, and distinct values catches problems before they propagate downstream. Most pipelines bury this in commented‑out notebook cells; I made it a first‑class step.

### 6️⃣ Why fail hard rather than auto‑recover?  
The pipeline raises `SystemExit(1)` on critical errors (missing env vars, missing target tables, JDBC write failures). **Silent recovery is the enemy of observability** — a pipeline that "kind of worked" is worse than one that loudly didn't, because nobody investigates a green log.

---

# 📊 By the Numbers  

| Metric | Value |
|---|---|
| Source rows | 1,000,000 |
| Source columns | 23 |
| Dimensions | 4 (`dim_customer`, `dim_transaction`, `dim_employee`, `dim_date`) |
| Fact tables | 1 (`fact_transactions`) |
| First‑run end‑to‑end | ~25 minutes (local Spark) |
| Idempotent re‑run | ~9 minutes (no inserts, just verification) |
| Duplicate rows on re‑run | 0 across all 5 tables |
| Foreign keys | 4 (fact → all 4 dimensions) |
| Indexes | 7 (5 on fact, 2 on dimensions) |
| Surrogate key algorithm | SHA‑256 over natural key tuple |
| Surrogate key length | 64 chars (hex) |

---

# ▶️ Running the Pipeline  

### 1. Install dependencies  
```
pip install -r requirements.txt
```

### 2. Configure environment  
Create a `.env` file in the project root:
```
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=fibbie_bank_db
```

### 3. Create the warehouse schema  
```
psql -U postgres -d fibbie_bank_db -f sql/create_tables.sql
```
This drops any existing tables and recreates the full star schema with PKs, FKs, and indexes.

### 4. Run the full pipeline  
```
python -m etl.load
```
The orchestration is chained — `load` calls `transform`, which calls `clean`, which calls `extract`.

### 5. Verify idempotency  
Run it again:
```
python -m etl.load
```
You should see four `ℹ️ No new rows to insert` log lines (one per dimension + fact) — proof that the deterministic surrogate keys are working.

---

# 🧪 Running Individual Stages  

Each module can be run independently for debugging:

```
python -m etl.extract     # Just read the CSV + show schema
python -m etl.explore     # Profile the data (nulls, distinct values, counts)
python -m etl.clean       # Run extract + clean
python -m etl.transform   # Run extract + clean + transform
python -m etl.load        # Full pipeline end to end
```

---

# 🔮 Future Iterations  

If I were continuing to evolve this, the next things I'd ship:

- **SCD Type 2 on dim_customer** with `valid_from` / `valid_to` columns to track customer attribute changes over time  
- **dbt layer** on top of the warehouse for analyst‑authored transformations and tested SQL models  
- **Airflow / Prefect orchestration** so the pipeline runs on a schedule with proper retry / alerting semantics  
- **Parallelised JDBC writes** via Spark's `numPartitions` + `partitionColumn` for faster cold loads at higher scales  
- **Migration tooling (Alembic or Flyway)** for evolving the DDL rather than relying on `DROP + CREATE`  
- **Unit tests for transform logic** using `pyspark.sql.SparkSession.builder.master("local[2]")` with small fixtures

---

# 🤝 Contributing  

If you'd like to contribute, feel free to:

1. Fork the repo  
2. Create a feature branch  
3. Commit changes  
4. Open a pull request  

---

# 📄 License  

This project is released under the **MIT License**.

---

# 👤 Author  

**Yomi Ismail**  
Data Engineer · Suffolk, UK  
[LinkedIn](https://www.linkedin.com/in/yomi-ismail) · [GitHub](https://github.com/yoismail) · [Portfolio](https://yoismail.github.io/portfolio/)

---
