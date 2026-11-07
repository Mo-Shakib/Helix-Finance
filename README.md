# Helix Finance — Mini Data Warehouse & ETL Pipeline

A lightweight, end-to-end financial data warehouse portfolio project: four intentionally messy operational CSV feeds are cleaned by a Python/Pandas ETL pipeline, loaded into a PostgreSQL star schema with SQLAlchemy, validated inside the load transaction, and explored through a Streamlit control room.

No real customer or financial data is used. Every row is synthetic and generated deterministically from seed `20261108`.

## Dashboard preview

![Helix Finance data warehouse dashboard](docs/dashboard.png)

## What the demo proves

| Capability | Implementation |
|---|---|
| Multi-source ingestion | Four separate CSV feeds: CRM customers, product catalog, loan core, payment ledger |
| Data cleaning | Missing values, duplicate updates, whitespace/casing, currency text, mixed date formats, mixed category casing |
| Dimensional model | `dim_date`, `dim_customer`, `dim_product`, `dim_loan` + two fact tables |
| Warehouse load | PostgreSQL via SQLAlchemy Core and Pandas, wrapped in one atomic transaction |
| Data validation | 24 post-load checks: uniqueness, referential integrity, row counts, amount reconciliation, quality warnings |
| Rejected-row audit | Every quarantined row keeps its source, source row number, reason, and original JSON payload |
| Observability | Run history, per-source counts, stage timings, validation results, rejection analytics |
| Analytics | Six named SQL reports for portfolio, origination, transaction, customer, status, and segment analysis |
| Dashboard | Streamlit UI to run the pipeline and inspect every stage of the result |
| Documentation | Architecture, setup, execution, data dictionary, and SQL library |
| Testing | 25 pytest tests covering parsers, transforms, PostgreSQL-compatible schema behavior, idempotency, analytics, and rollback |

## At a glance

With the checked-in seed data:

| Metric | Result |
|---|---:|
| Raw rows from 4 sources | 1,713 |
| Rows retained after cleaning | 1,659 |
| Rows quarantined | 54 |
| Dimension + fact rows loaded | 3,704 |
| Validation checks | 24 |
| Critical failures | 0 |
| Quality warnings | 3 |

The three warnings are deliberate and explain policy decisions: placeholder emails, unknown loan statuses, and unknown transaction statuses. They do not block a load. Referential-integrity, uniqueness, row-count, and amount-reconciliation checks all pass.

## Architecture

```mermaid
flowchart LR
    subgraph SRC["Operational sources"]
        CRM["crm_customers.csv"]
        PRD["product_catalog.csv"]
        LOAN["loan_core.csv"]
        TXN["payment_ledger.csv"]
    end

    subgraph ETL["Python + Pandas ETL"]
        EX["Extract<br/>typed CSV reads"]
        CL["Clean & normalize<br/>IDs, dates, amounts, categories"]
        DD["De-duplicate<br/>keep latest valid record"]
        DIM["Build dimensions<br/>+ calendar"]
        FACT["Build facts<br/>surrogate keys"]
        VAL["Validation gate<br/>24 checks"]
    end

    subgraph PG["PostgreSQL star schema"]
        D["dim_date · dim_customer<br/>dim_product · dim_loan"]
        F["fact_loan_originations<br/>fact_transactions"]
        A["ETL audit tables<br/>runs · rejects · validations"]
    end

    UI["Streamlit control room<br/>KPIs · rejects · validations · SQL"]

    CRM --> EX
    PRD --> EX
    LOAN --> EX
    TXN --> EX
    EX --> CL --> DD --> DIM --> FACT --> VAL
    VAL -->|"critical FAIL: rollback"| A
    VAL -->|"PASS / WARN"| D
    VAL -->|"PASS / WARN"| F
    DD -->|"quarantine"| A
    D --> UI
    F --> UI
    A --> UI
```

### ETL behavior

1. **Extract** all four CSVs as strings so IDs such as `CUST-0001` are never coerced to numbers.
2. **Normalize** identifiers, names, emails, countries, segments, number strings, percentages, and dates.
3. **Validate row-level required fields** and reject bad records with a stable reason code.
4. **De-duplicate** by natural key. The latest business timestamp wins (source row order breaks ties); superseded rows are quarantined.
5. **Resolve foreign keys** against the cleaned customer and product dimensions. Orphans are rejected, not silently dropped.
6. **Build the date dimension** spanning all customer signup, loan origination, and transaction dates.
7. **Assign surrogate keys**, then build both fact frames.
8. **Replace the warehouse atomically** in a single SQLAlchemy transaction and run 24 post-load validation checks.
9. **Persist the audit trail** whether the run succeeds or fails.

### Star schema

```
                 dim_date
                    │
dim_customer ── fact_loan_originations ── dim_product
      │                    │
      │                 dim_loan
      │                    │
      └────────── fact_transactions ──── dim_date
```

| Table | Grain |
|---|---|
| `dim_date` | One row per calendar day |
| `dim_customer` | One de-duplicated customer |
| `dim_product` | One product code |
| `dim_loan` | One de-duplicated loan contract |
| `fact_loan_originations` | One originated loan |
| `fact_transactions` | One transaction event |

Full column-level documentation, types, constraints, and business rules are in the [data dictionary](docs/data_dictionary.md).

## Project structure

```text
.
├── app.py                          # Streamlit dashboard
├── docker-compose.yml              # PostgreSQL 16 service
├── Makefile                        # setup/run/test shortcuts
├── requirements.txt
├── requirements-dev.txt
├── .env.example
├── data/raw/
│   ├── crm_customers.csv           # 234 rows
│   ├── product_catalog.csv         # 10 rows
│   ├── loan_core.csv               # 296 rows
│   ├── payment_ledger.csv          # 1,173 rows
│   └── _manifest.json              # seed, counts, intentional issues
├── docs/
│   └── data_dictionary.md
├── finance_dwh/
│   ├── analytics.py                # SQL library parser/executor
│   ├── config.py                   # env-backed settings
│   ├── generate_data.py            # deterministic synthetic data generator
│   ├── pipeline.py                 # ETL orchestration + CLI
│   ├── quality.py                  # parsing/normalization helpers
│   ├── schema.py                   # SQLAlchemy star schema + audit tables
│   ├── transforms.py               # cleaning, dedupe, dimension/fact frames
│   ├── validation.py               # post-load validation checks
│   └── warehouse.py                # atomic load + audit persistence
├── sql/
│   └── analytics_queries.sql       # six named analytical reports
└── tests/
    ├── test_pipeline.py
    └── test_quality.py
```

## Quick start

### Prerequisites

- Python **3.11+**
- Docker Desktop (or a local PostgreSQL 16+ server)
- `make` is optional

### 1. Start PostgreSQL

```bash
cp .env.example .env      # optional: defaults already match docker-compose.yml
docker compose up -d
docker compose ps         # wait for the healthcheck to report healthy
```

The container publishes PostgreSQL on host port **5433** to avoid clashes with a local PostgreSQL on 5432. The database name, user, and password are `finance_dwh`, `dwh_user`, and `dwh_password`.

The Compose stack intentionally contains only PostgreSQL. The sample datasets are versioned under [`data/raw/`](data/raw/) and are read by the Python ETL, keeping the container footprint minimal.

### 2. Install Python dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

On Windows, activate with `.venv\Scripts\activate`.

### 3. Generate the synthetic sources (optional)

The generated CSVs are committed, so this step is only needed if you want to regenerate them:

```bash
python -m finance_dwh.generate_data
```

The generator overwrites `data/raw/*.csv` with the same deterministic data and writes a manifest describing the intentional defects.

### 4. Run the ETL pipeline

```bash
python -m finance_dwh.pipeline
```

Expected result with the checked-in seed:

```text
[100%] Pipeline completed successfully.

Pipeline result
---------------
Run ID:              1
Status:              SUCCESS
Rows extracted:      1,713
Rows transformed:    1,659
Rows rejected:       54
Rows loaded:         3,704
Validation failures: 0
Validation warnings: 3
```

### 5. Launch the dashboard

```bash
streamlit run app.py
```

Open **http://localhost:8501**. The dashboard can also run the pipeline from the sidebar, so steps 4 and 5 can be reversed.

### Makefile shortcuts

```bash
make install      # create .venv and install runtime dependencies
make up           # start PostgreSQL
make generate     # regenerate synthetic data
make etl          # run the pipeline
make dashboard    # run Streamlit
make test         # run pytest
make down         # stop PostgreSQL
```

Typical first run:

```bash
make setup && make up && make etl && make dashboard
```

## Dashboard tour

| Tab | What it shows |
|---|---|
| **Overview** | Run status, source/accept/reject/load KPIs, validation pass rate, source-to-warehouse reconciliation, warehouse inventory, rejection mix, stage timings, run history |
| **Rejections** | Rejection rate, duplicate vs invalid split, reason charts, raw quarantined payload inspector, CSV download |
| **Validation** | PASS/WARN/FAIL counts, check-level expected vs actual values, policy explanations, blockers vs warnings |
| **Warehouse** | Live row counts and previews for every dimension and fact, CSV download |
| **Analytics** | Six SQL reports with charts and a "View SQL" panel, result downloads |
| **Data dictionary** | Rendered table/column documentation with download |

Use the sidebar to run a new pipeline execution or inspect a historical run. Each execution is persisted, so the dashboard remains a complete run-history view rather than a transient job log.

## Data quality rules

| Scenario | Handling |
|---|---|
| Missing customer ID / name / signup date | Reject with a specific code |
| Missing or malformed email | Deterministic placeholder + `email_imputed = TRUE` warning |
| Missing country / segment | Default to `Unknown` / `Retail` |
| Invalid customer date of birth | Store `NULL`, retain customer |
| Duplicate customer / product / loan / transaction | Keep latest valid source record, quarantine superseded rows |
| Mixed date formats | Parse ISO, day-first, month-first fallback, abbreviated month, and timestamp variants |
| Currency and percentage text | Strip `$`, `€`, `£`, commas, spaces, and `%` before numeric conversion |
| Loan/transaction orphan foreign key | Reject before load |
| Unknown loan or transaction status | Canonicalize to `unknown` and raise a non-blocking warning |
| Blocking validation failure | Roll back the entire warehouse replacement, retain the previous snapshot |

Rejection reason codes and validation checks are catalogued in the [data dictionary](docs/data_dictionary.md#8-rejection-reason-codes).

## Analytical reporting

The named SQL queries in [`sql/analytics_queries.sql`](sql/analytics_queries.sql) are parsed and rendered in the dashboard. They answer:

1. Portfolio exposure and rate profile by product.
2. Monthly origination trends.
3. Monthly transaction volume, failures, and success rate.
4. Top customers by successful payment volume.
5. Loan book composition by status.
6. Customer segment performance.

Example: portfolio by product

```sql
SELECT
    p.product_name,
    COUNT(*) AS loan_count,
    ROUND(SUM(f.principal_amount), 2) AS total_principal,
    ROUND(SUM(CASE WHEN f.loan_status = 'active' THEN f.principal_amount ELSE 0 END), 2) AS active_principal,
    ROUND(AVG(f.interest_rate), 2) AS avg_interest_rate_pct
FROM fact_loan_originations AS f
JOIN dim_product AS p ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_principal DESC;
```

You can run the same file directly in `psql` after splitting by named blocks, or copy any query into your SQL client.

## Testing

```bash
pip install -r requirements-dev.txt
pytest -q
```

The suite uses a temporary SQLite database for fast, Docker-free execution while exercising the same SQLAlchemy schema and load logic. It verifies:

- Mixed date and number parsing.
- Normalization and validation helpers.
- Full ETL success, exact row counts, and audit persistence.
- Repeatable, idempotent full refreshes.
- Blocking validation failure rollback.
- All six analytical SQL reports.

PostgreSQL is exercised by the real pipeline and dashboard in the quick-start flow. The PostgreSQL-specific load path is the same SQLAlchemy transaction used by the test suite.

## Configuration

Environment variables are loaded from `.env` through `python-dotenv`.

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `postgresql+psycopg2://dwh_user:dwh_password@localhost:5433/finance_dwh` | SQLAlchemy database URL |
| `POSTGRES_DB` | `finance_dwh` | Compose database name |
| `POSTGRES_USER` | `dwh_user` | Compose user |
| `POSTGRES_PASSWORD` | `dwh_password` | Compose password |
| `POSTGRES_PORT` | `5433` | Host port published by Compose |
| `DATA_DIR` | `./data/raw` | Raw CSV directory |

If you change `POSTGRES_PORT`, update `DATABASE_URL` to match. To use an existing PostgreSQL server instead of Docker:

```bash
export DATABASE_URL="postgresql+psycopg2://<user>:<password>@localhost:5432/<database>"
python -m finance_dwh.pipeline
```

The pipeline creates all tables automatically; no manual DDL is required.

## Design decisions and trade-offs

- **Full refresh, not incremental.** The dataset is intentionally small. A single atomic delete-and-reload makes idempotency and audit simplicity more valuable than incremental complexity.
- **Validation inside the load transaction.** Blocking checks run before commit, so a bad publication never replaces a known-good warehouse.
- **Separate analytical and audit schemas.** Star-schema tables are disposable; audit tables append forever and are never truncated.
- **Surrogate keys assigned per run.** Natural keys remain stable, but surrogate keys are not durable across full refreshes. This keeps the model simple for a demo and is documented for analysts.
- **Pandas + SQLAlchemy, no orchestration framework.** The goal is to make ETL mechanics legible. Airflow, dbt, or Spark would add operational surface area without improving the demo.
- **Warnings are visible but non-blocking.** Imputed emails and unknown category values are policy decisions, not silent data loss. The UI distinguishes them from integrity failures.
- **SQLite for tests, PostgreSQL for the product.** Fast tests do not require Docker, while the runtime path uses psycopg2 and PostgreSQL.

## Limitations / next steps

- No incremental loads, CDC, or late-arriving dimension handling.
- Single synthetic currency; no FX conversion.
- No authentication or row-level security on the dashboard.
- Defect injection is generated in code, not replayed from a production sample.
- Natural production extensions: dbt models, Airflow scheduling, Great Expectations/Soda checks, Slowly Changing Dimensions (SCD2), partitioning, and a BI semantic layer.

## Useful commands

```bash
# Connect to the Compose database
docker compose exec postgres psql -U dwh_user -d finance_dwh

# Inspect run history
psql "postgresql://dwh_user:dwh_password@localhost:5433/finance_dwh" \
  -c "SELECT run_id, status, extracted_rows, rejected_rows, loaded_rows FROM etl_pipeline_runs ORDER BY run_id DESC;"

# Stop the database
docker compose down

# Remove the database volume as well
docker compose down -v
```

## Documentation

- [Data dictionary](docs/data_dictionary.md)
- [Analytics SQL library](sql/analytics_queries.sql)
- [Source generation manifest](data/raw/_manifest.json)

---

Built with Python, Pandas, PostgreSQL, SQLAlchemy, and Streamlit. All data is synthetic and safe to share.
