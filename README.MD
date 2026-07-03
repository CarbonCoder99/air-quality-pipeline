# 🌍 Global Air Quality Pipeline

A production-grade data engineering pipeline that ingests daily air quality
measurements from 15 countries, transforms them into analytics-ready models,
and orchestrates everything automatically — built to demonstrate end-to-end
data engineering competency.

---

## The Problem

Every day, millions of people make decisions blind to the quality of the air
around them. Governments set environmental policy. City planners decide where
to build schools. Public health officials track respiratory disease trends.
All of these are data decisions — and the data exists, scattered across
thousands of monitoring stations worldwide, uncleaned and unconnected.

This pipeline solves that. It collects, standardises, and models air quality
data from across the globe into a single analytics-ready dataset that can
power dashboards, reports, and public health research.

---

## Architecture

OpenAQ API v3
│
│  Airflow DAG 1 — daily 06:00 UTC
▼
GCS Data Lake
measurements/date=YYYY-MM-DD/country=XX/data.json
│
│  Airflow DAG 2 — daily 08:00 UTC
▼
BigQuery Raw Layer
air_quality_raw.raw_measurements
│
│  dbt — staging models
▼
BigQuery Staging Layer
air_quality_staging.stg_measurements
air_quality_staging.stg_locations
│
│  dbt — mart models
▼
BigQuery Mart Layer
air_quality_mart.fct_daily_measurements  ← incremental, partitioned, clustered
air_quality_mart.dim_locations           ← enriched with continent + region
air_quality_mart.dim_dates               ← full date spine

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Infrastructure | Terraform | Provision GCS bucket + BigQuery datasets |
| Orchestration | Apache Airflow 2.9 (CeleryExecutor) | Schedule and monitor pipeline |
| Data Lake | Google Cloud Storage | Raw NDJSON landing zone |
| Data Warehouse | Google BigQuery | Partitioned + clustered tables |
| Transformation | dbt-bigquery 1.8 | Staging models, mart models, tests |
| Containerisation | Docker + Docker Compose | Reproducible local environment |
| Language | Python 3.11 | Ingestion scripts and DAGs |

---

## Data Coverage

**15 countries across 6 continents:**

| Country | Continent | Notes |
|---|---|---|
| United States | North America | Dense sensor network |
| United Kingdom | Europe | Urban + rural mix |
| Germany | Europe | Industrial + residential |
| France | Europe | |
| India | Asia | High pollution, many sensors |
| China | Asia | Industrial context |
| Japan | Asia | High data quality |
| Brazil | South America | Urban + forest fire smoke |
| Mexico | North America | Urban pollution |
| South Africa | Africa | Mining + industrial |
| Nigeria | Africa | West Africa representation |
| Kenya | Africa | East Africa representation |
| Australia | Oceania | Bushfire smoke context |
| Pakistan | Asia | Historically poor air quality |
| Indonesia | Southeast Asia | Forest fires + urban |

**6 core pollutants tracked:**
PM2.5 · PM10 · NO2 · O3 · CO · SO2

---

## dbt Models

### Staging Layer (views)
| Model | Description |
|---|---|
| `stg_measurements` | Cleaned raw measurements — nulls removed, negatives filtered, types cast |
| `stg_locations` | Deduplicated monitoring stations |

### Mart Layer (incremental tables)
| Model | Description |
|---|---|
| `fct_daily_measurements` | One row per sensor per day. Includes AQI category + 7-day rolling average |
| `dim_locations` | Station dimension enriched with continent and region |
| `dim_dates` | Full date spine from 2024-01-01 to today |

### dbt Features Demonstrated
- ✅ Incremental models with late-arriving data handling
- ✅ Partitioning by `measurement_date` (query cost optimisation)
- ✅ Clustering by `country_iso` + `pollutant` (scan optimisation)
- ✅ Custom macro (`aqi_category`) based on US EPA breakpoints
- ✅ `dbt_utils.generate_surrogate_key` for unique row identification
- ✅ `dbt_utils.date_spine` for complete date dimension
- ✅ Schema tests: `unique`, `not_null`, `accepted_values`
- ✅ Source freshness definitions
- ✅ Seed file for country metadata

---

## Pipeline Design Decisions

**Why GCS before BigQuery?**
Raw files in GCS act as an immutable archive. If our parsing logic in dbt
ever changes, we can reprocess from the raw files without re-calling the API.
This is the medallion architecture pattern.

**Why Hive-partitioned paths in GCS?**
`measurements/date=2024-11-01/country=US/data.json` — the `date=` and
`country=` prefixes are recognised by BigQuery as virtual partition columns,
making bulk loads from GCS more efficient.

**Why incremental dbt models?**
We add new data every day. Reprocessing all historical data daily would be
wasteful and expensive. Incremental models only process new partitions,
keeping BigQuery costs low and run times fast.

**Why CeleryExecutor in Airflow?**
The scheduler and workers are separate processes — the scheduler queues tasks,
workers execute them. This mirrors how Airflow runs in production on Cloud
Composer or MWAA, making this setup more realistic than LocalExecutor.

**Why two DAGs instead of one?**
Separating ingestion from transformation isolates failures. If the OpenAQ API
goes down, only DAG 1 fails — DAG 2 detects this via ExternalTaskSensor and
skips gracefully. BigQuery data stays consistent.

---

## Project Structure

air-quality-pipeline/
├── terraform/              # GCP infrastructure as code
│   ├── main.tf             # GCS bucket + BigQuery datasets
│   ├── variables.tf
│   └── outputs.tf
├── airflow/
│   ├── dags/
│   │   ├── ingest_openaq.py        # DAG 1: API → GCS
│   │   └── load_and_transform.py   # DAG 2: GCS → BQ → dbt
│   └── utils/
│       ├── openaq_client.py        # OpenAQ API wrapper
│       └── bigquery_client.py      # BigQuery load utilities
├── dbt/
│   ├── models/
│   │   ├── staging/        # stg_measurements, stg_locations
│   │   └── marts/          # fct_daily_measurements, dim_*
│   ├── macros/
│   │   └── aqi_category.sql  # EPA AQI breakpoints macro
│   └── seeds/
│       └── countries.csv     # Country metadata with continents
├── exploration/
│   └── explore_openaq.py   # API discovery script (not part of pipeline)
├── Dockerfile              # Custom Airflow image with dbt + GCP
├── docker-compose.yml      # Full stack: Airflow + Postgres + Redis
└── Makefile                # Shortcut commands

---

## Getting Started

### Prerequisites
- Docker + Docker Compose
- GCP account with billing enabled
- OpenAQ API key (free at explore.openaq.org)
- Terraform

### 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/air-quality-pipeline.git
cd air-quality-pipeline
```

### 2. Configure environment
```bash
cp .env.example .env
# Fill in: GCP_PROJECT_ID, GCS_BUCKET_NAME, OPENAQ_API_KEY
# Generate Fernet key:
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3. Add GCP service account key
```bash
mkdir keys/
# Place your service account JSON at: keys/gcp-service-account.json
```

### 4. Provision GCP infrastructure
```bash
cd terraform/
terraform init
terraform apply -var="project_id=YOUR_PROJECT_ID"
```

### 5. Start the pipeline
```bash
make setup    # builds image, initialises DB, starts all services
```

### 6. Open Airflow UI

http://localhost:8090
Username: admin
Password: admin


### 7. Trigger a run
```bash
# Trigger ingestion for a specific date
docker compose exec airflow-scheduler bash -c "
  airflow dags trigger ingest_openaq \
    --exec-date '2024-11-01T00:00:00+00:00'
"
```

### Useful commands
```bash
make up               # start all services
make down             # stop all services
make logs             # tail all logs
make logs-worker      # tail worker logs only
make shell            # shell into worker container
make dbt-run          # run dbt models manually
make dbt-test         # run dbt tests manually
make status           # check container health
```

---

## What This Project Demonstrates

| Concept | Implementation |
|---|---|
| Infrastructure as Code | Terraform provisions all GCP resources |
| Workflow Orchestration | Airflow with CeleryExecutor, sensors, XCom |
| Data Lake Design | GCS with Hive-partitioned paths |
| Warehouse Optimisation | BigQuery partitioning + clustering |
| Data Modelling | Star schema with fact + dimension tables |
| Incremental Processing | dbt incremental models with late data handling |
| Data Quality | dbt tests, null/negative filtering, outlier removal |
| Reusable Logic | dbt macros for AQI categorisation |
| Containerisation | Docker Compose with multi-service orchestration |
| Secret Management | Environment variables, never hardcoded |
| Separation of Concerns | Ingestion DAG separate from transformation DAG |
| Defensive Programming | Empty API responses handled gracefully |

---

## Next Steps (Roadmap)

- [ ] Add Looker Studio dashboard connected to `air_quality_mart`
- [ ] Add streaming ingestion with Pub/Sub for near-real-time data
- [ ] Add dbt snapshots for slowly changing dimension on locations
- [ ] Deploy Airflow to Cloud Composer for fully managed orchestration
- [ ] Add Great Expectations for advanced data quality checks
- [ ] Add CI/CD pipeline with GitHub Actions for dbt model testing

---

## Course Context

Built as a capstone project while completing the
[DataTalks.Club Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp),
covering modules 1-4: containerisation, workflow orchestration,
data warehousing, and analytics engineering.

---

## Author

**Waziri** — Data Engineering Student
[GitHub](https://github.com/CarbonCoder99) ·
[LinkedIn](https://linkedin.com/in/waziri-olugbade-60627522a/)