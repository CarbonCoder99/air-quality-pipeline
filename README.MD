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
| Infrastructure | Terraform | Provision GCS bucket and BigQuery datasets |
| Orchestration | Apache Airflow 2.9 (CeleryExecutor) | Schedule and monitor pipeline |
| Data Lake | Google Cloud Storage | Raw NDJSON landing zone |
| Data Warehouse | Google BigQuery | Partitioned and clustered tables |
| Transformation | dbt-bigquery 1.8 | Staging models, mart models, tests |
| Containerisation | Docker + Docker Compose | Reproducible local environment |
| Language | Python 3.11 | Ingestion scripts and DAGs |

---

## Project Structure

    air-quality-pipeline/
    │
    ├── terraform/
    │   ├── main.tf                     # GCS bucket + BigQuery datasets
    │   ├── variables.tf                # Input variables
    │   └── outputs.tf                  # Output values after apply
    │
    ├── airflow/
    │   ├── dags/
    │   │   ├── ingest_openaq.py        # DAG 1: OpenAQ API → GCS
    │   │   └── load_and_transform.py   # DAG 2: GCS → BigQuery → dbt
    │   ├── utils/
    │   │   ├── openaq_client.py        # OpenAQ API wrapper with retry logic
    │   │   └── bigquery_client.py      # BigQuery load utilities
    │   └── requirements.txt            # Python dependencies for Airflow image
    │
    ├── dbt/
    │   ├── models/
    │   │   ├── staging/
    │   │   │   ├── sources.yml         # Raw BigQuery source definitions
    │   │   │   ├── schema.yml          # Staging model tests
    │   │   │   ├── stg_measurements.sql
    │   │   │   └── stg_locations.sql
    │   │   └── marts/
    │   │       ├── schema.yml          # Mart model tests
    │   │       ├── fct_daily_measurements.sql
    │   │       ├── dim_locations.sql
    │   │       └── dim_dates.sql
    │   ├── macros/
    │   │   └── aqi_category.sql        # EPA AQI breakpoints macro
    │   ├── seeds/
    │   │   └── countries.csv           # Country metadata with continents
    │   ├── dbt_project.yml
    │   ├── packages.yml
    │   └── profiles.yml
    │
    ├── exploration/
    │   ├── explore_openaq.py           # API discovery script (not part of pipeline)
    │   └── API_FINDINGS.md             # Documented API findings before build
    │
    ├── keys/                           # GCP service account key (gitignored)
    ├── Dockerfile                      # Custom Airflow image with dbt + GCP libs
    ├── docker-compose.yml              # Full stack: Airflow + Postgres + Redis
    ├── Makefile                        # Developer shortcut commands
    ├── .env.example                    # Environment variable template
    └── .gitignore

---

## Data Coverage

**15 countries across 6 continents:**

| Country | Continent | Notes |
|---|---|---|
| United States | North America | Dense sensor network |
| United Kingdom | Europe | Urban and rural mix |
| Germany | Europe | Industrial and residential |
| France | Europe | |
| India | Asia | High pollution, many sensors |
| China | Asia | Industrial context |
| Japan | Asia | High data quality |
| Brazil | South America | Urban and forest fire smoke |
| Mexico | North America | Urban pollution |
| South Africa | Africa | Mining and industrial |
| Nigeria | Africa | West Africa representation |
| Kenya | Africa | East Africa representation |
| Australia | Oceania | Bushfire smoke context |
| Pakistan | Asia | Historically poor air quality |
| Indonesia | Southeast Asia | Forest fires and urban pollution |

**6 core pollutants tracked:**
PM2.5 · PM10 · NO2 · O3 · CO · SO2

---

## dbt Models

### Staging Layer (views)

| Model | Description |
|---|---|
| stg_measurements | Cleaned raw measurements — nulls removed, negatives filtered, types cast |
| stg_locations | Deduplicated monitoring stations |

### Mart Layer (incremental tables)

| Model | Description |
|---|---|
| fct_daily_measurements | One row per sensor per day. Includes AQI category and 7-day rolling average |
| dim_locations | Station dimension enriched with continent and region from seed |
| dim_dates | Full date spine from 2024-01-01 to today with season attributes |

### dbt Features Demonstrated

- Incremental models with late-arriving data handling (3-day lookback window)
- Partitioning by measurement_date for query cost optimisation
- Clustering by country_iso and pollutant for scan optimisation
- Custom macro (aqi_category) based on US EPA AQI breakpoints
- dbt_utils.generate_surrogate_key for unique row identification
- dbt_utils.date_spine for complete date dimension
- Schema tests: unique, not_null, accepted_values
- Source definitions with column-level documentation
- Seed file for static country metadata

---

## Pipeline Design Decisions

**Why GCS before BigQuery?**
Raw files in GCS act as an immutable archive. If our parsing logic in dbt
ever changes, we can reprocess from the raw files without re-calling the API.
This is the medallion architecture pattern: raw → staging → mart.

**Why Hive-partitioned paths in GCS?**
The path pattern measurements/date=2024-11-01/country=US/data.json uses
Hive-style partitioning. BigQuery recognises these prefixes as virtual
partition columns, making bulk loads from GCS more efficient.

**Why incremental dbt models?**
We add new data every day. Reprocessing all historical data daily would be
wasteful and expensive in BigQuery. Incremental models only process new
partitions, keeping costs low and run times fast.

**Why CeleryExecutor in Airflow?**
The scheduler and workers are separate processes. This mirrors how Airflow
runs in production on Cloud Composer or MWAA, making this setup more
realistic than LocalExecutor.

**Why two DAGs instead of one?**
Separating ingestion from transformation isolates failures. If the OpenAQ
API goes down, only DAG 1 fails. DAG 2 detects this via ExternalTaskSensor
and skips gracefully. BigQuery data stays consistent.

**Why explore before building?**
Before writing any pipeline code, we ran an API exploration script
(exploration/explore_openaq.py) to understand the exact data structure,
identify sparse data countries, and discover that the API requires numeric
country IDs — not ISO string codes. Every pipeline design decision traces
back to that exploration.

---

## Getting Started

### Prerequisites

- Docker and Docker Compose
- GCP account with billing enabled
- OpenAQ API key (free at explore.openaq.org/register)
- Terraform

### 1. Clone the repository

    git clone https://github.com/YOUR_USERNAME/air-quality-pipeline.git
    cd air-quality-pipeline

### 2. Configure environment variables

    cp .env.example .env

Open .env and fill in:

    GCP_PROJECT_ID=your-gcp-project-id
    GCS_BUCKET_NAME=your-gcp-project-id-air-quality-raw
    OPENAQ_API_KEY=your-openaq-api-key
    AIRFLOW__CORE__FERNET_KEY=   # generate below
    AIRFLOW__WEBSERVER__SECRET_KEY=any-random-string

Generate a Fernet key:

    python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

### 3. Add your GCP service account key

    mkdir keys/
    # Place your service account JSON at: keys/gcp-service-account.json

### 4. Provision GCP infrastructure

    cd terraform/
    terraform init
    terraform apply -var="project_id=YOUR_PROJECT_ID"
    cd ..

### 5. Build and start the pipeline

    make setup

This builds the custom Docker image, initialises the Airflow database,
and starts all services.

### 6. Open the Airflow UI

    http://localhost:8090

    Username: admin
    Password: admin

### 7. Trigger a pipeline run

    docker compose exec airflow-scheduler bash -c "
      airflow dags trigger ingest_openaq \
        --exec-date '2024-11-01T00:00:00+00:00'
    "

### Useful Makefile commands

    make up                # start all services
    make down              # stop all services
    make logs              # tail all logs
    make logs-worker       # tail worker logs only
    make shell             # open shell inside worker container
    make dbt-run           # run dbt models manually
    make dbt-test          # run dbt tests manually
    make status            # check container health

---

## What This Project Demonstrates

| Concept | Implementation |
|---|---|
| Infrastructure as Code | Terraform provisions all GCP resources reproducibly |
| Workflow Orchestration | Airflow with CeleryExecutor, ExternalTaskSensor, XCom |
| Data Lake Design | GCS with Hive-partitioned paths per date and country |
| Warehouse Optimisation | BigQuery partitioning by date, clustering by country and pollutant |
| Data Modelling | Star schema: one fact table, two dimension tables |
| Incremental Processing | dbt incremental models with 3-day late data lookback |
| Data Quality | dbt schema tests, null filtering, negative value removal |
| Reusable SQL Logic | dbt macro for EPA AQI category classification |
| Containerisation | Docker Compose orchestrating 6 services |
| Secret Management | Environment variables injected at runtime, never hardcoded |
| Separation of Concerns | Ingestion DAG fully decoupled from transformation DAG |
| Defensive Programming | Empty API responses handled without pipeline failure |
| Data Discovery | API explored and documented before any pipeline code written |

---

## Roadmap

- Add Looker Studio dashboard connected to air_quality_mart
- Add streaming ingestion with Pub/Sub for near-real-time updates
- Add dbt snapshots for slowly changing dimension tracking on locations
- Deploy Airflow to Cloud Composer for fully managed orchestration
- Add CI/CD pipeline with GitHub Actions to run dbt tests on every push
- Extend coverage to 50 countries

---

## Course Context

Built as a portfolio project while completing the
DataTalks.Club Data Engineering Zoomcamp, covering:

- Module 1: Containerisation with Docker
- Module 2: Workflow orchestration with Airflow
- Module 3: Data warehousing with BigQuery
- Module 4: Analytics engineering with dbt

---

## Author

**Waziri** — Data Engineering Student
[GitHub](https://github.com/CarbonCoder99) ·
[LinkedIn](https://linkedin.com/in/waziri-olugbade-60627522a/)