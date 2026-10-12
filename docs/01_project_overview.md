# 01. Project Overview

## Executive Summary

The **Weather Data Engineering Pipeline** is an enterprise-grade, end-to-end Extract, Transform, Load (ETL) and Data Warehouse solution engineered to ingest live meteorological telemetry from global meteorological APIs (OpenWeatherMap REST API), validate observation data structures, transform multi-dimensional nested JSON payloads using **Apache Spark (PySpark)**, stage atomic measurements, and load a dimensional **Star Schema Data Warehouse** hosted in **PostgreSQL**.

Built with idempotency, fault-tolerance, and scalable analytics in mind, the pipeline enables historical meteorological trend analysis, cross-city climate comparisons, microclimate anomaly detection, and business intelligence reporting.

---

## Business & Engineering Objectives

```
+----------------------------------------------------------------------------------------------------+
|                                    PROJECT CORE OBJECTIVES                                         |
+------------------------------------+---------------------------------------------------------------+
| Objective                          | Technical Implementation                                      |
+------------------------------------+---------------------------------------------------------------+
| Real-time Telemetry Ingestion      | Resilient HTTP extraction with timeouts and error handling    |
| Schema & Data Integrity Assurance  | Pre-ingestion rule validation (lat/lon, pressure, temp)       |
| Scalable Data Transformation       | Apache Spark multi-line JSON parsing and type-safe casting    |
| Dimensional Data Warehousing       | Kimball Star Schema: surrogate keys, dim tables, fact table   |
| Idempotent Incremental Loading     | Spark LEFT ANTI JOINs guaranteeing zero duplicate records      |
| Containerized Reproducibility      | Multi-service Docker & Docker-Compose deployment              |
| Automated CI/CD & Testing          | Unit test suite, Windows/Linux runners, GitHub Actions CI     |
+------------------------------------+---------------------------------------------------------------+
```

---

## Architectural Highlights

- **Raw Data Lake Layer**: Raw extracted API responses are persisted with millisecond timestamps in `data/raw/*.json` to ensure full auditability, re-processability, and lineage recovery.
- **Spark Engine Compute**: In-memory distributed transformation of deeply nested JSON hierarchies into relational columnar records.
- **Staging Table (`weather_data`)**: Transient and historical buffer capturing full weather snapshots.
- **Star Schema Warehouse**:
  - `dim_city`: Auto-incrementing surrogate keys (`city_id`), unique city names, and coordinates.
  - `dim_weather`: Unique condition categorizations (`weather_id`, `weather`, `description`).
  - `fact_weather`: Grain of one measurement per city per timestamp linking dimensions with numeric metrics (temperature, humidity, atmospheric pressure, wind speed).
- **Idempotency Strategy**: Uses `LEFT ANTI JOIN` against target dimension and fact tables to eliminate duplicate insertions during retries or backfills.

---

## Technology Stack

| Layer | Technology | Version / Specification | Role |
| :--- | :--- | :--- | :--- |
| **Data Source** | OpenWeatherMap API | 2.5 REST JSON | Live real-time global weather observations |
| **Ingestion** | Python `requests` | 2.34+ | HTTP client with connection pooling and timeouts |
| **Data Validation** | Custom Python Engine | Pure Python 3.10+ | Range checks, mandatory field checks, coordinate bounding |
| **Distributed Compute**| Apache Spark (PySpark) | 4.2+ | In-memory distributed DataFrame processing and JDBC I/O |
| **Data Warehouse** | PostgreSQL | 15+ / 16+ | ACID relational storage, star schema tables, indexed views |
| **Database Driver** | PostgreSQL JDBC | 42.7.13 | High-performance Java database connectivity for Spark |
| **Containerization** | Docker & Compose | Compose v2 | Isolated PostgreSQL container & reproducible runtimes |
| **Testing** | Unittest & Pytest | Python Standard / 8.x | Automated unit testing & end-to-end integration tests |
| **Automation** | PowerShell & Batch | Core 7+ / CMD | 1-click execution scripts for local and server environments |

---

## Project Repository Layout

```
weather_etl_api/
├── .github/workflows/ci.yml       # Automated GitHub Actions CI workflow
├── config/
│   ├── __init__.py
│   └── settings.py               # Central environment configuration & JDBC settings
├── data/
│   └── raw/                      # Raw JSON lake storage (persisted by extract.py)
├── database/
│   └── schema.sql                # Complete PostgreSQL DDL (staging, dims, fact, views)
├── docs/                         # 10-part comprehensive engineering documentation
│   ├── 01_project_overview.md
│   ├── 02_architecture_and_design.md
│   ├── 03_data_dictionary_and_schema.md
│   ├── 04_setup_and_installation_guide.md
│   ├── 05_pipeline_execution_guide.md
│   ├── 06_data_quality_and_validation_framework.md
│   ├── 07_sql_analytics_and_warehouse_queries.md
│   ├── 08_testing_and_qa_strategy.md
│   ├── 09_deployment_and_orchestration.md
│   └── 10_contributing_and_developer_guide.md
├── drivers/
│   └── postgresql-42.7.13.jar    # Certified PostgreSQL JDBC driver
├── logs/
│   └── pipeline.log              # Structured timestamped runtime logs
├── scripts/
│   ├── __init__.py
│   ├── extract.py                # REST API extraction & validation
│   ├── transform.py              # PySpark flattening & type conversion
│   ├── load.py                   # PySpark JDBC staging table writer
│   ├── logger.py                 # Centralized logging subsystem
│   ├── main.py                   # Master pipeline orchestrator
│   ├── validator.py              # Defensive data quality validator
│   └── warehouse/
│       ├── __init__.py
│       ├── database.py           # Centralized Spark JDBC read/write utilities
│       ├── dim_city.py           # Incremental City Dimension loader (LEFT ANTI JOIN)
│       ├── dim_weather.py        # Incremental Weather Dimension loader (LEFT ANTI JOIN)
│       └── fact_weather.py       # Fact table join resolver & loader
├── tests/
│   ├── test_config.py            # Configuration unit tests
│   ├── test_dim_city.py          # City dimension loader integration tests
│   ├── test_dim_weather.py       # Weather dimension loader integration tests
│   ├── test_fact_weather.py      # Fact table loader integration tests
│   ├── test_transform.py         # PySpark transformation schema unit tests
│   └── test_validator.py         # Data quality boundary tests
├── .env.example                  # Environment configuration template
├── .gitignore                    # Git exclusions
├── docker-compose.yml            # PostgreSQL container definition
├── Dockerfile                    # Container image definition
├── pyproject.toml                # Standard Python package metadata & test settings
├── README.md                     # Central project presentation
├── requirements.txt              # Pinned Python package dependencies
├── run_pipeline.bat              # 1-click Windows Batch execution script
└── run_pipeline.ps1              # 1-click PowerShell execution script
```
