![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-4.x-orange?logo=apachespark)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue?logo=docker)
![CI](https://img.shields.io/badge/CI-GitHub%20Actions-success?logo=githubactions)
![License](https://img.shields.io/badge/License-MIT-green)

# 🌦️ Weather Data Engineering Pipeline & Star Schema Data Warehouse

A production-grade, end-to-end Weather ETL (Extract, Transform, Load) and Data Warehousing pipeline. It extracts real-time meteorological observations from the **OpenWeatherMap REST API**, enforces rigorous pre-ingestion data quality constraints, flattens multi-layered nested JSON using **Apache Spark (PySpark)**, writes observations into a **PostgreSQL** staging table via high-performance JDBC, and incrementally populates an audit-logged **Kimball Star Schema Data Warehouse** (`dim_city`, `dim_weather`, `fact_weather`) utilizing **LEFT ANTI JOINs** to guarantee absolute idempotency.

Built by **Arul G** with enterprise-level architecture, automated CI/CD, and Power BI / Tableau analytical readiness.

---

## 📚 Complete Engineering Documentation Suite

This project includes a comprehensive 10-chapter documentation library covering every aspect of the system:

| Chapter | Document Title | Primary Focus |
| :---: | :--- | :--- |
| **01** | [**Project Overview**](docs/01_project_overview.md) | Executive summary, business goals, feature matrix, and architecture layout |
| **02** | [**Architecture & Design**](docs/02_architecture_and_design.md) | Component diagrams, PySpark engine design, and Kimball Star Schema modeling |
| **03** | [**Data Dictionary & Schema**](docs/03_data_dictionary_and_schema.md) | DDL tables, types, surrogate keys, nullability, and analytical views |
| **04** | [**Setup & Installation Guide**](docs/04_setup_and_installation_guide.md) | Prerequisites (Python, JDK 17, PostgreSQL), venv setup, and Docker quickstart |
| **05** | [**Pipeline Execution Guide**](docs/05_pipeline_execution_guide.md) | 1-click execution (`.ps1`, `.bat`, CLI), stage-by-stage lifecycle, and outputs |
| **06** | [**Data Quality & Validation Framework**](docs/06_data_quality_and_validation_framework.md) | Geospatial bounding, atmospheric boundary rules, and deduplication logic |
| **07** | [**SQL Analytics & Warehouse Queries**](docs/07_sql_analytics_and_warehouse_queries.md) | Production views, window functions, moving averages, and heat index analytics |
| **08** | [**Testing & QA Strategy**](docs/08_testing_and_qa_strategy.md) | Unit test suite, PySpark in-memory tests, integration tests, and GitHub Actions CI |
| **09** | [**Deployment & Orchestration**](docs/09_deployment_and_orchestration.md) | Docker Compose, Windows Task Scheduler, Linux crontab, and Apache Airflow DAG |
| **10** | [**Contributing & Developer Guide**](docs/10_contributing_and_developer_guide.md) | Coding standards, adding new cities, PR checklist, and Git workflow |

*Additional Reference Docs:*
- [Architecture Technical Notes](docs/ARCHITECTURE.md) | [OpenWeather API Reference](docs/API_DOCUMENTATION.md) | [Operational & Maintenance Runbook](docs/OPERATIONAL_GUIDE.md) | [Changelog](docs/CHANGELOG.md)

---

## 🏗️ System Architecture & Data Flow

```
                                  ┌─────────────────────────┐
                                  │   OpenWeatherMap API    │
                                  └────────────┬────────────┘
                                               │ HTTP GET (REST)
                                               ▼
                                  ┌─────────────────────────┐
                                  │   scripts/validator.py  │  ◄── Boundary & Range Checks
                                  └────────────┬────────────┘
                                               │ Validated JSON
                                               ▼
                                  ┌─────────────────────────┐
                                  │     Local Data Lake     │  ◄── data/raw/*.json
                                  └────────────┬────────────┘
                                               │ Multi-line JSON
                                               ▼
                                  ┌─────────────────────────┐
                                  │   scripts/transform.py  │  ◄── PySpark Engine (Flatten & Cast)
                                  └────────────┬────────────┘
                                               │ Clean DataFrame
                                               ▼
                                  ┌─────────────────────────┐
                                  │    scripts/load.py      │  ◄── JDBC Writer (append)
                                  └────────────┬────────────┘
                                               │
                                               ▼
 ┌────────────────────────────────────────────────────────────────────────────────────────┐
 │                              PostgreSQL Data Warehouse                                 │
 │                                                                                        │
 │  ┌──────────────────────┐   LEFT ANTI JOIN   ┌──────────────────────┐                  │
 │  │ Staging: weather_data │ ─────────────────► │  Dimension: dim_city │ (city_id PK)     │
 │  └──────────┬───────────┘                    └──────────────────────┘                  │
 │             │                                                                          │
 │             │               LEFT ANTI JOIN   ┌──────────────────────┐                  │
 │             ├──────────────────────────────► │Dimension: dim_weather│ (weather_id PK)  │
 │             │                                └──────────────────────┘                  │
 │             ▼                                                                          │
 │     Surrogate Lookup & LEFT ANTI JOIN ────► ┌──────────────────────┐                  │
 │                                             │  Fact: fact_weather  │ (surrogate keys)  │
 │                                             └──────────────────────┘                  │
 └────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🗄️ Kimball Star Schema Model

```
                    ┌────────────────────────┐
                    │        dim_city        │
                    ├────────────────────────┤
                    │ city_id (PK)           │
                    │ city_name (UK)         │
                    │ latitude               │
                    │ longitude              │
                    │ created_at             │
                    └───────────┬────────────┘
                                │ 1
                                │
                                │ *
 ┌────────────────────────┐     │     ┌────────────────────────┐
 │      dim_weather       │     │     │      fact_weather      │
 ├────────────────────────┤     │     ├────────────────────────┤
 │ weather_id (PK)        │ 1   │   * │ fact_id (PK)           │
 │ weather                ├─────┴─────┤ city_id (FK)           │
 │ description            │           │ weather_id (FK)        │
 │ created_at             │           │ temperature            │
 └────────────────────────┘           │ humidity               │
                                      │ pressure               │
                                      │ wind_speed             │
                                      │ collected_at           │
                                      │ created_at             │
                                      └────────────────────────┘
```

---

## 🚀 Quickstart Guide

### 1. Prerequisites
- Python 3.10+
- Java JDK 17 (Eclipse Temurin or OpenJDK) with `JAVA_HOME` set
- PostgreSQL 15+ (Local or Docker)

### 2. Environment Setup
```powershell
# 1. Clone repository
git clone https://github.com/ARULKINT/weather_data_eng.git
cd weather_data_eng

# 2. Configure .env file
Copy-Item .env.example .env
# Edit .env with your OPENWEATHER_API_KEY and DB credentials

# 3. Initialize Python Virtual Environment
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 3. Database Initialization
```bash
# Initialize PostgreSQL schema
psql -h localhost -U postgres -d weather_db -f database/schema.sql
```
*(Or use Docker: `docker compose up -d postgres`)*

### 4. Run the Pipeline (1-Click)
```powershell
# Option A: PowerShell Turnkey Script
.\run_pipeline.ps1

# Option B: Windows Batch
run_pipeline.bat

# Option C: Direct Python
python -m scripts.main
```

---

## 🧪 Testing and Quality Assurance

Run the automated test suite covering unit validation and PySpark transformations:

```powershell
# 1. Fast unit tests (validator & settings)
python -m unittest tests/test_validator.py tests/test_settings.py

# 2. PySpark schema transformation unit tests
python -m unittest tests/test_transform.py

# 3. Complete test suite discovery
python -m unittest discover tests
```

---

## 📂 Repository File Tree

```
weather_etl_api/
├── .github/workflows/ci.yml       # GitHub Actions CI workflow
├── config/
│   ├── __init__.py
│   └── settings.py               # Central environment configuration
├── data/
│   └── raw/                      # Raw JSON data lake storage
├── database/
│   └── schema.sql                # Complete PostgreSQL DDL (staging, dims, fact, views)
├── docs/                         # 10-chapter engineering documentation library
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
│   ├── extract.py                # REST API extraction & validation
│   ├── transform.py              # PySpark flattening & type conversion
│   ├── load.py                   # PySpark JDBC staging table writer
│   ├── logger.py                 # Centralized logging engine
│   ├── main.py                   # Master pipeline orchestrator
│   ├── validator.py              # Pre-ingestion data quality validator
│   └── warehouse/
│       ├── database.py           # Reusable Spark JDBC helpers
│       ├── dim_city.py           # City Dimension incremental loader
│       ├── dim_weather.py        # Weather Dimension incremental loader
│       └── fact_weather.py       # Fact table surrogate key resolver & loader
├── tests/
│   ├── test_config.py            # Configuration tests
│   ├── test_dim_city.py          # City dimension integration test
│   ├── test_dim_weather.py       # Weather dimension integration test
│   ├── test_fact_weather.py      # Fact table integration test
│   ├── test_transform.py         # PySpark transformation unit test
│   └── test_validator.py         # Data quality validator unit test
├── .env.example                  # Environment configuration template
├── docker-compose.yml            # Multi-service container orchestration
├── Dockerfile                    # Container image definition
├── pyproject.toml                # Project metadata & test configuration
├── README.md                     # Central project presentation
├── requirements.txt              # Pinned Python package dependencies
├── run_pipeline.bat              # Turnkey Windows Batch execution script
└── run_pipeline.ps1              # Turnkey PowerShell execution script
```

---

## 👨‍💻 Author & Contact

**Arul G**  
- **Role**: Data Engineer
- **GitHub**: [@ARULKINT](https://github.com/ARULKINT)
- **Repository**: [ARULKINT/weather_data_eng](https://github.com/ARULKINT/weather_data_eng)

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).