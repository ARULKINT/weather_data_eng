![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![PySpark](https://img.shields.io/badge/PySpark-4.x-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Data%20Warehouse-blue)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue)
![License](https://img.shields.io/badge/License-MIT-green)

# 🌦️ Weather Data Engineering Pipeline & Star Schema Data Warehouse

A production-style, end-to-end Weather ETL (Extract, Transform, Load) and Data Warehousing pipeline. It extracts real-time weather data from the **OpenWeather REST API**, validates and normalizes raw JSON payloads, flattens and transforms data using **PySpark**, loads observations into a **PostgreSQL** staging table, and builds an incremental **Star Schema Data Warehouse** (`dim_city`, `dim_weather`, `fact_weather`).

Built by **Arul G** using Python, PySpark, PostgreSQL, Docker, and Power BI readiness, following modern Data Engineering principles.

---

## 📌 Table of Contents

- [📖 Project Overview](#-project-overview)
- [✨ Key Features](#-key-features)
- [🛠️ Tech Stack](#-tech-stack)
- [🏗️ System Architecture & Data Flow](#️-system-architecture--data-flow)
- [📂 Detailed Repository & File Structure](#-detailed-repository--file-structure)
- [🗄️ Database Schema & Star Schema Model](#️-database-schema--star-schema-model)
- [🔄 Pipeline Execution Workflow](#-pipeline-execution-workflow)
- [💻 Installation & Operational Guide](#-installation--operational-guide)
- [🐳 Docker Deployment](#-docker-deployment)
- [🧪 Testing & Quality Assurance](#-testing--quality-assurance)
- [📚 Supporting Documentation](#-supporting-documentation)
- [👨‍💻 Author & Acknowledgments](#-author--acknowledgments)
- [📄 License](#-license)

---

## 📖 Project Overview

The **Weather Data Engineering Pipeline** is engineered to automate the collection, validation, processing, and analytical structuring of weather data across major Indian urban hubs (`Chennai`, `Delhi`, `Mumbai`, `Bangalore`, `Hyderabad`, `Pune`, `Kolkata`).

### Why this project?
Raw API data is often nested, unstructured, and transient. This pipeline converts live JSON responses into an incremental, audit-logged Data Warehouse structured optimized for OLAP analytics, time-series aggregation, and Business Intelligence dashboards (e.g., Power BI, Tableau).

---

## ✨ Key Features

- 🌍 **Automated API Data Extraction**: Ingests real-time weather metrics from OpenWeather API.
- ✅ **Strict Schema & Range Validation**: Ensures latitude, longitude, temperature, humidity, pressure, and wind speed values are valid prior to storage.
- ⚡ **Scalable PySpark Data Transformation**: Handles multi-line JSON parsing, schema enforcement, column casting, and nested field flattening.
- 🗄️ **Incremental Star Schema Data Warehouse**: Implements **Left Anti Joins** to process dimension updates (`dim_city`, `dim_weather`) and fact observations (`fact_weather`) without duplicates.
- 📝 **Centralized Logging Engine**: Unified logger broadcasting to console and `logs/pipeline.log`.
- 🐳 **Full Containerization**: Dockerfile and Multi-Container Docker Compose setup with PostgreSQL 16.
- 📊 **BI & Analytics Ready**: Includes database views for daily aggregation and latest city observations.

---

## 🛠️ Tech Stack

| Category | Technology / Framework | Description |
|----------|------------------------|-------------|
| **Language** | Python 3.10 - 3.14 | Core pipeline scripting & automation |
| **Big Data Engine** | PySpark 4.x | Distributed data transformation & flattening |
| **Database / OLAP** | PostgreSQL 16 | Relational Data Warehouse (Staging + Star Schema) |
| **Database Driver** | PostgreSQL JDBC 42.7.13 | High-performance Java database connection |
| **Data Source** | OpenWeather API | Real-time weather data provider |
| **Containerization** | Docker & Docker Compose | Application containerization & DB orchestration |
| **Environment Mgmt** | `python-dotenv` | Environment variable management |
| **Visualization** | Power BI | Dashboard reporting integration |

---

## 🏗️ System Architecture & Data Flow

```
                                  ┌─────────────────────────┐
                                  │     OpenWeather API     │
                                  └────────────┬────────────┘
                                               │ HTTP GET
                                               ▼
                                  ┌─────────────────────────┐
                                  │   scripts/extract.py    │
                                  └────────────┬────────────┘
                                               │ Raw JSON
                                               ▼
                                  ┌─────────────────────────┐
                                  │  scripts/validator.py   │
                                  └────────────┬────────────┘
                                               │ Validated Data
                                               ▼
                                  ┌─────────────────────────┐
                                  │  scripts/transform.py   │ ◄── PySpark Engine
                                  └────────────┬────────────┘
                                               │ Clean Spark DataFrame
                                               ▼
                                  ┌─────────────────────────┐
                                  │    scripts/load.py      │
                                  └────────────┬────────────┘
                                               │ JDBC Write
                                               ▼
 ┌────────────────────────────────────────────────────────────────────────────────────────┐
 │                              PostgreSQL Data Warehouse                                 │
 │                                                                                        │
 │ ┌──────────────────┐    LEFT ANTI JOIN     ┌──────────────────┐                        │
 │ │     dim_city     │ ◄───────────────────► │   weather_data   │ (Staging Table)        │
 │ └──────────────────┘                       └────────┬─────────┘                        │
 │ ┌──────────────────┐                                │                                  │
 │ │   dim_weather    │ ◄──────────────────────────────┤                                  │
 │ └──────────────────┘                                ▼                                  │
 │                                            ┌──────────────────┐                        │
 │                                            │   fact_weather   │ (Fact Table)           │
 │                                            └──────────────────┘                        │
 └────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 📂 Detailed Repository & File Structure

```text
weather_etl_api/
├── .env.example                  # Environment variables template
├── .gitignore                    # Git exclusion rules
├── Dockerfile                    # Containerization instructions (Python + Java OpenJDK)
├── docker-compose.yml            # Multi-container orchestration (PostgreSQL + ETL)
├── README.md                     # Master project documentation
├── requirements.txt              # Python package dependencies
├── my_history.txt                # CLI command execution logs & setup notes
│
├── config/                       # Configuration Module
│   ├── __init__.py
│   └── settings.py               # Environment loader, DB credentials, API config, city list
│
├── data/                         # Data Storage
│   └── raw/                      # Raw API JSON payloads stored by timestamp
│
├── database/                     # Database Schema DDL
│   └── schema.sql                # Table definitions, constraints, indexes, and views
│
├── docs/                         # Extended Technical Documentation
│   ├── ARCHITECTURE.md           # Pipeline architecture & PySpark design specs
│   ├── API_DOCUMENTATION.md      # API endpoints, response schemas, & validation rules
│   ├── OPERATIONAL_GUIDE.md      # Setup, execution, SQL verification, & troubleshooting
│   └── CHANGELOG.md              # Project history & release milestones
│
├── drivers/                      # Database Connectors
│   └── postgresql-42.7.13.jar    # PostgreSQL JDBC Driver for PySpark
│
├── logs/                         # Application Logs
│   └── pipeline.log              # Persistent log outputs
│
├── scripts/                      # Core Pipeline Code
│   ├── extract.py                # OpenWeather API data extractor
│   ├── transform.py              # PySpark JSON flattener & transformer
│   ├── load.py                   # PostgreSQL staging table loader
│   ├── validator.py              # Raw JSON validation module
│   ├── logger.py                 # Centralized logging configuration
│   ├── main.py                   # Main pipeline entrypoint orchestrator
│   └── warehouse/                # Data Warehouse Dimensional Loaders
│       ├── __init__.py
│       ├── database.py           # Reusable Spark JDBC read/write helper
│       ├── dim_city.py           # Incremental City Dimension loader
│       ├── dim_weather.py        # Incremental Weather Dimension loader
│       └── fact_weather.py       # Incremental Fact Weather observation loader
│
└── tests/                        # Unit & Integration Tests
    ├── test_dim_city.py          # City Dimension test script
    ├── test_dim_weather.py       # Weather Dimension test script
    └── test_fact_weather.py      # Fact Weather test script
```

---

## 🗄️ Database Schema & Star Schema Model

The data warehouse uses a **Star Schema** to optimize query performance and reporting analytical workloads:

```
                    ┌──────────────────────────────┐
                    │           dim_city           │
                    ├──────────────────────────────┤
                    │ city_id (PK, SERIAL)         │
                    │ city_name (VARCHAR, UNIQUE)  │
                    │ latitude (DOUBLE PRECISION)  │
                    │ longitude (DOUBLE PRECISION) │
                    │ created_at (TIMESTAMP)       │
                    └──────────────┬───────────────┘
                                   │ 1
                                   │
                                   │ N
                    ┌──────────────┴───────────────┐
                    │          fact_weather        │
                    ├──────────────────────────────┤
                    │ fact_id (PK, SERIAL)         │
                    │ city_id (FK)                 │
                    │ weather_id (FK)              │
                    │ temperature (DOUBLE)         │
                    │ humidity (INTEGER)           │
                    │ pressure (INTEGER)           │
                    │ wind_speed (DOUBLE)          │
                    │ collected_at (TIMESTAMP)     │
                    │ created_at (TIMESTAMP)       │
                    └──────────────┬───────────────┘
                                   │ N
                                   │
                                   │ 1
                    ┌──────────────┴───────────────┐
                    │          dim_weather         │
                    ├──────────────────────────────┤
                    │ weather_id (PK, SERIAL)      │
                    │ weather (VARCHAR)            │
                    │ description (VARCHAR)        │
                    │ created_at (TIMESTAMP)       │
                    └──────────────────────────────┘
```

---

## 💻 Installation & Operational Guide

### 1. Prerequisites
- **Python**: Version 3.10 - 3.14
- **Java Runtime**: OpenJDK 17 or Java 21 (Required for PySpark)
- **PostgreSQL**: Version 14+ (Local installation or Docker container)

### 2. Quick Setup

```bash
# Clone the repository
git clone https://github.com/ARULKINT/weather_data_eng.git
cd weather_data_eng

# Create and activate virtual environment
python -m venv venv
# On Windows:
.\venv\Scripts\Activate.ps1
# On Linux/macOS:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Create environment configuration
cp .env.example .env
```

### 3. Database Initialization
Run the DDL script to create the staging, dimension, and fact tables:
```bash
psql -h localhost -U postgres -d weather_db -f database/schema.sql
```

### 4. Run the Pipeline
```bash
python scripts/main.py
```

---

## 🐳 Docker Deployment

To run PostgreSQL and the ETL pipeline together in containerized isolation:

```bash
# Build and start services
docker-compose up --build -d

# Check pipeline logs
docker logs -f weather_etl_pipeline

# Stop services
docker-compose down
```

---

## 🧪 Testing & Quality Assurance

Run the test suite to verify dimensional warehouse loading logic:

```bash
python tests/test_dim_city.py
python tests/test_dim_weather.py
python tests/test_fact_weather.py
```

---

## 📚 Supporting Documentation

For deeper technical details, explore the dedicated documentation in the [`docs/`](file:///c:/drive%20g/Projects_july/weather_etl_api/docs) directory:

- 🏛️ [**Architecture Specification**](file:///c:/drive%20g/Projects_july/weather_etl_api/docs/ARCHITECTURE.md): Technical deep dive into PySpark workflows, Left Anti Joins, and state design.
- 🌐 [**API Documentation**](file:///c:/drive%20g/Projects_july/weather_etl_api/docs/API_DOCUMENTATION.md): OpenWeather API specs, payload structures, schema mapping matrix, and validation rules.
- 🛠️ [**Operational Guide**](file:///c:/drive%20g/Projects_july/weather_etl_api/docs/OPERATIONAL_GUIDE.md): Complete operations, troubleshooting matrix, and database verification queries.
- 📝 [**Changelog**](file:///c:/drive%20g/Projects_july/weather_etl_api/docs/CHANGELOG.md): Project release history and feature milestones.

---

## 👨‍💻 Author & Acknowledgments

**Arul G**
- **Degree**: B.Tech Computer Science Engineering
- **Role**: Data Engineer
- **GitHub**: [@ARULKINT](https://github.com/ARULKINT)
- **Email**: aruldme004@gmail.com

---

## 📄 License

This project is open-source software licensed under the [MIT License](LICENSE).