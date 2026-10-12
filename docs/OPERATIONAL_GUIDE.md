# 🛠️ Operations & Maintenance Guide

## Weather Data Engineering Pipeline

---

## 🚀 Environment Setup & Operations

### 1. Prerequisites Check

Ensure the following tools are installed on your host machine:

- **Python**: Version 3.10 to 3.14
- **Java Runtime / JDK**: OpenJDK 17 or Java 21 (Required for PySpark execution)
- **PostgreSQL**: Version 14+ (Local service or Docker container)
- **Git**: Version Control

#### Verify Java installation
```bash
java -version
```

#### Verify Python installation
```bash
python --version
```

---

## 🛠️ Step-by-Step Installation

### Step 1: Clone Workspace
```bash
git clone https://github.com/ARULKINT/weather_data_eng.git
cd weather_data_eng
```

### Step 2: Create & Activate Python Virtual Environment

#### On Windows (PowerShell):
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

#### On Linux / macOS:
```bash
python3 -m venv venv
source venv/bin/activate
```

### Step 3: Install Required Dependencies
```bash
pip install -r requirements.txt
```

### Step 4: Configure Environment Variables
Copy `.env.example` to `.env` and fill in your credentials:
```bash
cp .env.example .env
```
Edit `.env`:
```env
OPENWEATHER_API_KEY=your_actual_api_key_here
DB_HOST=localhost
DB_PORT=5432
DB_NAME=weather_db
DB_USER=postgres
DB_PASSWORD=your_password
```

### Step 5: Initialize PostgreSQL Database & Tables
Execute the database initialization script against your PostgreSQL instance:
```bash
psql -h localhost -U postgres -d weather_db -f database/schema.sql
```

---

## ⚡ Execution Instructions

### Running the Full Pipeline
To execute the complete end-to-end ETL flow (Extraction -> Validation -> PySpark Transformation -> PostgreSQL Staging -> Star Schema Data Warehouse):

```bash
python scripts/main.py
```

### Running Individual Pipeline Modules

#### 1. Extract API Weather Data
```bash
python scripts/extract.py
```

#### 2. Test Transformation locally
```bash
python scripts/transform.py
```

### Running Unit & Integration Tests

```bash
python tests/test_dim_city.py
python tests/test_dim_weather.py
python tests/test_fact_weather.py
```

---

## 🐳 Docker Deployment Guide

### Option A: Spin up PostgreSQL and ETL with Docker Compose
```bash
docker-compose up --build -d
```

### Option B: Check Container Logs
```bash
docker logs -f weather_etl_pipeline
```

### Option C: Stop Containers
```bash
docker-compose down
```

---

## 🔍 Database Sanity Check Queries

Run the following SQL queries in `psql` or pgAdmin to inspect data warehouse load success:

### 1. Check Staging Row Counts
```sql
SELECT COUNT(*) FROM weather_data;
```

### 2. Inspect City Dimension
```sql
SELECT * FROM dim_city ORDER BY city_id;
```

### 3. Inspect Weather Dimension
```sql
SELECT * FROM dim_weather ORDER BY weather_id;
```

### 4. Check Fact Weather Observations
```sql
SELECT f.fact_id, c.city_name, w.weather, w.description, f.temperature, f.humidity, f.collected_at
FROM fact_weather f
JOIN dim_city c ON f.city_id = c.city_id
JOIN dim_weather w ON f.weather_id = w.weather_id
ORDER BY f.collected_at DESC
LIMIT 10;
```

---

## 🚨 Troubleshooting & Diagnostic Guide

| Error / Issue | Root Cause | Solution |
|---------------|------------|----------|
| `FileNotFoundError: PostgreSQL JDBC Driver not found` | `drivers/postgresql-42.7.13.jar` is missing or path unresolved. | Verify `drivers/postgresql-42.7.13.jar` exists in project root. |
| `java.lang.ClassNotFoundException: org.postgresql.Driver` | Spark failed to load the JDBC driver JAR. | Ensure `JAVA_HOME` is set and `spark.jars` config points to valid JAR path. |
| `requests.exceptions.HTTPError: 401 Client Error: Unauthorized` | Invalid or inactive OpenWeather API Key. | Check `OPENWEATHER_API_KEY` value in `.env`. |
| `psycopg2.OperationalError: could not connect to server` | PostgreSQL server is stopped or port `5432` blocked. | Start PostgreSQL service (`sudo systemctl start postgresql` or Docker). |
| `ValueError: Invalid latitude/longitude` | API response failed validation rules. | Inspect `logs/pipeline.log` for exact response payload error. |
