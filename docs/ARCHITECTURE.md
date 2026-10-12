# 🏛️ Technical Architecture Specification

## Weather Data Engineering Pipeline

---

## 📐 Architecture Overview

The **Weather Data Engineering Pipeline** is designed as a modular, production-ready, multi-stage ETL data pipeline. It extracts current meteorological data from the OpenWeather API, validates data schemas and value bounds, normalizes and transforms raw JSON structures using **PySpark**, loads raw transformed records into a PostgreSQL staging table, and builds an incremental **Star Schema Data Warehouse** (`dim_city`, `dim_weather`, `fact_weather`).

```
┌─────────────────────────┐
│     OpenWeather API     │
└────────────┬────────────┘
             │ HTTP GET (JSON)
             ▼
┌─────────────────────────┐
│     scripts/extract.py   │  ──▶ Local Storage (data/raw/*.json)
└────────────┬────────────┘
             │ JSON Payload
             ▼
┌─────────────────────────┐
│   scripts/validator.py  │  ──▶ Range & Schema Validation
└────────────┬────────────┘
             │ Validated Raw Data
             ▼
┌─────────────────────────┐
│   scripts/transform.py  │  ──▶ PySpark Data Structuring & Type Casting
└────────────┬────────────┘
             │ Clean Spark DataFrame
             ▼
┌─────────────────────────┐
│     scripts/load.py     │  ──▶ PostgreSQL Staging Table (weather_data)
└────────────┬────────────┘
             │ JDBC Driver
             ▼
┌───────────────────────────────────────────────────────────┐
│                 PostgreSQL Data Warehouse                 │
│                                                           │
│  ┌────────────────┐   LEFT ANTI   ┌───────────────────┐  │
│  │   dim_city     │ ◄───────────► │   weather_data    │  │
│  └────────────────┘     JOIN      │     (Staging)     │  │
│  ┌────────────────┐               └─────────┬─────────┘  │
│  │  dim_weather   │ ◄───────────────────────┤            │
│  └────────────────┘                         ▼            │
│                                   ┌───────────────────┐  │
│                                   │   fact_weather    │  │
│                                   └───────────────────┘  │
└───────────────────────────────────────────────────────────┘
```

---

## 🧩 Pipeline Components & Module Breakdown

### 1. Configuration (`config/settings.py`)
- Reads environment configurations using `python-dotenv`.
- Supplies connection credentials for PostgreSQL database and OpenWeather API key.
- Defines targeted urban centers (`Chennai`, `Delhi`, `Mumbai`, `Bangalore`, `Hyderabad`, `Pune`, `Kolkata`).
- Constructs JDBC URL and driver property dictionaries dynamically.

### 2. Centralized Logging (`scripts/logger.py`)
- Standardized logging module producing unified logs to both stdout console and `logs/pipeline.log`.
- Configured format: `%(asctime)s | %(levelname)s | %(message)s`.
- Replaces raw `print()` statements across all pipeline execution modules.

### 3. API Data Extraction (`scripts/extract.py`)
- Queries OpenWeather API for each target city.
- Requests metric units (`Celsius`, `m/s`, `hPa`).
- Invokes validation prior to writing payload to storage.
- Saves formatted JSON files to `data/raw/{city}_{YYYYMMDD_HHMMSS}.json`.

### 4. Data Validation Engine (`scripts/validator.py`)
- Verifies structural integrity of incoming API responses.
- Evaluates constraints:
  - Latitude: \([-90, 90]\)
  - Longitude: \([-180, 180]\)
  - Humidity: \([0, 100]\%\)
  - Pressure: \(> 0 \text{ hPa}\)
  - Wind Speed: \(\ge 0 \text{ m/s}\)
  - Weather Condition & Description presence.

### 5. Data Transformation Engine (`scripts/transform.py`)
- Leverages **PySpark (`SparkSession`)** to process multi-line JSON structures.
- Flattens nested JSON hierarchies (`coord.lat`, `coord.lon`, `main.temp`, `main.humidity`, `main.pressure`, `wind.speed`, `weather[0].main`, `weather[0].description`).
- Casts Spark columns into strict data types (`DoubleType`, `IntegerType`, `StringType`).

### 6. Staging Loader (`scripts/load.py`)
- Writes clean PySpark DataFrames into the `weather_data` PostgreSQL staging table via JDBC.
- Appends new observations with execution timestamps.

### 7. Warehouse Star Schema Modules (`scripts/warehouse/`)

- **Database Helpers (`scripts/warehouse/database.py`)**: Reusable helper functions `read_table()` and `write_table()` encapsulating Spark JDBC IO.
- **City Dimension Builder (`scripts/warehouse/dim_city.py`)**:
  - Extracts distinct cities and coordinate pairs.
  - Performs **Left Anti Join** against `dim_city` on `city_name` to isolate unseen cities.
  - Appends new city records incrementally.
- **Weather Dimension Builder (`scripts/warehouse/dim_weather.py`)**:
  - Extracts distinct `(weather, description)` tuples.
  - Performs **Left Anti Join** against `dim_weather` matching both fields.
  - Appends new weather condition combinations incrementally.
- **Fact Weather Builder (`scripts/warehouse/fact_weather.py`)**:
  - Joins staging records with `dim_city` and `dim_weather` to resolve surrogate keys (`city_id`, `weather_id`).
  - Filters out existing observations using a Left Anti Join on `(city_id, weather_id, collected_at)`.
  - Appends new metric rows into `fact_weather`.

---

## ⭐ Star Schema ER Diagram

```
                 ┌────────────────────────────────┐
                 │           dim_city             │
                 ├────────────────────────────────┤
                 │ city_id (PK, SERIAL)           │
                 │ city_name (VARCHAR, UNIQUE)    │
                 │ latitude (DOUBLE)              │
                 │ longitude (DOUBLE)             │
                 │ created_at (TIMESTAMP)         │
                 └──────────────┬─────────────────┘
                                │ 1
                                │
                                │ N
                 ┌──────────────┴─────────────────┐
                 │          fact_weather          │
                 ├────────────────────────────────┤
                 │ fact_id (PK, SERIAL)           │
                 │ city_id (FK)                   │
                 │ weather_id (FK)                │
                 │ temperature (DOUBLE)           │
                 │ humidity (INTEGER)             │
                 │ pressure (INTEGER)             │
                 │ wind_speed (DOUBLE)            │
                 │ collected_at (TIMESTAMP)       │
                 │ created_at (TIMESTAMP)         │
                 └──────────────┬─────────────────┘
                                │ N
                                │
                                │ 1
                 ┌──────────────┴─────────────────┐
                 │          dim_weather           │
                 ├────────────────────────────────┤
                 │ weather_id (PK, SERIAL)        │
                 │ weather (VARCHAR)              │
                 │ description (VARCHAR)          │
                 │ created_at (TIMESTAMP)         │
                 └────────────────────────────────┘
```

---

## ⚡ PySpark & Incremental Load Design Patterns

### 1. Left Anti Join Pattern for Incremental Dimensions
To ensure idempotency and prevent duplicate primary keys or repeated entries:
$$\text{NewRecords} = \text{StagingRecords} \setminus \text{DimensionRecords}$$
Using PySpark SQL:
```python
new_city_df = city_df.join(
    dim_city_df,
    city_df.city == dim_city_df.city_name,
    how="left_anti"
)
```

### 2. Spark Session Configuration
- Driver extra classpath dynamically linked to `./drivers/postgresql-42.7.13.jar`.
- Local partition tuning: `spark.sql.shuffle.partitions` set to `4` for optimal local CPU utilization.
- Native Hadoop library warnings suppressed on Windows environments (`spark.hadoop.hadoop.native.lib` set to `false`).
