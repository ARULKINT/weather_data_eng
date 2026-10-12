# 05. Pipeline Execution Guide

## Quickstart Execution Methods

The pipeline provides three execution interfaces depending on your preferred environment:

### Method 1: Turnkey PowerShell Runner (Recommended for Windows)
```powershell
.\run_pipeline.ps1
```
*Optional Flags*:
- `-RunTests`: Runs test suite before triggering pipeline execution.
```powershell
.\run_pipeline.ps1 -RunTests
```

### Method 2: Turnkey Batch Runner (Double-Click or CMD)
```cmd
run_pipeline.bat
```

### Method 3: Direct Python CLI Invocation
```bash
python -m scripts.main
```

---

## Pipeline Execution Lifecycle & Step Breakdown

When `scripts/main.py` is invoked, the execution follows six strictly sequenced phases:

```
[MAIN ENTRYPOINT]
       │
       ▼
 1. Spark Session Initialization (Loads PostgreSQL JDBC driver & tunes partitions)
       │
       ▼
 2. STEP 1: REST Extraction (Loops through CITIES, validates schema, writes to data/raw/)
       │
       ▼
 3. STEP 2: Staging Load (Flattens nested JSON & writes to staging table weather_data)
       │
       ▼
 4. STEP 3: Dim City Load (Extracts distinct cities, runs LEFT ANTI JOIN, inserts novel rows)
       │
       ▼
 5. STEP 4: Dim Weather Load (Extracts distinct conditions, runs LEFT ANTI JOIN, inserts novel rows)
       │
       ▼
 6. STEP 5: Fact Weather Load (Resolves surrogate keys, deduplicates, appends to fact_weather)
       │
       ▼
 7. Graceful Teardown (Shuts down SparkSession and logs execution summary)
```

---

## Detailed Step Breakdown

### Step 1: Extraction & Validation (`extract.py`)
- Loops through configured metropolitan targets in `config/settings.py` (`CITIES = ["Chennai", "Delhi", "Mumbai", "Bangalore", "Hyderabad", "Pune", "Kolkata"]`).
- Sends HTTP GET request with parameters: `q={city}&appid={API_KEY}&units=metric`.
- Invokes `validate_weather_data(payload)` from `scripts/validator.py`.
- Formats payload filename: `data/raw/{city}_{YYYYMMDD_HHMMSS}.json`.
- Logs extraction progress to `logs/pipeline.log`.

### Step 2: Transformation & Staging (`transform.py` & `load.py`)
- Spark reads the generated JSON files with `.option("multiLine", "true")`.
- Columns are extracted, projected, and cast:
  - Coordinate extraction (`coord.lat`, `coord.lon`)
  - Measurement extraction (`main.temp`, `main.humidity`, `main.pressure`, `wind.speed`)
  - Classification extraction (`weather[0].main`, `weather[0].description`)
- Spark writes DataFrame into PostgreSQL `weather_data` using JDBC `append` mode.

### Step 3: Incremental City Dimension Loading (`dim_city.py`)
- Extracts unique tuples of `(city, latitude, longitude)` from `weather_data`.
- Reads existing `dim_city` table.
- Executes `LEFT ANTI JOIN` on `city == city_name`.
- Appends only newly detected cities to `dim_city`.

### Step 4: Incremental Weather Dimension Loading (`dim_weather.py`)
- Extracts unique tuples of `(weather, description)` from `weather_data`.
- Reads existing `dim_weather` table.
- Executes `LEFT ANTI JOIN` matching composite key `(weather, description)`.
- Appends only newly detected conditions to `dim_weather`.

### Step 5: Fact Table Resolution (`fact_weather.py`)
- Joins staging records with `dim_city` to lookup `city_id`.
- Joins staging records with `dim_weather` to lookup `weather_id`.
- Resolves full observation rows with metrics and observation timestamp (`collected_at`).
- Executes `LEFT ANTI JOIN` on `(city_id, weather_id, collected_at)` against `fact_weather`.
- Appends deduplicated fact records.

---

## Verifying Pipeline Execution Output

### 1. Inspecting File System Outputs
Raw JSON payloads are saved under `data/raw/`:
```powershell
Get-ChildItem data\raw\ | Select-Object -First 5
```

### 2. Inspecting Structured Execution Logs
Detailed logs are written to `logs/pipeline.log`:
```powershell
Get-Content logs\pipeline.log -Tail 30
```

### 3. Querying PostgreSQL Database
Log in to PostgreSQL to inspect table record counts:
```sql
SELECT 'staging' AS table_type, COUNT(*) AS total_rows FROM weather_data
UNION ALL
SELECT 'dim_city', COUNT(*) FROM dim_city
UNION ALL
SELECT 'dim_weather', COUNT(*) FROM dim_weather
UNION ALL
SELECT 'fact_weather', COUNT(*) FROM fact_weather;
```

Expected query output after initial pipeline execution:
```
 table_type   | total_rows 
--------------+------------
 staging      |          7
 dim_city     |          7
 dim_weather  |          4
 fact_weather |          7
```
*(Subsequent runs will incrementally increment `staging` and `fact_weather`, while preserving `dim_city` and `dim_weather` stability).*

---

## Standalone Component Execution

For debugging individual stages in isolation:

| Stage | Command |
| :--- | :--- |
| **Extract Only** | `python scripts/extract.py` |
| **Transform Only** | `python scripts/transform.py` |
| **Test Dim City** | `python tests/test_dim_city.py` |
| **Test Dim Weather** | `python tests/test_dim_weather.py` |
| **Test Fact Weather**| `python tests/test_fact_weather.py` |
