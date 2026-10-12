# 03. Data Dictionary and Warehouse Schema

## Schema Architecture Overview

The Weather Data Engineering Pipeline utilizes PostgreSQL to maintain both a high-throughput landing/staging zone and an optimized Kimball dimensional star schema.

```
Database: weather_db
 ├── Staging Table:
 │    └── weather_data
 ├── Dimension Tables:
 │    ├── dim_city
 │    └── dim_weather
 ├── Fact Table:
 │    └── fact_weather
 └── Analytical Views:
      ├── vw_latest_city_weather
      └── vw_daily_weather_summary
```

---

## 1. Staging Table: `weather_data`

Serves as the raw transformed observation landing zone loaded by PySpark. Captures atomic, un-normalized weather records directly from each API pipeline execution.

| Column Name | Data Type | Nullable | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `id` | `SERIAL` | No | Auto-increment | Primary surrogate identifier for staging row |
| `city` | `VARCHAR(100)` | No | None | Name of the city reporting weather observations |
| `latitude` | `DOUBLE PRECISION`| No | None | Geographic latitude (-90.0 to 90.0) |
| `longitude` | `DOUBLE PRECISION`| No | None | Geographic longitude (-180.0 to 180.0) |
| `temperature` | `DOUBLE PRECISION`| No | None | Ambient temperature measured in Celsius (°C) |
| `humidity` | `INTEGER` | No | None | Relative humidity percentage (0 to 100%) |
| `pressure` | `INTEGER` | No | None | Atmospheric pressure at sea level in hectopascals (hPa) |
| `wind_speed` | `DOUBLE PRECISION`| No | None | Wind speed velocity in meters per second (m/s) |
| `weather` | `VARCHAR(50)` | No | None | High-level weather category (e.g., Rain, Clear, Clouds) |
| `description` | `VARCHAR(100)` | No | None | Granular weather description (e.g., light intensity drizzle)|
| `collected_at` | `TIMESTAMPTZ` | No | `CURRENT_TIMESTAMP` | Timestamp indicating when row was staged in the database |

### Staging Indexes
- `idx_weather_data_city`: B-Tree index on `city` for fast filtering.
- `idx_weather_data_collected_at`: B-Tree index on `collected_at` for chronological queries.

---

## 2. Dimension Table: `dim_city`

Conforms to standard Kimball Dimension guidelines. Contains one unique record per distinct geographic municipality observed by the pipeline.

| Column Name | Data Type | Nullable | Constraints | Description |
| :--- | :--- | :--- | :--- | :--- |
| `city_id` | `SERIAL` | No | `PRIMARY KEY` | Surrogate integer identifier for the city |
| `city_name` | `VARCHAR(100)` | No | `UNIQUE` | Business natural key (Municipality / City Name) |
| `latitude` | `DOUBLE PRECISION`| No | None | Pinned geographic latitude coordinate |
| `longitude` | `DOUBLE PRECISION`| No | None | Pinned geographic longitude coordinate |
| `created_at` | `TIMESTAMPTZ` | No | `CURRENT_TIMESTAMP` | Audit timestamp when city was first discovered |

### Dimension Indexes
- `idx_dim_city_name`: Unique B-Tree index on `city_name`.

---

## 3. Dimension Table: `dim_weather`

Captures the distinct conditions, weather groups, and granular descriptors emitted by the OpenWeatherMap API taxonomy.

| Column Name | Data Type | Nullable | Constraints | Description |
| :--- | :--- | :--- | :--- | :--- |
| `weather_id` | `SERIAL` | No | `PRIMARY KEY` | Surrogate integer identifier for weather condition |
| `weather` | `VARCHAR(50)` | No | Part of Unique | Broad category (e.g., Rain, Clouds, Snow, Mist) |
| `description` | `VARCHAR(100)` | No | Part of Unique | Granular condition (e.g., heavy intensity rain) |
| `created_at` | `TIMESTAMPTZ` | No | `CURRENT_TIMESTAMP` | Audit timestamp when condition was first recorded |

### Constraints & Indexes
- `uq_weather_desc`: Composite `UNIQUE (weather, description)`.
- `idx_dim_weather_type`: B-Tree index on `weather`.

---

## 4. Fact Table: `fact_weather`

The central fact table storing periodic weather metrics. Every record represents a discrete atmospheric measurement associated with foreign keys referencing dimension tables.

| Column Name | Data Type | Nullable | Constraints | Description |
| :--- | :--- | :--- | :--- | :--- |
| `fact_id` | `SERIAL` | No | `PRIMARY KEY` | Unique surrogate identifier for fact row |
| `city_id` | `INTEGER` | No | `FK -> dim_city(city_id)` | Foreign key link to City Dimension |
| `weather_id` | `INTEGER` | No | `FK -> dim_weather(weather_id)` | Foreign key link to Weather Dimension |
| `temperature` | `DOUBLE PRECISION`| No | None | Numeric ambient temperature in °C |
| `humidity` | `INTEGER` | No | None | Numeric relative humidity (0–100%) |
| `pressure` | `INTEGER` | No | None | Atmospheric barometric pressure in hPa |
| `wind_speed` | `DOUBLE PRECISION`| No | None | Measured wind velocity in m/s |
| `collected_at` | `TIMESTAMPTZ` | No | Part of Unique | True physical timestamp of measurement |
| `created_at` | `TIMESTAMPTZ` | No | `CURRENT_TIMESTAMP` | Audit timestamp of warehouse insertion |

### Constraints & Indexes
- `uq_fact_observation`: Composite `UNIQUE (city_id, weather_id, collected_at)` guaranteeing idempotency.
- `idx_fact_city`: Foreign key B-Tree index on `city_id`.
- `idx_fact_weather`: Foreign key B-Tree index on `weather_id`.
- `idx_fact_collected`: B-Tree index on `collected_at` for time-slice partition pruning and range queries.

---

## 5. Analytical Views

### View: `vw_latest_city_weather`
Returns the single most recent weather observation for every tracked municipality:
```sql
CREATE OR REPLACE VIEW vw_latest_city_weather AS
SELECT 
    c.city_name,
    c.latitude,
    c.longitude,
    w.weather,
    w.description,
    f.temperature,
    f.humidity,
    f.pressure,
    f.wind_speed,
    f.collected_at
FROM fact_weather f
JOIN dim_city c ON f.city_id = c.city_id
JOIN dim_weather w ON f.weather_id = w.weather_id
WHERE f.collected_at = (
    SELECT MAX(f2.collected_at) 
    FROM fact_weather f2 
    WHERE f2.city_id = f.city_id
);
```

### View: `vw_daily_weather_summary`
Aggregates hourly observation telemetry into daily summary statistics suitable for BI reporting:
```sql
CREATE OR REPLACE VIEW vw_daily_weather_summary AS
SELECT 
    c.city_name,
    DATE(f.collected_at) AS observation_date,
    ROUND(AVG(f.temperature)::numeric, 2) AS avg_temperature,
    MAX(f.temperature) AS max_temperature,
    MIN(f.temperature) AS min_temperature,
    ROUND(AVG(f.humidity)::numeric, 2) AS avg_humidity,
    ROUND(AVG(f.wind_speed)::numeric, 2) AS avg_wind_speed
FROM fact_weather f
JOIN dim_city c ON f.city_id = c.city_id
GROUP BY c.city_name, DATE(f.collected_at)
ORDER BY observation_date DESC, c.city_name;
```
