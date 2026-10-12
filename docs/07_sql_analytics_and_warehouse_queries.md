# 07. SQL Analytics and Warehouse Queries

## Overview

The star schema warehouse allows business analysts, data scientists, and reporting dashboards to execute high-performance analytical queries without scanning un-normalized staging logs.

---

## 1. Production Analytical Views

The warehouse includes two pre-built analytical views created in `database/schema.sql`:

### View 1: Real-Time Weather Status (`vw_latest_city_weather`)
Retrieves the most recent meteorological snapshot for every monitored city:
```sql
SELECT 
    city_name,
    weather,
    description,
    temperature,
    humidity,
    wind_speed,
    collected_at
FROM vw_latest_city_weather
ORDER BY temperature DESC;
```

### View 2: Daily Aggregated Climate Trends (`vw_daily_weather_summary`)
Provides day-level summary statistics per city for charting trends:
```sql
SELECT 
    city_name,
    observation_date,
    avg_temperature,
    max_temperature,
    min_temperature,
    avg_humidity,
    avg_wind_speed
FROM vw_daily_weather_summary
WHERE observation_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY observation_date DESC, avg_temperature DESC;
```

---

## 2. Advanced Analytical Queries

### Query 1: Hottest and Coldest Cities (Latest Batch)
Identifies extreme temperatures across all monitored metropolitan areas:
```sql
WITH latest_observation AS (
    SELECT 
        c.city_name,
        f.temperature,
        f.humidity,
        f.collected_at,
        ROW_NUMBER() OVER (PARTITION BY c.city_name ORDER BY f.collected_at DESC) AS rn
    FROM fact_weather f
    JOIN dim_city c ON f.city_id = c.city_id
)
SELECT 
    city_name,
    temperature,
    humidity,
    collected_at,
    CASE 
        WHEN temperature >= 35.0 THEN 'High Heat Alert'
        WHEN temperature <= 10.0 THEN 'Cold Advisory'
        ELSE 'Normal Range'
    END AS thermal_status
FROM latest_observation
WHERE rn = 1
ORDER BY temperature DESC;
```

---

### Query 2: Frequency Distribution of Weather Conditions
Analyzes which weather classifications occur most frequently per municipality:
```sql
SELECT 
    c.city_name,
    w.weather,
    w.description,
    COUNT(f.fact_id) AS observation_count,
    ROUND(
        COUNT(f.fact_id) * 100.0 / SUM(COUNT(f.fact_id)) OVER (PARTITION BY c.city_name),
        2
    ) AS percentage_of_total_observations
FROM fact_weather f
JOIN dim_city c ON f.city_id = c.city_id
JOIN dim_weather w ON f.weather_id = w.weather_id
GROUP BY c.city_name, w.weather, w.description
ORDER BY c.city_name, observation_count DESC;
```

---

### Query 3: 7-Day Rolling Temperature Moving Average
Calculates a 7-day trailing moving average to smooth short-term microclimate fluctuations:
```sql
SELECT 
    c.city_name,
    DATE(f.collected_at) AS observation_date,
    ROUND(AVG(f.temperature)::numeric, 2) AS daily_avg_temp,
    ROUND(
        AVG(AVG(f.temperature)) OVER (
            PARTITION BY c.city_name 
            ORDER BY DATE(f.collected_at)
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )::numeric,
        2
    ) AS seven_day_moving_avg_temp
FROM fact_weather f
JOIN dim_city c ON f.city_id = c.city_id
GROUP BY c.city_name, DATE(f.collected_at)
ORDER BY c.city_name, observation_date DESC;
```

---

### Query 4: Temperature Heat Index / Discomfort Level
Computes an estimated discomfort index combining temperature and relative humidity:
```sql
SELECT 
    c.city_name,
    f.temperature,
    f.humidity,
    ROUND(
        (f.temperature - 0.55 * (1 - 0.01 * f.humidity) * (f.temperature - 14.5))::numeric,
        2
    ) AS apparent_discomfort_index,
    f.collected_at
FROM fact_weather f
JOIN dim_city c ON f.city_id = c.city_id
WHERE f.collected_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
ORDER BY apparent_discomfort_index DESC;
```

---

### Query 5: Diurnal Temperature Spread (Max vs Min Delta)
Evaluates day vs. night temperature stability per city:
```sql
SELECT 
    c.city_name,
    DATE(f.collected_at) AS observation_date,
    MAX(f.temperature) AS high_temp,
    MIN(f.temperature) AS low_temp,
    ROUND((MAX(f.temperature) - MIN(f.temperature))::numeric, 2) AS diurnal_spread,
    ROUND(AVG(f.wind_speed)::numeric, 2) AS avg_wind
FROM fact_weather f
JOIN dim_city c ON f.city_id = c.city_id
GROUP BY c.city_name, DATE(f.collected_at)
ORDER BY diurnal_spread DESC;
```

---

## 3. Business Intelligence (Power BI / Tableau) Integration

1. **Connector**: PostgreSQL ODBC / JDBC driver.
2. **Server**: `localhost` (or Docker container IP / cloud endpoint).
3. **Database**: `weather_db`.
4. **Target Model**:
   - Direct connection to `vw_latest_city_weather` for real-time Executive Dashboards.
   - Star Schema connection linking `dim_city` (1-to-many) $\rightarrow$ `fact_weather` $\leftarrow$ (many-to-1) `dim_weather` for multi-dimensional slicing and dicing.
