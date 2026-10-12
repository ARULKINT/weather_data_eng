# 06. Data Quality and Validation Framework

## Overview

In mission-critical data engineering systems, bad data can corrupt downstream data lakes, warehouse dimensions, and reporting dashboards. The **Weather Data Engineering Pipeline** enforces a defense-in-depth data quality framework across three consecutive checkpoints:

```
[API Telemetry]
      │
      ▼
┌────────────────────────────────────────────────────────┐
│ 1. PRE-INGESTION VALIDATION (Pure Python Gatekeeper)   │
│    • Coordinates Bounding    • Physical Metric Ranges  │
│    • Null / Presence Checks  • Payload Integrity       │
└────────────────────────────────────────────────────────┘
      │
      ▼
┌────────────────────────────────────────────────────────┐
│ 2. IN-FLIGHT SCHEMA ENFORCEMENT (PySpark Transformation)│
│    • Explicit Column Projections • Explicit Casting    │
│    • Nested Array Element Parsing                      │
└────────────────────────────────────────────────────────┘
      │
      ▼
┌────────────────────────────────────────────────────────┐
│ 3. STORAGE & INTEGRITY CONSTRAINTS (PostgreSQL DDL)    │
│    • NOT NULL Constraints    • Foreign Key Integrity   │
│    • Unique Keys & Left Anti Join Deduplication        │
└────────────────────────────────────────────────────────┘
```

---

## 1. Pre-Ingestion Validation Rules (`scripts/validator.py`)

Every HTTP payload returned by the OpenWeatherMap API is intercepted and evaluated against strict mathematical and atmospheric rules before disk persistence:

| Field | Validation Rule | Physical / Logical Justification | Error Condition |
| :--- | :--- | :--- | :--- |
| `name` (City) | Non-empty `str` | Must identify a known geographic municipality | Missing, empty string, or `None` |
| `coord.lat` | $-90.0 \le \text{lat} \le +90.0$ | Valid geographic latitude coordinate range | $\text{lat} < -90$ or $\text{lat} > 90$ |
| `coord.lon` | $-180.0 \le \text{lon} \le +180.0$ | Valid geographic longitude coordinate range | $\text{lon} < -180$ or $\text{lon} > 180$ |
| `main.temp` | Non-null `float` | Ambient temperature measurement must be present | Missing or `None` |
| `main.humidity`| $0 \le \text{humidity} \le 100$ | Relative humidity percentage cannot exceed $100\%$ | $\text{humidity} < 0$ or $> 100$ |
| `main.pressure`| $\text{pressure} > 0$ | Barometric pressure cannot be zero or negative | $\text{pressure} \le 0$ or `None` |
| `wind.speed` | $\text{speed} \ge 0.0$ | Scalar wind speed velocity cannot be negative | $\text{speed} < 0.0$ or `None` |
| `weather` | Array $\ge 1$ item | Must contain at least one weather descriptor object | Empty array or missing key |
| `weather[0].main` | Non-empty `str` | Broad taxonomy category (e.g., Rain, Clouds) | Empty string or `None` |
| `weather[0].description` | Non-empty `str` | Granular condition descriptor | Empty string or `None` |

### Validator Implementation Snippet
```python
def validate_weather_data(weather_data: dict) -> bool:
    city = weather_data.get("name")
    if not city:
        raise ValueError("City name is missing.")

    coord = weather_data.get("coord", {})
    latitude = coord.get("lat")
    longitude = coord.get("lon")
    if latitude is None or not (-90 <= latitude <= 90):
        raise ValueError("Invalid latitude.")
    if longitude is None or not (-180 <= longitude <= 180):
        raise ValueError("Invalid longitude.")

    main = weather_data.get("main", {})
    temperature = main.get("temp")
    humidity = main.get("humidity")
    pressure = main.get("pressure")
    if temperature is None:
        raise ValueError("Temperature is missing.")
    if humidity is None or not (0 <= humidity <= 100):
        raise ValueError("Invalid humidity.")
    if pressure is None or pressure <= 0:
        raise ValueError("Invalid pressure.")

    wind = weather_data.get("wind", {})
    wind_speed = wind.get("speed")
    if wind_speed is None or wind_speed < 0:
        raise ValueError("Invalid wind speed.")

    weather = weather_data.get("weather", [])
    if not weather or not weather[0].get("main") or not weather[0].get("description"):
        raise ValueError("Weather condition details missing.")

    return True
```

---

## 2. In-Flight Transformation Type Safety

PySpark acts as a strongly typed intermediary between unstructured raw JSON and relational database tables:
- **Explicit Schema Casting**: Rather than relying on implicit type coercion, every field is cast to its target primitive type (`DoubleType()`, `IntegerType()`, `StringType()`).
- **Array Extraction**: The nested weather array is deterministically indexed (`col("weather")[0]["main"]`), preventing schema mismatches when payloads contain auxiliary descriptors.

---

## 3. Relational & Dimensional Integrity

PostgreSQL enforces ACID guarantees and relational constraints:

### NOT NULL Enforcement
All staging and dimensional tables define mandatory columns as `NOT NULL`, guaranteeing that incomplete observations cannot corrupt analytical calculations.

### Foreign Key Integrity
The `fact_weather` table defines explicit foreign key constraints:
```sql
CONSTRAINT fk_city FOREIGN KEY (city_id) REFERENCES dim_city(city_id) ON DELETE CASCADE,
CONSTRAINT fk_weather FOREIGN KEY (weather_id) REFERENCES dim_weather(weather_id) ON DELETE CASCADE
```
This guarantees that no orphan fact records can ever exist without associated dimensional attributes.

### Composite Uniqueness & Deduplication
To enforce idempotency at the database engine level:
1. `dim_city`: `UNIQUE (city_name)`
2. `dim_weather`: `CONSTRAINT uq_weather_desc UNIQUE (weather, description)`
3. `fact_weather`: `CONSTRAINT uq_fact_observation UNIQUE (city_id, weather_id, collected_at)`

---

## 4. Anomaly Handling & Audit Logging

When an anomaly is encountered during extraction or transformation:
1. The error is intercepted without terminating the entire pipeline run.
2. A complete stack trace is recorded to `logs/pipeline.log` via Python's standard `logging.Logger.exception()`.
3. The pipeline proceeds to process subsequent cities, ensuring maximum data availability even when a single city's feed encounters network hiccups or malformed JSON.
