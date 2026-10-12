# 08. Testing and QA Strategy

## Overview

Quality Assurance (QA) in the Weather Data Engineering Pipeline follows the **Testing Pyramid** methodology, incorporating fast unit tests, isolated PySpark transformation tests, and full integration tests against PostgreSQL.

```
       / \
      /   \        Warehouse Integration Tests
     /     \       (test_dim_city, test_dim_weather, test_fact_weather)
    /───────\
   /         \     PySpark Transform Unit Tests
  /           \    (test_transform - in-memory local Spark session)
 /─────────────\
/               \  Fast Unit & Boundary Tests
─────────────────  (test_validator, test_settings)
```

---

## Test Inventory & Coverage Matrix

| Test Suite | File Location | Scope | Dependencies | Execution Time |
| :--- | :--- | :--- | :--- | :--- |
| **Data Quality Validator** | `tests/test_validator.py` | Validates coordinate boundaries, temperature nulls, humidity 0–100%, pressure, wind speed | Standard Python | < 0.01 sec |
| **Configuration Settings** | `tests/test_settings.py` | Verifies `CITIES`, JDBC configuration, DB URL string formatting | Standard Python | < 0.01 sec |
| **PySpark Transformation** | `tests/test_transform.py` | Tests multi-line JSON parsing, type casting, and column projections | In-memory PySpark | ~ 5–15 sec |
| **City Dimension** | `tests/test_dim_city.py` | Tests staging extraction, `LEFT ANTI JOIN`, and PostgreSQL insert | Spark + PostgreSQL | ~ 10–20 sec |
| **Weather Dimension** | `tests/test_dim_weather.py` | Tests composite key `(weather, description)` deduplication | Spark + PostgreSQL | ~ 10–20 sec |
| **Fact Weather Loader** | `tests/test_fact_weather.py` | Tests surrogate key resolution and fact table deduplication | Spark + PostgreSQL | ~ 10–20 sec |

---

## Executing the Test Suites

### 1. Executing Fast Unit Tests (Zero External Dependencies)
These tests require only Python and run in milliseconds:
```bash
python -m unittest tests/test_validator.py tests/test_settings.py
```

### 2. Executing PySpark Transformation Tests (Local Compute)
Tests in-memory PySpark JSON parsing and schema alignment:
```bash
python -m unittest tests/test_transform.py
```

### 3. Executing Full Test Suite via Discover
Discovers and executes all registered test cases:
```bash
python -m unittest discover tests
```

### 4. Executing via Pytest (If Pytest Installed)
```bash
pytest tests/ -v
```

---

## Detailed Test Scenarios

### 1. Data Validator Test Scenarios (`test_validator.py`)
- `test_valid_payload_success`: Validates complete standard OpenWeather JSON payload.
- `test_missing_city_name`: Rejects payloads where city name is empty or missing.
- `test_invalid_latitude_boundaries`: Rejects latitudes $< -90^\circ$ or $> +90^\circ$.
- `test_invalid_longitude_boundaries`: Rejects longitudes $< -180^\circ$ or $> +180^\circ$.
- `test_missing_temperature`: Rejects payloads missing ambient temperature.
- `test_invalid_humidity_percentage`: Rejects humidity $< 0\%$ or $> 100\%$.
- `test_invalid_pressure`: Rejects pressure $\le 0 \text{ hPa}$.
- `test_invalid_wind_speed`: Rejects negative wind velocity $< 0.0 \text{ m/s}$.
- `test_missing_weather_condition`: Rejects empty or missing weather condition arrays.

### 2. PySpark Transformation Test Scenarios (`test_transform.py`)
- Generates a controlled, isolated temporary JSON file on disk.
- Mounts a single-thread local Spark session (`local[1]`).
- Validates the resulting DataFrame schema types:
  - `city`: `StringType`
  - `latitude`: `DoubleType`
  - `longitude`: `DoubleType`
  - `temperature`: `DoubleType`
  - `humidity`: `IntegerType`
  - `pressure`: `IntegerType`
  - `wind_speed`: `DoubleType`
  - `weather`: `StringType`
  - `description`: `StringType`
- Asserts that all extracted values match raw source coordinates and metrics.

---

## CI/CD Automation (GitHub Actions)

The repository includes `.github/workflows/ci.yml`. On every `push` and `pull_request` to `main`, GitHub Actions:
1. Provisions an Ubuntu runtime.
2. Configures Python 3.11 with cached pip packages.
3. Sets up OpenJDK 17 (`temurin`).
4. Installs `requirements.txt`.
5. Downloads the verified PostgreSQL JDBC driver.
6. Automatically executes unit tests.
