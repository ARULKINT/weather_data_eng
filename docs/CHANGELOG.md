# 📝 Project Changelog

All notable changes to the **Weather Data Engineering Pipeline** project will be documented in this file.

---

## [1.1.0] - 2026-10-03

### Added
- **Incremental Star Schema Data Warehouse Support**:
  - Implemented `dim_city.py` for incremental city dimension loading using Left Anti Joins.
  - Implemented `dim_weather.py` for incremental weather condition dimension loading.
  - Implemented `fact_weather.py` for loading fact observations linking surrogate dimension keys.
  - Reusable database helper module `database.py` with central `read_table()` and `write_table()` methods.
- **Data Validation Layer**:
  - Added strict data quality checks in `validator.py` covering latitude, longitude, temperature, humidity, pressure, and wind speed.
- **Centralized Logging Engine**:
  - Added `logger.py` replacing standard print output with structured stdout & file logging (`logs/pipeline.log`).
- **Containerization Support**:
  - Added `Dockerfile` with OpenJDK 17 and Python 3.12 support.
  - Added `docker-compose.yml` for orchestration of PostgreSQL database and PySpark ETL process.
- **Comprehensive Technical Documentation**:
  - `database/schema.sql`: Full DDL schema script with views and performance indexes.
  - `docs/ARCHITECTURE.md`: Deep dive technical architecture and data flow spec.
  - `docs/API_DOCUMENTATION.md`: OpenWeather API specs, JSON payload structure, validation rules, and schema matrix.
  - `docs/OPERATIONAL_GUIDE.md`: Installation, operational execution, testing, Docker, and troubleshooting guide.
  - `.env.example`: Configuration template for API keys and database credentials.

### Changed
- Standardized PySpark driver loading for PostgreSQL JDBC (`postgresql-42.7.13.jar`).
- Updated `main.py` entrypoint to orchestrate 5 distinct pipeline stages (Extract -> Transform/Staging -> dim_city -> dim_weather -> fact_weather).

---

## [1.0.0] - 2026-07-30

### Added
- Initial project scaffolding for Weather ETL Pipeline.
- `extract.py` script for querying OpenWeather API across major Indian cities (`Chennai`, `Delhi`, `Mumbai`, `Bangalore`, `Hyderabad`, `Pune`, `Kolkata`).
- Basic PySpark JSON transformation in `transform.py`.
- PostgreSQL staging table loader in `load.py`.
- `config/settings.py` environment setup using `.env`.
