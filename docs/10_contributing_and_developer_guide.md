# 10. Contributing and Developer Guide

## Developer Environment Setup

1. Fork and clone the repository:
   ```bash
   git clone https://github.com/ARULKINT/weather_data_eng.git
   cd weather_data_eng
   ```
2. Create your isolated development branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. Set up the local virtual environment and install dev dependencies:
   ```bash
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```

---

## Coding Standards and Style Guidelines

### 1. Python Code Style (PEP 8)
- Maintain clear docstrings on all functions and modules explaining:
  - Purpose
  - Parameters
  - Return values
  - Raised exceptions
- Enforce explicit type hints on function signatures:
  ```python
  def read_table(spark: SparkSession, table_name: str) -> DataFrame:
  ```
- Avoid wildcard imports (`from module import *`). Explicitly specify all imported symbols.

### 2. Logging Over Printing
- **Never use `print()` in production pipeline code**.
- Always use the centralized logger from `scripts.logger`:
  ```python
  from scripts.logger import logger
  logger.info("Informative status message.")
  logger.warning("Potential issue detected.")
  logger.exception("Error context with automatic stack trace.")
  ```

### 3. Cross-Platform Path Handling
- Always use Python's standard `pathlib.Path` instead of raw string concatenation.
- Avoid hardcoded Unix `/` or Windows `\` path separators.
- Use `.resolve()` when passing jar or file paths to Java/PySpark runtimes.

---

## How to Add a New Monitored City

Adding a new geographic target is seamless and requires zero code modifications:

1. Open `config/settings.py`.
2. Append the target municipality name to the `CITIES` list:
   ```python
   CITIES = [
       "Chennai",
       "Delhi",
       "Mumbai",
       "Bangalore",
       "Hyderabad",
       "Pune",
       "Kolkata",
       "Coimbatore",  # <--- New city
       "Ahmedabad"    # <--- New city
   ]
   ```
3. Run the pipeline:
   ```powershell
   .\run_pipeline.ps1
   ```
4. The pipeline will automatically:
   - Extract weather for the new city from the API.
   - Insert raw JSON into `data/raw/`.
   - Stage rows in `weather_data`.
   - Incrementally detect the new city via `LEFT ANTI JOIN` and insert into `dim_city`.
   - Record the corresponding fact row in `fact_weather`.

---

## Git Commit & Pull Request Workflow

### Conventional Commit Messages
Follow conventional commit formatting:
- `feat: Add support for hourly barometric pressure variance`
- `fix: Resolve null reference in wind gust array`
- `docs: Expand SQL analytics documentation`
- `test: Add edge case boundary tests for negative temperatures`
- `refactor: Optimize Left Anti Join memory partition count`

### Pre-PR Checklist
Before pushing code to your remote branch and opening a Pull Request:
- [ ] Run fast unit tests: `python -m unittest tests/test_validator.py tests/test_settings.py`
- [ ] Run PySpark transform test: `python -m unittest tests/test_transform.py`
- [ ] Verify that no secrets or `.env` files are tracked by Git (`git status`)
- [ ] Ensure all modified functions contain up-to-date docstrings
- [ ] Verify that new or changed SQL queries conform to PostgreSQL 15+ standards
