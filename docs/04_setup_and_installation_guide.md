# 04. Setup and Installation Guide

## System Prerequisites

Before configuring and executing the pipeline, ensure the following core dependencies are installed on the host machine:

| Component | Minimum Version | Recommended | Verification Command |
| :--- | :--- | :--- | :--- |
| **Python** | 3.10+ | 3.11 / 3.12 | `python --version` |
| **Java Development Kit (JDK)** | OpenJDK 8 / 11 / 17 | JDK 17 (Eclipse Temurin) | `java -version` |
| **PostgreSQL** | 14.x | 16.x or Docker container | `psql --version` |
| **Git** | 2.30+ | Latest | `git --version` |
| **Docker & Docker Compose** | v20.10+ | Docker Desktop 4.x | `docker compose version` |

---

## Step 1: Clone the Repository

```bash
git clone https://github.com/ARULKINT/weather_data_eng.git
cd weather_data_eng
```

---

## Step 2: Configure Environment Variables

The project uses `.env` for secrets and database configurations. Copy the template:

### Windows (PowerShell):
```powershell
Copy-Item .env.example .env
```

### Linux / macOS:
```bash
cp .env.example .env
```

Edit `.env` using your preferred text editor and furnish your credentials:
```ini
# OpenWeather API Key (Obtain from https://openweathermap.org/api)
OPENWEATHER_API_KEY=your_actual_openweather_api_key

# PostgreSQL Connection Credentials
DB_HOST=localhost
DB_PORT=5432
DB_NAME=weather_db
DB_USER=postgres
DB_PASSWORD=your_secure_password
```

---

## Step 3: Python Environment & Dependencies

### Option A: Using Standard Virtual Environment (Recommended)

#### On Windows (PowerShell):
```powershell
# Create virtual environment
python -m venv venv

# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Upgrade package manager and install dependencies
python -m pip install --upgrade pip
pip install -r requirements.txt
```

#### On Linux / macOS:
```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Upgrade package manager and install dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

---

## Step 4: Java Runtime Configuration (Required for PySpark)

PySpark requires a valid Java runtime (`JAVA_HOME`):
1. **Windows**:
   - Download and install [Eclipse Temurin JDK 17](https://adoptium.net/).
   - Set environment variable: `[System.Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Eclipse Adoptium\jdk-17...", "Machine")`
   - Ensure `%JAVA_HOME%\bin` is on your system `PATH`.
2. **Linux (Ubuntu/Debian)**:
   ```bash
   sudo apt-get update
   sudo apt-get install -y openjdk-17-jdk
   export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
   ```

---

## Step 5: Database Initialization

### Option A: Using Docker Compose (Zero local PostgreSQL install required)
Run the pre-configured PostgreSQL container:
```bash
docker compose up -d postgres
```
This automatically boots a PostgreSQL instance and mounts the healthcheck.

### Option B: Using Existing Local PostgreSQL
Log in to your local PostgreSQL server and create the database and tables:
```bash
psql -U postgres -h localhost -c "CREATE DATABASE weather_db;"
psql -U postgres -h localhost -d weather_db -f database/schema.sql
```

Verify tables were created successfully:
```bash
psql -U postgres -h localhost -d weather_db -c "\dt"
```
Expected output:
```
              List of relations
 Schema |     Name     | Type  |  Owner   
--------+--------------+-------+----------
 public | dim_city     | table | postgres
 public | dim_weather  | table | postgres
 public | fact_weather | table | postgres
 public | weather_data | table | postgres
```

---

## Step 6: Verify PostgreSQL JDBC Driver

The repository includes the certified JDBC driver at:
`drivers/postgresql-42.7.13.jar`

Verify driver existence:
```powershell
Test-Path drivers\postgresql-42.7.13.jar
```
If missing on a fresh pull, download it:
```bash
curl -o drivers/postgresql-42.7.13.jar https://jdbc.postgresql.org/download/postgresql-42.7.13.jar
```

---

## Step 7: Sanity Test Verification

Verify installation by running the unit test suite:
```powershell
python -m unittest tests/test_validator.py tests/test_settings.py
```
If all tests pass, the environment is ready for pipeline execution!
