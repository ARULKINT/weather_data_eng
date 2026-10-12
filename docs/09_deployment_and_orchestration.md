# 09. Deployment and Orchestration

## Deployment Topologies

The Weather Data Engineering Pipeline supports three primary deployment architectures:

```
┌─────────────────────────┐    ┌─────────────────────────┐    ┌─────────────────────────┐
│  1. LOCAL WORKSTATION   │    │  2. CONTAINERIZED COMPOSE│    │ 3. ENTERPRISE ORCHESTRATION
│  • PowerShell / Batch   │    │  • Docker + Compose     │    │  • Apache Airflow       │
│  • Local Python + Spark │    │  • Isolated PostgreSQL  │    │  • Production Cloud     │
└─────────────────────────┘    └─────────────────────────┘    └─────────────────────────┘
```

---

## 1. Containerized Deployment (Docker & Compose)

The repository provides a production-ready `Dockerfile` and `docker-compose.yml`.

### Docker Architecture
- **Service 1 (`postgres`)**: PostgreSQL 16 container with persistent volume storage (`postgres_data`) and port exposure on `5432`.
- **Service 2 (`etl_pipeline`)**: Python 3.11 environment with OpenJDK 17, Apache Spark, and dependencies pre-baked.

### Running with Docker Compose
```bash
# Start PostgreSQL service
docker compose up -d postgres

# Execute pipeline inside container
docker compose run --rm etl_pipeline

# Check container logs
docker compose logs -f postgres
```

---

## 2. Operating System Scheduling

### Option A: Windows Task Scheduler (Local / Windows Server)

You can schedule the pipeline to run every hour using Windows Task Scheduler and the provided `run_pipeline.ps1`:

1. Open **Task Scheduler** (`taskschd.msc`).
2. Click **Create Basic Task**.
3. **Trigger**: Select *Daily* or *Repeat task every: 1 hour*.
4. **Action**: *Start a program*.
5. **Program/script**: `powershell.exe`
6. **Arguments**: `-ExecutionPolicy Bypass -File "C:\drive g\Projects_july\weather_etl_api\run_pipeline.ps1"`
7. **Start in**: `C:\drive g\Projects_july\weather_etl_api`

Or register via PowerShell Administrator prompt:
```powershell
$Action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument '-ExecutionPolicy Bypass -File "C:\drive g\Projects_july\weather_etl_api\run_pipeline.ps1"' -WorkingDirectory "C:\drive g\Projects_july\weather_etl_api"
$Trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Hours 1) -RepetitionDuration ([TimeSpan]::MaxValue)
Register-ScheduledTask -TaskName "Weather_ETL_Pipeline" -Action $Action -Trigger $Trigger -Description "Hourly Weather Telemetry ETL"
```

---

### Option B: Linux Crontab (Linux Server / EC2)

To run the pipeline every 30 minutes on a Linux host:
```bash
crontab -e
```
Add the cron rule:
```cron
*/30 * * * * cd /opt/weather_data_eng && /opt/weather_data_eng/venv/bin/python -m scripts.main >> /opt/weather_data_eng/logs/cron.log 2>&1
```

---

## 3. Enterprise Orchestration with Apache Airflow

For enterprise data platforms, the pipeline can be orchestrated as a directed acyclic graph (DAG) in Apache Airflow:

```python
"""
dags/weather_etl_pipeline_dag.py
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['alerts@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='weather_data_engineering_pipeline',
    default_args=default_args,
    description='Hourly extraction, transformation, and dimensional loading of weather telemetry',
    schedule_interval='@hourly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'etl', 'pyspark', 'warehouse'],
) as dag:

    run_pipeline_task = BashOperator(
        task_id='execute_weather_etl',
        bash_command='cd /opt/weather_data_eng && ./venv/bin/python -m scripts.main',
        execution_timeout=timedelta(minutes=15),
    )
```

---

## 4. Spark Resource Tuning for Production

For scaling beyond local testing to larger batch volumes, adjust Spark configurations in `scripts/main.py`:

```python
spark = (
    SparkSession.builder
    .appName("Weather Data Engineering Pipeline")
    .master("local[*]")
    # Allocate heap memory
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    # Parallelism tuning
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.default.parallelism", "8")
    # Driver Classpath
    .config("spark.jars", jdbc_jar)
    .config("spark.driver.extraClassPath", jdbc_jar)
    .getOrCreate()
)
```
