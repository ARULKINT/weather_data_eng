# ============================================================
# Dockerfile for Weather Data Engineering Pipeline
# ============================================================

FROM python:3.12-slim

# Install Java OpenJDK (Required for PySpark) & system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt-get/lists/*

# Set Java Home environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

# Set Python path
ENV PYTHONPATH=/app

# Command to execute the main ETL pipeline
CMD ["python", "scripts/main.py"]
