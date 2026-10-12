-- ============================================================
-- Database Schema DDL for Weather Data Warehouse
-- Project : Weather Data Engineering Pipeline
-- Author  : Arul G
-- RDBMS   : PostgreSQL
-- ============================================================

-- Create Database (Run separately if needed)
-- CREATE DATABASE weather_db;
-- \c weather_db;

-- ============================================================
-- 1. STAGING TABLE: weather_data
-- Stores raw transformed observations loaded by PySpark.
-- ============================================================

CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    humidity INT NOT NULL,
    pressure INT NOT NULL,
    wind_speed DOUBLE PRECISION NOT NULL,
    weather VARCHAR(50) NOT NULL,
    description VARCHAR(100) NOT NULL,
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_weather_data_city ON weather_data (city);
CREATE INDEX IF NOT EXISTS idx_weather_data_collected_at ON weather_data (collected_at);

-- ============================================================
-- 2. DIMENSION TABLE: dim_city
-- Stores unique city metadata with auto-incrementing primary key.
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_city (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) UNIQUE NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_city_name ON dim_city (city_name);

-- ============================================================
-- 3. DIMENSION TABLE: dim_weather
-- Stores unique weather conditions and descriptions.
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_weather (
    weather_id SERIAL PRIMARY KEY,
    weather VARCHAR(50) NOT NULL,
    description VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_weather_desc UNIQUE (weather, description)
);

CREATE INDEX IF NOT EXISTS idx_dim_weather_type ON dim_weather (weather);

-- ============================================================
-- 4. FACT TABLE: fact_weather
-- Stores weather metrics linking city_id and weather_id.
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_weather (
    fact_id SERIAL PRIMARY KEY,
    city_id INT NOT NULL REFERENCES dim_city(city_id) ON DELETE CASCADE,
    weather_id INT NOT NULL REFERENCES dim_weather(weather_id) ON DELETE CASCADE,
    temperature DOUBLE PRECISION NOT NULL,
    humidity INT NOT NULL,
    pressure INT NOT NULL,
    wind_speed DOUBLE PRECISION NOT NULL,
    collected_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fact_observation UNIQUE (city_id, weather_id, collected_at)
);

CREATE INDEX IF NOT EXISTS idx_fact_city ON fact_weather (city_id);
CREATE INDEX IF NOT EXISTS idx_fact_weather ON fact_weather (weather_id);
CREATE INDEX IF NOT EXISTS idx_fact_collected ON fact_weather (collected_at);

-- ============================================================
-- 5. ANALYTICAL VIEWS
-- Useful views for reporting and Power BI integration.
-- ============================================================

-- View: Detailed Latest City Weather
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

-- View: Daily Aggregated Weather Summary
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
