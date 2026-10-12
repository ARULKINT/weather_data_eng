# 🌐 OpenWeather API & Data Validation Specification

## Weather Data Engineering Pipeline

---

## 📡 API Source Overview

- **Provider**: OpenWeather Current Weather Data API
- **Endpoint**: `https://api.openweathermap.org/data/2.5/weather`
- **Method**: HTTP GET
- **Authentication**: API Key (`appid` query parameter)
- **Response Format**: JSON
- **Units**: Metric (`units=metric` -> Celsius, m/s, hPa)

---

## 📋 Request Parameters

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `q` | String | Yes | City name | `Chennai` |
| `appid` | String | Yes | OpenWeather API authentication key | `YOUR_API_KEY` |
| `units` | String | Yes | Unit standard (`metric`, `imperial`, `standard`) | `metric` |

---

## 📄 Raw API Response Payload Example

```json
{
    "coord": {
        "lon": 80.2785,
        "lat": 13.0878
    },
    "weather": [
        {
            "id": 801,
            "main": "Clouds",
            "description": "few clouds",
            "icon": "02d"
        }
    ],
    "base": "stations",
    "main": {
        "temp": 31.99,
        "feels_like": 37.99,
        "temp_min": 31.99,
        "temp_max": 31.99,
        "pressure": 1007,
        "humidity": 62
    },
    "visibility": 6000,
    "wind": {
        "speed": 4.12,
        "deg": 140
    },
    "clouds": {
        "all": 20
    },
    "dt": 1722299604,
    "sys": {
        "type": 1,
        "id": 9205,
        "country": "IN",
        "sunrise": 1722299100,
        "sunset": 1722344700
    },
    "timezone": 19800,
    "id": 1264527,
    "name": "Chennai",
    "cod": 200
}
```

---

## ✅ Data Validation Rules (`scripts/validator.py`)

The pipeline implements strict validation before any raw JSON payload is persisted to `data/raw/`.

```
Raw API JSON
     │
     ├── 1. Check City Name ──► Must be non-empty String
     │
     ├── 2. Check Coordinates ──► Lat: [-90, 90], Lon: [-180, 180]
     │
     ├── 3. Check Main Metrics ──► Temp present, Humidity [0, 100], Pressure > 0
     │
     ├── 4. Check Wind Speed ──► Speed >= 0
     │
     └── 5. Check Weather Tag ──► Main & Description present in array
```

### Validation Error Handling
If validation fails:
1. `validator.py` raises a `ValueError` detailing the invalid property.
2. `extract.py` catches `ValueError`, logs the exception with city context, and skips saving the payload.

---

## 🔄 Schema Mapping Matrix

| Raw API JSON Field | PySpark Column Name | PySpark Data Type | Staging Table Column | Dimension / Fact Target |
|--------------------|---------------------|-------------------|----------------------|-------------------------|
| `name` | `city` | `StringType` | `weather_data.city` | `dim_city.city_name` |
| `coord.lat` | `latitude` | `DoubleType` | `weather_data.latitude` | `dim_city.latitude` |
| `coord.lon` | `longitude` | `DoubleType` | `weather_data.longitude` | `dim_city.longitude` |
| `main.temp` | `temperature` | `DoubleType` | `weather_data.temperature` | `fact_weather.temperature` |
| `main.humidity` | `humidity` | `IntegerType` | `weather_data.humidity` | `fact_weather.humidity` |
| `main.pressure` | `pressure` | `IntegerType` | `weather_data.pressure` | `fact_weather.pressure` |
| `wind.speed` | `wind_speed` | `DoubleType` | `weather_data.wind_speed` | `fact_weather.wind_speed` |
| `weather[0].main` | `weather` | `StringType` | `weather_data.weather` | `dim_weather.weather` |
| `weather[0].description` | `description` | `StringType` | `weather_data.description` | `dim_weather.description` |
| `N/A (Generated)` | `collected_at` | `TimestampType` | `weather_data.collected_at` | `fact_weather.collected_at` |
