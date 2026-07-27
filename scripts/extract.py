from config.settings import API_KEY

import requests
import json
from datetime import datetime
from pathlib import Path

CITY = "Chennai"

url = (
    f"https://api.openweathermap.org/data/2.5/weather"
    f"?q={CITY}&appid={API_KEY}&units=metric"
)

response = requests.get(url)

if response.status_code == 200:
    weather_data = response.json()

    raw_data_dir = Path("data/raw")
    raw_data_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = raw_data_dir / f"weather_{timestamp}.json"

    with open(file_path, "w") as file:
        json.dump(weather_data, file, indent=4)

    print(f"✅ Weather data saved to: {file_path}")

else:
    print(f"❌ Error: {response.status_code}")
    print(response.text)