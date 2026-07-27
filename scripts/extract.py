from config.settings import API_KEY
import requests

CITY = "Chennai"

url = (
    f"https://api.openweathermap.org/data/2.5/weather"
    f"?q={CITY}&appid={API_KEY}&units=metric"
)

response = requests.get(url)

print(response.status_code)
print(response.json())