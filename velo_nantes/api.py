from datetime import datetime
import requests
from rich import print
import os


def get_records() -> list:
    """Get records from API."""
    step_start = datetime.now()
    print("Getting records")
    payload = {
        "dataset": "244400404_comptages-velo-nantes-metropole",
        "rows": int(os.environ["RECORD_NUMBER"]),
        "sort": "jour",
    }
    try:
        response = requests.get(
            "https://data.nantesmetropole.fr/api/records/1.0/search", params=payload, timeout=5*60)
        data = response.json()
        print("[green]Records retrieved[/green]")
        return data["records"]
    except requests.exceptions.RequestException as exception:
        raise SystemExit(
            f"Could not get data from Nantes Metropole (step failed after {datetime.now() - step_start})") from exception


def get_weather(min_date, max_date):
    """Get weather from API."""
    step_start = datetime.now()
    print("Getting weather")
    payload = {
        "latitude": 47.22,
        "longitude": -1.55,
        "start_date": min_date.strftime('%Y-%m-%d'),
        "end_date": max_date.strftime('%Y-%m-%d'),
        "timezone": "Europe/Paris",
    }
    try:
        response = requests.get(
            "https://archive-api.open-meteo.com/v1/archive?hourly=temperature_2m,weathercode,relativehumidity_2m,cloudcover,precipitation&daily=sunrise,sunset",
            params=payload, timeout=5*60)
        print("[green]Weather retrieved[/green]")
        return response.json()
    except requests.exceptions.RequestException as exception:
        raise SystemExit(
            f"Could not get weather data (step failed after {datetime.now() - step_start})") from exception
