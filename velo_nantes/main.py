import os

from velo_nantes import database, weather_code
from dotenv import load_dotenv
import requests
from datetime import datetime
from rich import print

excluded_keys = ["jour", "jour_de_la_semaine", "boucle_num", "libelle",
                 "boucle_libelle", "total", "probabilite_presence_anomalie"]

load_dotenv()


def run():
    start_time = datetime.now()
    print("[bold green]Starting script[/bold green]")
    # Database connection
    try:
        db = database.connect(
            os.environ["MONGO_CONNECTION_STRING"], os.environ["MONGO_DATABASE_NAME"])
    except:
        raise Exception("Could not connect to database")

    record_collection = db["records"]
    date_collection = db["dates"]
    datetime_collection = db["datetimes"]
    circuit_collection = db["circuits"]

    # Drop tables
    print("[bold yellow]Dropping tables[/bold yellow]")
    try:
        record_collection.drop()
        date_collection.drop()
        datetime_collection.drop()
        circuit_collection.drop()
    except:
        raise Exception("Could not drop tables")

    records = []
    dates = {}
    datetimes = {}
    circuits = {}

    # Get records
    print("[bold yellow]Getting records[/bold yellow]")
    payload = {
        "dataset": "244400404_comptages-velo-nantes-metropole",
        "rows": int(os.environ["RECORD_NUMBER"]),
        "sort": "jour",
    }
    try:
        response = requests.get(
            "https://data.nantesmetropole.fr/api/records/1.0/search", params=payload)
        data_nantes = response.json()["records"]
    except:
        raise Exception("Could not get data from Nantes Metropole")

    print("[bold yellow]Records processing[/bold yellow]")

    min_date = datetime.fromisoformat(data_nantes[0]["fields"]["jour"])
    max_date = datetime.fromisoformat(data_nantes[0]["fields"]["jour"])

    # Data structuration
    for record in data_nantes:
        data = record["fields"]
        is_holiday = False
        if data["vacances_zone_b"] != "Hors Vacances":
            is_holiday = True

        del data["dateformat"]
        del data["vacances_zone_b"]

        date_array = data["jour"].split('-')
        current_day = datetime(int(date_array[0]), int(
            date_array[1]), int(date_array[2]))
        if current_day < min_date:
            min_date = current_day

        if current_day > max_date:
            max_date = current_day

        for hour in data.keys():
            if hour not in excluded_keys:
                datetime_string = datetime(int(date_array[0]), int(date_array[1]), int(date_array[2]),
                                           hour=int(hour),
                                           minute=0, second=0, microsecond=0, tzinfo=None, fold=0).isoformat()

                dates[data["jour"]] = {
                    "date": data["jour"],
                    "day_of_week": data["jour_de_la_semaine"],
                    "is_holiday": is_holiday,
                    "sunrise": "unknown",
                    "sunset": "unknown"
                }

                datetimes[datetime_string] = {
                    "date": data["jour"],
                    "datetime": datetime_string,
                    "temperature": "unknown",
                    "humidity": "unknown",
                    "cloud_cover": "unknown",
                    "precipitation": "unknown",
                    "weather": "unknown"
                }

                records.append({
                    "datetime": datetime_string,
                    "circuit_num": data["boucle_num"],
                    "bikes": data[hour],
                })
        if "libelle" in data:
            circuits[data["boucle_num"]] = {
                "circuit_num": data["boucle_num"],
                "circuit_libelle": data["libelle"]
            }

    # get weather
    print("[bold yellow]Getting weather[/bold yellow]")
    payload = {
        "latitude": 47.22,
        "longitude": -1.55,
        "start_date": min_date.isoformat().split('T')[0],
        "end_date": max_date.isoformat().split('T')[0],
        "timezone": "Europe/Paris",
    }
    try:
        response = requests.get(
            "https://archive-api.open-meteo.com/v1/archive?hourly=temperature_2m,weathercode,relativehumidity_2m,cloudcover,precipitation&daily=sunrise,sunset",
            params=payload)
        data_weather = response.json()
    except:
        raise Exception("[bold red]Could not get weather data[/bold red]")

    print("[bold yellow]Weather processing[/bold yellow]")
    for index, hour in enumerate(data_weather["hourly"]["time"]):
        datetime_string = datetime.fromisoformat(hour).isoformat()
        if datetime_string not in datetimes:
            continue

        datetimes[datetime_string]["temperature"] = data_weather["hourly"]["temperature_2m"][index]
        datetimes[datetime_string]["humidity"] = data_weather["hourly"]["relativehumidity_2m"][index]
        datetimes[datetime_string]["cloud_cover"] = data_weather["hourly"]["cloudcover"][index]
        datetimes[datetime_string]["precipitation"] = data_weather["hourly"]["precipitation"][index]
        datetimes[datetime_string]["weather"] = weather_code.code_to_string(
            data_weather["hourly"]["weathercode"][index])

    for index, day in enumerate(data_weather["daily"]["time"]):
        if day not in dates:
            continue

        dates[day]["sunrise"] = datetime.fromisoformat(
            data_weather["daily"]["sunrise"][index]).isoformat()
        dates[day]["sunset"] = datetime.fromisoformat(
            data_weather["daily"]["sunset"][index]).isoformat()

    # Insert datas
    print("[bold yellow]Inserting data[/bold yellow]")
    try:
        record_collection.insert_many(records)
        date_collection.insert_many(dates.values())
        datetime_collection.insert_many(datetimes.values())
        circuit_collection.insert_many(circuits.values())
    except:
        raise Exception("Could not insert datas")

    end_time = datetime.now()

    print(
        f"[bold green]Ending script ({len(records)} records in {end_time - start_time})[/bold green]")
