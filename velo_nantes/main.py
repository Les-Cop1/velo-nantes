import os

from velo_nantes import database, weather_code, api
from dotenv import load_dotenv
from datetime import datetime
from rich import print
from zoneinfo import ZoneInfo

excluded_keys = ["jour", "jour_de_la_semaine", "boucle_num", "libelle",
                 "boucle_libelle", "total", "probabilite_presence_anomalie"]

load_dotenv()


def run():
    """Run the velo-nantes script"""
    start_time = datetime.now()

    print("[bold green]Starting script[/bold green]")

    # Database connection
    mongo = database.connect(
        os.environ["MONGO_CONNECTION_STRING"], os.environ["MONGO_DATABASE_NAME"])

    record_collection = mongo["records"]
    date_collection = mongo["dates"]
    datetime_collection = mongo["datetimes"]
    circuit_collection = mongo["circuits"]

    # Drop tables
    database.drop_tables(
        [record_collection, date_collection, datetime_collection, circuit_collection])

    records = []
    dates = {}
    datetimes = {}
    circuits = {}

    # Get records
    data_nantes = api.get_records()

    print("Records processing")

    min_date = datetime.fromisoformat(data_nantes[0]["fields"]["jour"]).replace(tzinfo=ZoneInfo('Europe/Paris'))
    max_date = datetime.fromisoformat(data_nantes[0]["fields"]["jour"]).replace(tzinfo=ZoneInfo('Europe/Paris'))

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
            date_array[1]), int(date_array[2]), tzinfo=ZoneInfo('Europe/Paris'))

        if current_day < min_date:
            min_date = current_day

        if current_day > max_date:
            max_date = current_day

        for hour in data.keys():
            if hour not in excluded_keys:
                datetime_obj = datetime(int(date_array[0]), int(date_array[1]), int(date_array[2]),
                                        hour=int(hour),
                                        minute=0, second=0, microsecond=0, tzinfo=ZoneInfo('Europe/Paris'))

                dates[current_day.isoformat()] = {
                    "date": current_day,
                    "day_of_week": data["jour_de_la_semaine"],
                    "is_holiday": is_holiday,
                    "sunrise": None,
                    "sunset": None
                }

                datetimes[datetime_obj.isoformat()] = {
                    "date": current_day,
                    "datetime": datetime_obj,
                    "temperature": None,
                    "humidity": None,
                    "cloud_cover": None,
                    "precipitation": None,
                    "weather": None
                }

                records.append({
                    "date": current_day,
                    "datetime": datetime_obj,
                    "circuit_num": data["boucle_num"],
                    "bikes": data[hour],
                })
        if "libelle" in data:
            circuits[data["boucle_num"]] = {
                "circuit_num": data["boucle_num"],
                "circuit_libelle": data["libelle"]
            }

    print("[green]Records processed[/green]")

    # get weather
    data_weather = api.get_weather(min_date, max_date)

    print("Weather processing")
    for index, hour in enumerate(data_weather["hourly"]["time"]):
        datetime_string = datetime.fromisoformat(hour).replace(tzinfo=ZoneInfo('Europe/Paris')).isoformat()

        if datetime_string not in datetimes:
            continue

        datetimes[datetime_string]["temperature"] = data_weather["hourly"]["temperature_2m"][index]
        datetimes[datetime_string]["humidity"] = data_weather["hourly"]["relativehumidity_2m"][index]
        datetimes[datetime_string]["cloud_cover"] = data_weather["hourly"]["cloudcover"][index]
        datetimes[datetime_string]["precipitation"] = data_weather["hourly"]["precipitation"][index]
        datetimes[datetime_string]["weather"] = weather_code.code_to_string(
            data_weather["hourly"]["weathercode"][index])

    for index, day in enumerate(data_weather["daily"]["time"]):
        date_string = datetime.fromisoformat(day).replace(tzinfo=ZoneInfo('Europe/Paris')).isoformat()
        if date_string not in dates:
            continue

        dates[date_string]["sunrise"] = datetime.fromisoformat(
            data_weather["daily"]["sunrise"][index]).replace(tzinfo=ZoneInfo('Europe/Paris'))
        dates[date_string]["sunset"] = datetime.fromisoformat(
            data_weather["daily"]["sunset"][index]).replace(tzinfo=ZoneInfo('Europe/Paris'))

    print("[green]Weather processed[/green]")

    # Insert datas
    print("Inserting data")
    database.insert_in_collection(record_collection, records)
    database.insert_in_collection(date_collection, dates.values())
    database.insert_in_collection(datetime_collection, datetimes.values())
    database.insert_in_collection(circuit_collection, circuits.values())
    print("[green]Data inserted[/green]")

    end_time = datetime.now()

    print(
        f"[bold green]Ending script ({len(records)} records in {end_time - start_time})[/bold green]")
