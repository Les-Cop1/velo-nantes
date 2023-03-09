import pymongo
from datetime import datetime
from rich import print


def connect(mongo_connection_string: str, mongo_database: str):
    """ connect and return mongo db """
    step_start = datetime.now()
    print("Connexion à la base de données")
    try:
        mongo = pymongo.MongoClient(mongo_connection_string)
        database = mongo[mongo_database]
        print("[green]Base de données connectée[/green]")
        return database
    except Exception as exception:
        raise SystemExit(
            f"Erreur de connexion à la base de données (step failed after {datetime.now() - step_start})") from exception


def drop_tables(collections: list):
    """ drop tables """
    step_start = datetime.now()
    print("Dropping tables")
    try:
        for collection in collections:
            collection.drop()
        print("[green]Tables dropped[/green]")
    except Exception as exception:
        raise SystemExit(
            f"Erreur lors de la suppression des tables (step failed after {datetime.now() - step_start})") from exception


def insert_in_collection(collection, data: list):
    """ insert data in collection """
    step_start = datetime.now()
    try:
        collection.insert_many(data)
    except Exception as exception:
        raise SystemExit(
            f"Erreur lors de l'insertion des données (step failed after {datetime.now() - step_start})") from exception
