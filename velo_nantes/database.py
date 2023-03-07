import logging
import pymongo


def connect(mongo_connection_string: str, mongo_database: str):
    """ connect and return mongo db """
    logging.info("Connexion à la base de données")
    try:
        mongo = pymongo.MongoClient(mongo_connection_string)
        logging.info("Base de données connectée")
    except Exception as e:
        error = '[{0}] MongoClient connection error: {1}.'.format(
            str(type(e)), str(e))
        logging.error(error)
        raise Exception(error)
    db = mongo[mongo_database]
    return db
