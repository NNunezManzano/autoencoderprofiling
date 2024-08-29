from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import json
import os
import logging
from dotenv import load_dotenv
import re

'''
import sys
import collections

if sys.version_info >= (3, 10):
    import collections.abc as collections_abc
    collections.Callable = collections_abc.Callable

import pdbp
'''

#TODO: Comentar

class Mongoconnect:
    
    def __init__(self, mgUser, mgPass, mgCluster, appName):
        self.mgUser     = mgUser
        self.mgPass     = mgPass
        self.mgCluster  = mgCluster
        self.appName    = appName

        uri = f"mongodb+srv://{self.mgUser}:{self.mgPass}@{self.mgCluster}/?appName={self.appName}"        

        # Create a new client and connect to the server
        self.client = MongoClient(uri, server_api=ServerApi('1'))

        # Setup logging
        logging.basicConfig(filename='mongo_connect.log', level=logging.ERROR,
                            format='%(asctime)s:%(levelname)s:%(message)s')

        # Send a ping to confirm a successful connection
        try:
            self.client.admin.command('ping')
            print("Successfully connected to MongoDB!")
        except Exception as e:
            logging.error("Failed to connect to MongoDB", exc_info=True)
            print(e)

    def dbConnect(self, dbName: str) -> MongoClient:
        try:
            db = self.client[dbName]
            return db
        except Exception as e:
            logging.error(f"Failed to connect to database: {dbName}", exc_info=True)
            print(e)
            return None
    
    def collectionConnect(self, db: MongoClient, collName: str) -> MongoClient:
        try:
            collection = db[collName]
            return collection
        except Exception as e:
            logging.error(f"Failed to connect to collection: {collName}", exc_info=True)
            print(e)
            return None
        
    def insertDoc(self, collection: MongoClient, document: dict) -> str:
        try:
            result = collection.insert_one(document)
            return result.inserted_id
        except Exception as e:
            logging.error("Failed to insert document", exc_info=True)
            print(e)
            return None
    
    def dropDocs(self, collection: MongoClient, expression='.*Alpha Vantage*') -> str:
        rgx = re.compile(expression, re.IGNORECASE) 
        collection.delete_many({'Information':rgx})
        collection.delete_many({'note':rgx})
        return 'Mongo db cleared'
# Example usage:
if __name__ == "__main__":
    # Load sensitive information from environment variables for better security
    load_dotenv()
    mgUser = os.getenv('MONGO_USER')
    mgPass = os.getenv('MONGO_KEY')
    mgCluster = os.getenv('MONGO_CLUSTER')
    appName =  os.getenv("MONGO_APP")
    

    # Ensure environment variables are set
    if not mgUser or not mgPass or not mgCluster:
        raise EnvironmentError("Please set the environment variables for MongoDB credentials")

    mongo_conn = Mongoconnect(mgUser, mgPass, mgCluster, appName)

    # Example of connecting to a database and collection, and inserting a document
    db = mongo_conn.dbConnect("example_db")
    if not db == None:
        collection = mongo_conn.collectionConnect(db, "example_collection")
        if not collection == None:
            document = {"name": "John Doe", "age": 30}
            inserted_id = mongo_conn.insertDoc(collection, document)
            print(f"Inserted document ID: {inserted_id}")
    