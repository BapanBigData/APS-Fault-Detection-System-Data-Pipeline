import os
from dotenv import load_dotenv
import pymongo

# Load environment variables from .env file
load_dotenv()


class MongodbOperation:
    
    # Connection string
    CONN_STRING = os.getenv('MONGODB_CONN_STRING')

    def __init__(self, db_name) -> None:

        self.client = pymongo.MongoClient(MongodbOperation.CONN_STRING)
        self.db_name = db_name

    def insert_many(self,collection_name,records:list):
        self.client[self.db_name][collection_name].insert_many(records)

    def insert(self,collection_name,record):
        self.client[self.db_name][collection_name].insert_one(record)
        
