import json
import os
from pymongo import MongoClient

# Set path to file with jsons
DATA_DIRECTORY = r'Output_results'

CONNECTION_STRING = 'mongodb://localhost:27017'
DB_NAME = 'test'
COLLECTION_NAME = 'mycollection'


# Connect to the MongoDB
with MongoClient(CONNECTION_STRING) as client:
    db = client[DB_NAME]
    mycollection = db[COLLECTION_NAME]

    # Loop through all JSON files in the data directory
    for filename in os.listdir(DATA_DIRECTORY):
        if filename.endswith('.json'):
            filepath = os.path.join(DATA_DIRECTORY, filename)
            # Load JSON data from file
            with open(filepath, 'r') as file_json:
                data = json.load(file_json)
            # Insert data into MongoDB collection
            x = mycollection.insert_many(data)

    print("Data has been loaded into Mongo successfully")

    count = mycollection.count_documents({})
    # Print the count of records
    print("Number of records: ", count)
