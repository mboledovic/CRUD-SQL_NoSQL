import json
import os
import redis

# Set the data directory path
DATA_DIRECTORY = r'Output_results'

# Connect to Redis instance using a context manager
with redis.Redis(host='localhost', port=6379, db=1) as session:
    # Define function to store record in Redis
    def store_record(record_id, record):
        # Check if the record ID already exists
        if not session.exists(f'record:{record_id}'):
            record_json = json.dumps(record)
            session.set(f'record:{record_id}', record_json)

    # Loop through all JSON files in the data directory
    for filename in os.listdir(DATA_DIRECTORY):
        if filename.endswith('.json'):
            filepath = os.path.join(DATA_DIRECTORY, filename)
            # Read JSON data from file
            with open(filepath, 'r') as f:
                records = json.load(f)

            # Store records in Redis
            for i, record in enumerate(records):
                record_id = session.incr('record_id')  # Generate the record ID
                store_record(record_id, record)

    print("Data has been loaded into Redis successfully")

    # Print the number of records stored in Redis
    num_records = session.dbsize()
    print(f"Number of records in Redis: {num_records}")
