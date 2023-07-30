from cassandra.cluster import Cluster
from datetime import datetime
import json
import os
import uuid

# Set path to file with jsons
DATA_DIRECTORY = r'Output_results'

# Connect to the Cassandra cluster
with Cluster(
    ['127.0.0.1'],
    port=9042
) as cluster:

    with cluster.connect() as session:
        # Create the keyspace
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS my_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
                        """)

        # Switch to the keyspace
        session.set_keyspace('my_keyspace')

        # Define a function to check if a table exists in the keyspace
        def check_table_exists(table_name):
            # Get the keyspace metadata
            keyspace = session.cluster.metadata.keyspaces['my_keyspace']

            # Check if the table exists in the keyspace
            return table_name in keyspace.tables

        # Check if the table already exists in the keyspace
        if not check_table_exists('Sensors'):
            # Create the table
            session.execute("""
                CREATE TABLE Sensors (
                            id uuid PRIMARY KEY, 
                            Nazov text, 
                            Hodnota float, 
                            Kvalita BOOLEAN, 
                            Datum text)
                            """)

        # Define a function to insert data from a single JSON file
        def insert_data_from_file(filepath):
            # Load the JSON file
            with open(filepath, 'r') as json_file:
                data = json.load(json_file)

            # Insert the data into Cassandra
            for i, item in enumerate(data):
                # Assign the id to each record
                item['id'] = uuid.uuid1()

                # Extract the fields from the JSON data
                nazov = item['Nazov']
                hodnota = item['Hodnota']
                kvalita = item['Kvalita']

                # Parse the date/time string and format it in ISO 8601 format
                datum_str = item['Datum']
                if '.' in datum_str:
                    # If the datetime string contains milliseconds
                    date_format = '%Y-%m-%d %H:%M:%S.%f'
                else:
                    # If the datetime string doesn't contain milliseconds
                    date_format = '%Y-%m-%d %H:%M:%S'
                datum = datetime.strptime(
                    datum_str, date_format).isoformat() + 'Z'

                # Insert the data into Cassandra
                query = "INSERT INTO Sensors (id, nazov, hodnota, kvalita, datum) VALUES (?, ?, ?, ?, ?)"
                session.execute(
                    query, (item['id'], nazov, hodnota, kvalita, datum))

        # Loop through all JSON files in the data directory
        for filename in os.listdir(DATA_DIRECTORY):
            if filename.endswith('.json'):
                filepath = os.path.join(DATA_DIRECTORY, filename)
                insert_data_from_file(filepath)

        print("Data has been loaded into Cassandra successfully")

        # Execute the count query and retrieve the result
        query = "SELECT COUNT(*) FROM Sensors"
        result = session.execute(query)

        # Print the count
        count = result[0][0]
        print(f"Number of records: {count}")
