# pip install psycopg2-binary

# import csv
import json
import os
import psycopg2 as psy


# directory for all .csv to upload
directory = r'Output_results'

with psy.connect(
    host='localhost',
    port=5431,
    user='test',
    password='password'
) as conn:

    with conn.cursor() as curr:
        curr.execute("""
            CREATE TABLE Sensors (
                     ID serial PRIMARY KEY, 
                     Nazov varchar ( 50 ) NOT NULL, 
                     Hodnota varchar ( 50 ) NOT NULL, 
                     Kvalita varchar ( 50 ) NOT NULL, 
                     Datum varchar ( 50 ) NOT NULL
                     );
        """)
    print("Table created")

# This will load data to database from multiple .json format files

    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            filepath = os.path.join(directory, filename)
            with open(filepath, 'r') as jsonfile:
                # Read JSON data
                json_data = json.load(jsonfile)
                # Insert data into table Sensors
                with conn.cursor() as cursor:
                    for row in json_data:
                        cursor.execute("INSERT INTO Sensors (Nazov, Hodnota, Kvalita, Datum) VALUES (%s, %s, %s, %s)", (
                            row['Nazov'], row['Hodnota'], row['Kvalita'], row['Datum']))
                conn.commit()

# This will load data to database from multiple .csv format files

    # for filename in os.listdir(directory):
    #     if filename.endswith(".csv"):
    #         filepath = os.path.join(directory, filename)
    #         with open(filepath, 'r') as csvfile:
    #             # Read CSV data
    #             csv_data = csv.reader(csvfile)
    #             header = next(csv_data)
    #             # Insert data into database
    #             with conn.cursor() as cursor:
    #                 for row in csv_data:
    #                     curr.execute("INSERT INTO Sensors (Nazov, Hodnota, Kvalita, Datum) VALUES (%s, %s, %s, %s)",row)
    #             conn.commit()

    print("Data has been loaded into Cassandra successfully")

    with conn.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) FROM Sensors')
        count = cursor.fetchone()[0]

    # Print the count of records
    print("Number of records: ", count)
