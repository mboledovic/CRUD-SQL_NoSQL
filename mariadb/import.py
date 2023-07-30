import os
import mariadb
import sys
import csv
import json

# Directory for all files with data to upload
directory = r'Output_results'

# Connection to create the schema 
with mariadb.connect(
    user="root", 
    password="password", 
    host="localhost", 
    port=3307
    ) as con:

    cur = con.cursor()
    cur.execute("CREATE SCHEMA test DEFAULT CHARACTER SET utf8mb4;")
    print("Schema created")

# Connection to the database 
with mariadb.connect(
    user="root", 
    password="password", 
    host="localhost", 
    port=3307, 
    database="test"
    ) as conn:
    curr = conn.cursor()
    curr.execute("""
        CREATE TABLE Sensors (
            ID serial PRIMARY KEY, 
            Nazov varchar(50) NOT NULL, 
            Hodnota varchar(50) NOT NULL, 
            Kvalita varchar(50) NOT NULL, 
            Datum varchar(50) NOT NULL);
                 """)
    print("Table created")


    # Load data from multiple .json files
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            filepath = os.path.join(directory, filename)
            with open(filepath, 'r') as jsonfile:
                # Read JSON data
                json_data = json.load(jsonfile)
                header = list(json_data[0].keys())
                # Insert data into the database
                for row in json_data:
                    values = [row[key] for key in header]
                    curr.execute("INSERT INTO Sensors (Nazov, Hodnota, Kvalita, Datum) VALUES (?, ?, ?, ?)", values)
                conn.commit()

    # Load data from multiple csv files
    
    # for filename in os.listdir(directory):
        # if filename.endswith(".csv"):
            # filepath = os.path.join(directory, filename)
            # with open(filepath, 'r') as csvfile:
                #Read CSV data
                # csv_data = csv.reader(csvfile)
                # header = next(csv_data)
                #Insert data into the database
                # for row in csv_data:
                    # curr.execute("INSERT INTO Sensors (Nazov, Hodnota, Kvalita, Datum) VALUES (?, ?, ?, ?)", row)
                # conn.commit()
    
    print("Data has been loaded into Mariadb successfully")

    curr.execute('SELECT COUNT(*) FROM Sensors')
    count = curr.fetchone()[0]

    # Print the count of records
    print("Number of records: ", count)


