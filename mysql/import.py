# pip install pymysql
# pip install mysql.connector

# import csv
import json
import pymysql
import pymysql.cursors
import os


# Set the road to data
# os.path.normpath
directory = r'Output_results'


# Connect to the MySQL database using a context manager
with pymysql.connect(
    host="localhost",
    port=3307,
    user='test',
    password='password',
    database='test',
    cursorclass=pymysql.cursors.DictCursor
) as conn:
    # Create a table called "Sensors" if it doesn't exist
    with conn.cursor() as mycursor:
        mycursor.execute("""
            CREATE TABLE IF NOT EXISTS Sensors (
                ID INT(11) NOT NULL AUTO_INCREMENT,
                Nazov VARCHAR(50) NOT NULL,
                Hodnota VARCHAR(50) NOT NULL,
                Kvalita VARCHAR(50) NOT NULL,
                Datum VARCHAR(50) NOT NULL,
                PRIMARY KEY (ID)
            )
        """)

    # Process JSON files from data directory
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            filepath = os.path.join(directory, filename)
            with open(filepath, 'r') as jsonfile:
                # Read JSON data
                json_data = json.load(jsonfile)
                header = list(json_data[0].keys())
                # Insert data into the "Sensors" table
                with conn.cursor() as cursor:
                    for row in json_data:
                        values = [row[key] for key in header]
                        cursor.execute(
                            "INSERT INTO Sensors (Nazov, Hodnota, Kvalita, Datum) VALUES (%s, %s, %s, %s)",
                            values
                        )
                conn.commit()

    # # Process CSV files from data directory
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
    #                     cursor.execute("INSERT INTO `Sensors` (`Nazov`,`Hodnota`,`Kvalita`,`Datum`) VALUES (%s, %s, %s, %s)",row)
    #             conn.commit()
    
print("Data has been loaded into MySQL successfully.")

with pymysql.connect(
    host="localhost",
    port=3307,
    user='test',
    password='password',
    database='test',
    cursorclass=pymysql.cursors.DictCursor
) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS count FROM Sensors")
        result = cursor.fetchone()
        count = result['count']

print("Number of records: ", count)

