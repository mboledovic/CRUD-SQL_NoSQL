from cassandra.cluster import Cluster
from datetime import datetime
import time
from time import sleep
import json


# Connect to the Cassandra cluster
with Cluster(["127.0.0.1"], port=9042) as cluster:
    with cluster.connect() as session:
        session.set_keyspace("my_keyspace")

        # Check connection
        try:
            session.execute("SELECT * FROM system_schema.keyspaces")
            print("Cassandradb is available")
        except:
            print("Cassandradb is not available")

        result = session.execute("SELECT COUNT(*) FROM Sensors")
        # fetch the result
        count = result.one()[0]
        print(f"Total number of records: {count}")

# start of manipulation with Read querries

# -------------------------------------------------------------------------
        file_read = open("cassandra\Value_Read_querry_time.txt", "w")

        with open(r"value_read.json") as file:
            var_json_read = json.load(file)
            for record in var_json_read:
                tic = time.perf_counter()
                rows = session.execute(
                    "SELECT * FROM Sensors WHERE datum=%s ALLOW FILTERING;", (record["Datum"],)
                )
                toc = time.perf_counter()
                output_query_time_2 = (toc - tic) * 10**3
                file_read.write(f"{output_query_time_2}\n")

        file_read.close()

        print("Querries for Read are done.")

        sleep(1)

        with open(r"cassandra\Value_Read_querry_time.txt", "r") as fp:
            a = 0
            lines = fp.readlines()
            b = len(lines)
            print("Total lines:", b)
            for line in lines:
                a += float(line)
        print("Avarage time for executing Read Querries is [ms]:", a / b)

# --------------------------------------------------------------------------------

# start of manipulation with Update querries

        file_update = open("cassandra\Value_Update_querry_time.txt", "w")

        with open(r"value_update.json") as file:
            var_json_update = json.load(file)
            for record in var_json_update:
                tic = time.perf_counter()
                datum = datetime.strptime(record['Datum'], '%Y-%m-%d %H:%M:%S.%f').isoformat() + 'Z' \
                    if '.' in record['Datum'] else datetime.strptime(record['Datum'], '%Y-%m-%d %H:%M:%S').isoformat() + 'Z'
                result = session.execute(
                    "SELECT * FROM Sensors WHERE datum=%s ALLOW FILTERING;", (datum,)
                )
                result = list(result)
                for row in result:
                    id_value = row.id
                    rows = session.execute(
                        "UPDATE Sensors SET Hodnota = %s WHERE id = %s IF EXISTS",
                        (float(record["Hodnota"]), id_value),
                    )
                toc = time.perf_counter()
                output_query_time_3 = (toc - tic) * 10**3
                file_update.write(f"{output_query_time_3}\n")

        file_update.close()

        print("Querries for Update are done.")

        with open(r"cassandra\Value_Update_querry_time.txt", "r") as fp:
            a = 0
            lines = fp.readlines()
            b = len(lines)
            print("Total lines:", b)
            for line in lines:
                a += float(line)
        print("Avarage time for executing UPDATE Querries is [ms]:", a / b)

        sleep(1)

# start of manipulation with Delete querries

# --------------------------------------------------------------------------------

        file_delete = open("cassandra\Value_Delete_querry_time.txt", "w")

        with open(r"value_delete.json") as file:
            var_json_delete = json.load(file)
            for record in var_json_delete:
                tic = time.perf_counter()
                datum = datetime.strptime(record['Datum'], '%Y-%m-%d %H:%M:%S.%f').isoformat() + 'Z' \
                    if '.' in record['Datum'] else datetime.strptime(record['Datum'], '%Y-%m-%d %H:%M:%S').isoformat() + 'Z'        
                result = session.execute(
                    "SELECT * FROM Sensors WHERE datum=%s ALLOW FILTERING;", (datum,)
                )
                result = list(result)
                for row in result:
                    id_value = row.id
                    rows = session.execute(
                        "DELETE FROM Sensors WHERE id = %s IF EXISTS", (id_value,)
                    )
                toc = time.perf_counter()
                output_query_time_4 = (toc - tic) * 10**3
                file_delete.write(f"{output_query_time_4}\n")

        file_delete.close()

        print("Querries for Delete are done.")
        sleep(1)

        with open(r"cassandra\Value_Delete_querry_time.txt", "r") as fp:
            a = 0
            lines = fp.readlines()
            b = len(lines)
            print("Total lines:", b)
            for line in lines:
                a += float(line)
        print("Avarage time for executing DELETE Querries is [ms]:", a / b)

        # Print the count
        query = "SELECT COUNT(*) FROM Sensors"
        result = session.execute(query)

       
