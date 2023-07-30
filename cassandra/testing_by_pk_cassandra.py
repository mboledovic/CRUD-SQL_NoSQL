from cassandra.cluster import Cluster
from datetime import datetime
import time
from time import sleep
import uuid
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

# start of manipulation with Create querries

# -------------------------------------------------------------------------
        file_create = open("cassandra\Create_querry_time.txt", "w")

        with open(r"file_create.json") as file:
            var_json_create = json.load(file)
            for record in var_json_create:
                tic = time.perf_counter()
                rows = session.execute(
                    "INSERT INTO Sensors (id, nazov, hodnota, kvalita, datum) VALUES (%s,%s,%s,%s,%s)",
                    (
                        uuid.uuid1(),
                        record["Nazov"],
                        float(record["Hodnota"]),
                        bool(record["Kvalita"]),
                        record["Datum"],
                    ),
                )
                toc = time.perf_counter()
                output_query_time_1 = (toc - tic) * 10**3
                output_result_1 = output_query_time_1
                file_create.write(f"{output_result_1}\n")

        file_create.close()

        print("Querries for Create are done.")

        sleep(1)

        with open(r"cassandra\Create_querry_time.txt") as fp:
            a = 0
            lines = fp.readlines()
            b = len(lines)
            print("Total lines:", b)
            for line in lines:
                a += float(line)
        print("Avarage time for executing Create Querries is [ms]:", a / b)

        query = "SELECT COUNT(*) FROM Sensors"
        result = session.execute(query)

        # Print the count
        count_after_create = result[0][0]
        print(f"Number of records: {count_after_create}")

# start of manipulation with Read querries

# -------------------------------------------------------------------------
        file_read = open("cassandra\Pk_Read_querry_time.txt", "w")

        with open(r"Pk_read.json") as file:
            var_json_read = json.load(file)
            for record in var_json_read:
                id_string = str(record["Id"]).zfill(32)
                id_uuid = uuid.UUID(id_string)
                tic = time.perf_counter()
                rows = session.execute("SELECT * FROM Sensors WHERE id=%s;", (id_uuid,))
                toc = time.perf_counter()
                output_query_time_2 = (toc - tic) * 10**3
                file_read.write(f"{output_query_time_2}\n")

        file_read.close()

        print("Querries for Read are done.")

        sleep(1)

        with open(r"cassandra\Pk_Read_querry_time.txt", "r") as fp:
            a = 0
            lines = fp.readlines()
            b = len(lines)
            print("Total lines:", b)
            for line in lines:
                a += float(line)
        print("Avarage time for executing Read Querries is [ms]:", a / b)

# --------------------------------------------------------------------------------

# start of manipulation with Update querries

        file_update = open("cassandra\Pk_Update_querry_time.txt", "w")

        with open(r"Pk_update.json") as file:
            var_json_update = json.load(file)
            for record in var_json_update:
                id_string = str(record["Id"]).zfill(32)
                id_uuid = uuid.UUID(id_string)
                tic = time.perf_counter()
                rows = session.execute("UPDATE Sensors SET Hodnota = %s WHERE id = %s", (record["Hodnota"],(id_uuid)))
                toc = time.perf_counter()
                output_query_time_3 = (toc - tic) * 10**3
                file_update.write(f"{output_query_time_3}\n")

        file_update.close()

        print("Querries for Update are done.")

        with open(r"cassandra\Pk_Update_querry_time.txt", "r") as fp:
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

        file_delete = open("cassandra\Pk_Delete_querry_time.txt", "w")

        with open(r"Pk_delete.json") as file:
            var_json_delete = json.load(file)
            for record in var_json_delete:
                id_string = str(record["Id"]).zfill(32)
                id_uuid = uuid.UUID(id_string)
                tic = time.perf_counter()
                rows = session.execute(
                    "DELETE FROM Sensors WHERE id = %s IF EXISTS", (id_uuid,))
                toc = time.perf_counter()
                output_query_time_4 = (toc - tic) * 10**3
                file_delete.write(f"{output_query_time_4}\n")

        file_delete.close()

        print("Querries for Delete are done.")
        sleep(1)

        with open(r"cassandra\Pk_Delete_querry_time.txt", "r") as fp:
            a = 0
            lines = fp.readlines()
            b = len(lines)
            print("Total lines:", b)
            for line in lines:
                a += float(line)
        print("Avarage time for executing DELETE Querries is [ms]:", a / b)

        query = "SELECT COUNT(*) FROM Sensors"
        result = session.execute(query)

        # Print the count
        count_after_delete = result[0][0]
        print(f"Number of records: {count_after_delete}")


