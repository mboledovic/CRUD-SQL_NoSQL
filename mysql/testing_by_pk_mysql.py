
import sys
import pymysql
import pymysql.cursors
import time
import json


# Connection to database
with pymysql.connect(
    host="localhost",
    port=3307,
    user="test",
    password="password",
    database="test",
    cursorclass=pymysql.cursors.DictCursor,
) as conn:
    mycursor = conn.cursor()

    # ping database
    count = mycursor.execute("SELECT COUNT(*) FROM Sensors")
    print(f"Total number of records: {count}")

    # start of manipulation with Create querries
    file_create = open("mysql/Create_querry_time.txt", "w")

    with open(r"file_create.json") as file:
        var_json_create = json.load(file)
        for record in var_json_create:
            tic = time.perf_counter()
            rows = mycursor.execute(
                "INSERT INTO Sensors(Nazov,Hodnota,Kvalita,Datum) VALUES(%s,%s,%s,%s);",
                (record["Nazov"], record["Hodnota"], record["Kvalita"], record["Datum"]),
            )
            toc = time.perf_counter()
            output_query_time_1 = (toc - tic) * 10**3
            file_create.write(f"{output_query_time_1}\n")

    file_create.close()

    print("Querries for Create are done.")

    time.sleep(1)

    with open(r"mysql\Create_querry_time.txt") as fp:
        a = 0
        lines = fp.readlines()
        b = len(lines)
        print("Total lines:", b)
        for line in lines:
            a += float(line)
    print("Avarage time for executing Create Querries is [ms]:", a / b)

    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM Sensors")
        records = cursor.fetchall()
        count_after_create = len(records)

    print("Number of records after create: ", count_after_create)


# --------------------------------------------------------------------------------

# start of manipulation with Read querries

    file_read = open("mysql/Pk_Read_querry_time.txt", "w")

    with open(r"Pk_read.json") as file:
        var_json_read = json.load(file)
        for record in var_json_read:
            tic = time.perf_counter()
            rows = mycursor.execute(
                "SELECT * FROM Sensors WHERE ID=%s;", (record["Id"],)
            )
            toc = time.perf_counter()
            output_query_time_2 = (toc - tic) * 10**3
            file_read.write(f"{output_query_time_2}\n")

    file_read.close()
    print("Querries for Read are done.")

    time.sleep(1)

    with open(r"mysql\Pk_Read_querry_time.txt", "r") as fp:
        a = 0
        lines = fp.readlines()
        b = len(lines)
        print("Total lines:", b)
        for line in lines:
            a += float(line)
    print("Avarage time for executing Read Querries is [ms]:", a / b)

# --------------------------------------------------------------------------------

# start of manipulation with Update querries

    file_update = open("mysql\Pk_Update_querry_time.txt", "w")

    with open(r"Pk_update.json") as file:
        var_json_update = json.load(file)
        for record in var_json_update:
            tic = time.perf_counter()
            rows = mycursor.execute(
                "UPDATE Sensors SET Hodnota=%s WHERE ID=%s;",(record["Hodnota"], record["Id"]),)
            toc = time.perf_counter()
            output_query_time_3 = (toc - tic) * 10**3
            file_update.write(f"{output_query_time_3}\n")

    file_update.close()
    print("Querries for Update are done.")

    with open(
        r"mysql\Pk_Update_querry_time.txt", "r"
    ) as fp:
        a = 0
        lines = fp.readlines()
        b = len(lines)
        print("Total lines:", b)
        for line in lines:
            a += float(line)
    print("Avarage time for executing UPDATE Querries is [ms]:", a / b)

    time.sleep(1)

# --------------------------------------------------------------------------------

# start of manipulation with Delete querries

    file_delete = open("mysql\Pk_Delete_querry_time.txt", "w")

    with open(r"Pk_delete.json") as file:
        var_json_delete = json.load(file)
        for record in var_json_delete:
            tic = time.perf_counter()
            rows = mycursor.execute(
                "DELETE FROM  Sensors WHERE ID=%s;", (record["Id"])
            )
            toc = time.perf_counter()
            output_query_time_4 = (toc - tic) * 10**3
            file_delete.write(f"{output_query_time_4}\n")

    file_delete.close()
    print("Querries for Delete are done.")

    time.sleep(1)

    with open(
        r"mysql\Pk_Delete_querry_time.txt", "r"
    ) as fp:
        a = 0
        lines = fp.readlines()
        b = len(lines)
        print("Total lines:", b)
        for line in lines:
            a += float(line)
    print("Avarage time for executing DELETE Querries is [ms]:", a / b)

    # Print number of records in table Sensors
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS count FROM Sensors")
        result = cursor.fetchone()
        count_after_delete = result['count']

    print("Number of records after delete: ", count_after_delete)

    
