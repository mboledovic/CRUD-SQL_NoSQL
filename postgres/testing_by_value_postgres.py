import psycopg2
from time import time
from time import sleep
import time
import json


# Connection to database
with psycopg2.connect(
    host='localhost',
    port=5431,
    user='test',
    password='password'
) as conn:
    
    with conn.cursor() as mycursor:
        if mycursor is not None:
            print("Server is available")
        else:
            print("Server is not available")

    # execute a SELECT statement to count all records
    mycursor.execute("SELECT COUNT(*) FROM Sensors")

    # fetch the result
    count = mycursor.fetchone()[0]
    print(f"Total number of records: {count}")

# --------------------------------------------------------------------------------

# start of manipulation with Read querries

    file_read = open("postgres\Value_Read_querry_time.txt", "w")

    with open(r"value_read.json") as file:
        var_json_read = json.load(file)
        for record in var_json_read:
            tic = time.perf_counter()
            rows = mycursor.execute(
                "select * from Sensors Where Datum=%s;", (record["Datum"],)
            )
            toc = time.perf_counter()
            output_query_time_2 = (toc - tic) * 10**3
            file_read.write(f"{output_query_time_2}\n")

    file_read.close()
    print("Querries for Read are done.")

    sleep(1)
    # Calculate avarage time 

    with open(r"postgres\Value_Read_querry_time.txt", "r") as fp:
        a = 0
        lines = fp.readlines()
        b = len(lines)
        print("Total lines:", b)
        for line in lines:
            a += float(line)
    print("Avarage time for executing Read Querries is [ms]:", a / b)

# --------------------------------------------------------------------------------

# start of manipulation with Update querries

    file_update = open("postgres\Value_Update_querry_time.txt", "w")

    with open(r"value_update.json") as file:
        var_json_update = json.load(file)
        for record in var_json_update:
            tic = time.perf_counter()
            rows = mycursor.execute(
                "update Sensors SET Hodnota=%s WHERE Datum=%s;",
                (record["Hodnota"], record["Datum"]),
            )
            toc = time.perf_counter()
            output_query_time_3 = (toc - tic) * 10**3
            file_update.write(f"{output_query_time_3}\n")

    file_update.close()
    print("Querries for Update are done.")
    # Calculate avarage time 

    with open(r"postgres\Value_Update_querry_time.txt", "r") as fp:
        a = 0
        lines = fp.readlines()
        b = len(lines)
        print("Total lines:", b)
        for line in lines:
            a += float(line)
    print("Avarage time for executing UPDATE Querries is [ms]:", a / b)

    sleep(1)

# --------------------------------------------------------------------------------

# start of manipulation with Delete querries

    file_delete = open("postgres\Value_Delete_querry_time.txt", "w")

    with open(r"value_delete.json") as file:
        var_json_delete = json.load(file)
        for record in var_json_delete:
            tic = time.perf_counter()
            rows = mycursor.execute(
                "DELETE FROM Sensors WHERE Datum=%s;", (record["Datum"],)
            )
            toc = time.perf_counter()
            output_query_time_4 = (toc - tic) * 10**3
            file_delete.write(f"{output_query_time_4}\n")

    file_delete.close()
    print("Querries for Delete are done.")

    sleep(1)
    # Calculate avarage time 

    with open(r"postgres\Value_Delete_querry_time.txt", "r") as fp:
        a = 0
        lines = fp.readlines()
        b = len(lines)
        print("Total lines:", b)
        for line in lines:
            a += float(line)
    print("Avarage time for executing DELETE Querries is [ms]:", a / b)

    # execute a SELECT statement to count all records
    mycursor.execute("SELECT COUNT(*) FROM Sensors")

    # fetch the result
    count_after_delete = mycursor.fetchone()[0]
    print(f"Total number of records after delete: {count_after_delete}")

