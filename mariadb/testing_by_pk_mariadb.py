
import mariadb
import time
import json

# Connection to database
with mariadb.connect(
    user="root", 
    password="password", 
    host="localhost", 
    port=3307, 
    database="test"
    ) as conn:
    mycursor = conn.cursor()

    with mycursor as cursor:
        cursor.execute('SELECT COUNT(*) FROM Sensors')
        count = cursor.fetchone()[0]

    print(f"Total number of records: {count}")

    # start of manipulation with Create queries
    file_create = open("mariadb\Mariadb_Create_querry_time.txt", "w")

    with open(r"file_create.json") as file:
        var_json_create = json.load(file)
        for record in var_json_create:
            tic = time.perf_counter()
            cursor.execute(
                "INSERT INTO Sensors(Nazov,Hodnota,Kvalita,Datum) VALUES(%s,%s,%s,%s);",
                (record["Nazov"], record["Hodnota"], record["Kvalita"], record["Datum"]),
            )
            toc = time.perf_counter()
            output_query_time_1 = (toc - tic) * 10**3
            file_create.write(f"{output_query_time_1}\n")

    file_create.close()

    print("Queries for Create are done.")

    time.sleep(1)
    # Calculate average time 

    with open(r"mariadb\Mariadb_Create_querry_time.txt") as fp:
        a = 0
        lines = fp.readlines()
        b = len(lines)
        print("Total lines:", b)
        for line in lines:
            a += float(line)
    print("Average time for executing Create Queries is [ms]:", a / b)


    with mycursor as cursor:
        cursor.execute('SELECT COUNT(*) FROM Sensors')
        count_after_create = cursor.fetchone()[0]

    # Print the count of records
    print("Number of records after create: ", count_after_create)

# --------------------------------------------------------------------------------

# start of manipulation with Read queries

    file_read = open("mariadb\Pk_Read_querry_time.txt", "w")

    with open(r"Pk_read.json") as file:
        var_json_read = json.load(file)
        for record in var_json_read:
            tic = time.perf_counter()
            cursor.execute(
                "SELECT * FROM Sensors WHERE ID=%s;", (record["Id"],)
            )
            toc = time.perf_counter()
            output_query_time_2 = (toc - tic) * 10**3
            file_read.write(f"{output_query_time_2}\n")

    file_read.close()
    print("Queries for Read are done.")

    time.sleep(1)
    # Calculate average time 

    with open(r"mariadb\Pk_Read_querry_time.txt", "r") as fp:
        a = 0
        lines = fp.readlines()
        b = len(lines)
        print("Total lines:", b)
        for line in lines:
            a += float(line)
    print("Average time for executing Read Queries is [ms]:", a / b)

# --------------------------------------------------------------------------------

# start of manipulation with Update queries

    file_update = open("mariadb\Pk_Update_querry_time.txt", "w")

    with open(r"Pk_update.json") as file:
        var_json_update = json.load(file)
        for record in var_json_update:
            tic = time.perf_counter()
            cursor.execute(
                "UPDATE Sensors SET Hodnota=%s WHERE ID=%s;",
                (record["Hodnota"], record["Id"]),
            )
            toc = time.perf_counter()
            output_query_time_3 = (toc - tic) * 10**3
            file_update.write(f"{output_query_time_3}\n")

    file_update.close()
    print("Queries for Update are done.")
    # Calculate average time 

    with open(r"mariadb\Pk_Update_querry_time.txt", "r") as fp:
        a = 0
        lines = fp.readlines()
        b = len(lines)
        print("Total lines:", b)
        for line in lines:
            a += float(line)
    print("Average time for executing UPDATE Queries is [ms]:", a / b)

    time.sleep(1)

# --------------------------------------------------------------------------------

# start of manipulation with Delete queries

    file_delete = open("mariadb\Pk_Delete_querry_time.txt", "w")

    with open(r"Pk_delete.json") as file:
        var_json_delete = json.load(file)
        for record in var_json_delete:
            tic = time.perf_counter()
            cursor.execute(
                "DELETE FROM Sensors WHERE ID=%s;", (record["Id"],)
            )
            toc = time.perf_counter()
            output_query_time_4 = (toc - tic) * 10**3
            file_delete.write(f"{output_query_time_4}\n")

    file_delete.close()
    print("Queries for Delete are done.")

    time.sleep(1)
    # Calculate average time 

    with open(r"mariadb\Pk_Delete_querry_time.txt", "r") as fp:
        a = 0
        lines = fp.readlines()
        b = len(lines)
        print("Total lines:", b)
        for line in lines:
            a += float(line)
    print("Average time for executing DELETE Queries is [ms]:", a / b)

    with mycursor as cursor:
        cursor.execute('SELECT COUNT(*) FROM Sensors')
        count_after_delete = cursor.fetchone()[0]

    # Print the count of records
    print("Number of records after delete: ", count_after_delete)
