import time
from pymongo import MongoClient
import json

CONNECTION_STRING = 'mongodb://localhost:27017'
DB_NAME = 'test'
COLLECTION_NAME = 'mycollection'


# Connect to the MongoDB
with MongoClient(CONNECTION_STRING) as client:
    db = client[DB_NAME]
    mycollection = db[COLLECTION_NAME]

    count_start = mycollection.count_documents({})
    # Print the count of records
    print("Number of records at start: ", count_start)

# -------------------------------------------------------------------------

# start of manipulation with Read queries


    temp_list_read = []
    with open(r"value_read.json") as file:
        var_json_read = json.load(file)
        for line in var_json_read:
            tic = time.perf_counter()
            result = mycollection.find(line)
            toc = time.perf_counter()
            output_query_time_read = (toc - tic) * 10**3
            temp_list_read.append(output_query_time_read)

    with open("mongo\Value_read_time.txt", "w") as file:
        for var_read in temp_list_read:
            file.write(str(var_read) + "\n")

    time.sleep(1)
    # Calculate average time
    with open("mongo\Value_read_time.txt", "r") as fp:
        a = 0
        lines = fp.readlines()
        b = len(lines)
        print("Total lines:", b)
        for line in lines:
            a += float(line)
    print("Average time for executing Read Queries is [ms]:", a / b)

# --------------------------------------------------------------------------------

# start of manipulation with Update queries

    temp_list_update = []
    with open(r"value_update.json") as file:
        var_json_update = json.load(file)
        for line in var_json_update:
            tic = time.perf_counter()
            result = mycollection.update_one(
                {"Datum": line["Datum"]}, {"$set": {"Hodnota": line["Hodnota"]}}
            )
            toc = time.perf_counter()
            output_query_time_update = (toc - tic) * 10**3
            temp_list_update.append(output_query_time_update)

    with open("mongo\Value_update_time.txt", "w") as file:
        for var_update in temp_list_update:
            file.write(str(var_update) + "\n")

    time.sleep(1)
    # Calculate average time
    with open("mongo\Value_update_time.txt", "r") as fp:
        a = 0
        lines = fp.readlines()
        b = len(lines)
        print("Total lines:", b)
        for line in lines:
            a += float(line)
    print("Average time for executing Update Queries is [ms]:", a / b)

# --------------------------------------------------------------------------------

# start of manipulation with Delete queries

    temp_list_delete = []
    with open(r"value_delete.json") as file:
        var_json_delete = json.load(file)
        for line in var_json_delete:
            tic = time.perf_counter()
            result = mycollection.delete_one(line)
            toc = time.perf_counter()
            output_query_time_delete = (toc - tic) * 10**3
            temp_list_delete.append(output_query_time_delete)

    with open("mongo\Value_delete_time.txt", "w") as file:
        for var_delete in temp_list_delete:
            file.write(str(var_delete) + "\n")

    time.sleep(1)
    # Calculate average time
    with open("mongo\Value_delete_time.txt", "r") as fp:
        a = 0
        lines = fp.readlines()
        b = len(lines)
        print("Total lines:", b)
        for line in lines:
            a += float(line)
    print("Average time for executing Delete Queries is [ms]:", a / b)

    count_after_delete = mycollection.count_documents({})
    # Print the count of records
    print("Number of records after delete: ", count_after_delete)
