import json
import redis
from datetime import datetime
import time

# Connect to Redis instance using a context manager
with redis.Redis(host='localhost', port=6379, db=1) as session:

    # Print total number of records 
    count = session.dbsize()
    print(f"Total number of records: {count}")

    # Define function to store record in Redis
    def store_record(record):
        record_id = session.incr('record_id')
        record_json = json.dumps(record)
        tic = time.perf_counter()
        session.set(f'record:{record_id}', record_json)
        toc = time.perf_counter()
        output_query_time_create = (toc - tic) * 10**3
        temp_list_create.append(output_query_time_create)

    # Read Create data (json-type)
    with open(r'file_create.json', 'r') as f:
        records = json.load(f)

    # Measure query time for Create
    temp_list_create = []
    for record in records:
        # Store record in Redis
        store_record(record)

    # Write query times to file
    with open(r'redis\redis_create_time.txt', 'w') as file:
        for var_create in temp_list_create:
            file.write(str(var_create) + '\n')

    time.sleep(1)

    # Calculate and print average query time
    with open(r'redis\redis_create_time.txt', 'r') as fp:
        a = 0
        lines = fp.readlines()
        b = len(lines)
        for line in lines:
            a += float(line)
        print('Average time for executing Create Queries is [ms]:', a / b)

    # Print total number of records 
    count_after_create = session.dbsize()
    print(f"Total number of records after create: {count_after_create}")

# start of manipulation with Read by primary key

# --------------------------------------------------------------------------------

    with open(r'Pk_read.json', 'r') as f:
        keys = json.load(f)

    temp_list_read_by_key = []
    for key in keys:
        tic = time.perf_counter()
        result = session.get(f"record:{key['Id']}")
        toc = time.perf_counter()
        output_query_time_read_pk = (toc - tic) * 10**3
        temp_list_read_by_key.append(output_query_time_read_pk)

    with open(r"redis\Pk_read_time.txt", "w") as file:
        for var_read in temp_list_read_by_key:
            file.write(str(var_read) + "\n")

    time.sleep(1)
    # Calculate avarage time 

    with open(r"redis\Pk_read_time.txt", "r") as fp:
        a = 0
        lines = fp.readlines()
        b = len(lines)
        for line in lines:
            a += float(line)
        print("Average time for executing Read by key Queries is [ms]:", a / b)

#--------------------------------------------------------------------------------

#start of manipulation with Delete querries
    with open(r'Pk_delete.json', 'r') as f:
        keys = json.load(f)

    temp_list_delete_by_key = []
    for key in keys:
        tic = time.perf_counter()
        delete_number = session.delete(f"record:{key['Id']}")
        toc = time.perf_counter()
        # print(f"deleted {delete_number} records")
        output_query_time_delete = (toc - tic) * 10**3
        temp_list_delete_by_key.append(output_query_time_delete)

    with open(r"redis\Pk_delete_time.txt", "w") as file:
        for var_delete in temp_list_delete_by_key:
            file.write(str(var_delete) + "\n")

    time.sleep(1)
    # Calculate avarage time 

    with open(r"redis\Pk_delete_time.txt", "r") as fp:
        a = 0
        lines = fp.readlines()
        b = len(lines)
        for line in lines:
            a += float(line)
        print("Avarage time for executing Delete Querries is [ms]:", a / b)

    #Print total number of records 
    count_end = session.dbsize()
    print(f"Total number of records: {count_end}")
