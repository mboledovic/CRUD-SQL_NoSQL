
# import csv
from concurrent.futures import ThreadPoolExecutor
import datetime
import json
import math
import os
import random
import shutil


class DataGenerator:
    def __init__(self):
        # Set the number of Threads for data generation
        self.MAX_THREADS = 5
        self.OUTPUT_DIRECTORY = "Output_results"
        self.USER_INPUT = {}

        self.create = []
        self.read_delete = []
        self.update = []

        self.update_pk = []
        self.read_pk = []
        self.delete_pk = []

    # function return random number in defined range
    def get_random_value(self, min=99, max=101):
        random_value = random.uniform(min, max)
        return random_value

    # function creates a thread pool and submits tasks to the pool to generate data in parallel
    def start_threads(self, options):
        time = options["data_date"]
        time_incrementation = options["increment_time"]
        total_data_count = options["data_count"]
        data_source_name = options["data_name"]
        fail_probability = options["fail_probability"]
        number_of_tests = options["number_of_tests"]

        # set number to chunk data into files
        threads_to_create = max(math.floor(total_data_count / 1_000_000), 1)

        data_per_thread = math.floor(total_data_count / threads_to_create)
        data_for_last_thread = total_data_count % threads_to_create

        crud_tests_per_thread = math.floor(number_of_tests / threads_to_create)
        crud_tests_for_last_thread = number_of_tests % threads_to_create

        with ThreadPoolExecutor(max_workers=self.MAX_THREADS) as executor:
            for thread_id in range(threads_to_create):
                if (thread_id + 1) == threads_to_create:
                    data_per_thread = data_per_thread + data_for_last_thread
                    crud_tests_per_thread = (
                        crud_tests_per_thread + crud_tests_for_last_thread
                    )

                add_time = thread_id * time_incrementation * data_per_thread
                start_time = time + datetime.timedelta(milliseconds=add_time)
                executor.submit(
                    self.generate_data,
                    data_per_thread,
                    start_time,
                    time_incrementation,
                    data_source_name,
                    fail_probability,
                    thread_id,
                    crud_tests_per_thread,
                )

            start_time_create = time + \
                (datetime.timedelta(milliseconds=time_incrementation) * total_data_count)

            self.create_tests(start_time_create, number_of_tests,
                         time_incrementation, data_source_name, fail_probability)


    # function which return generated data
    def generate_data(self, number_of_items, start_time, increment_time, name, fail_chance, thread_id, crud_tests,):

        generated_data = []
        time = start_time

        # for loop for generating data
        for _ in range(number_of_items):
            time = time + datetime.timedelta(milliseconds=increment_time)
            generated_data.append(
                {
                    "Nazov": name,
                    "Hodnota": self.get_random_value(),
                    "Kvalita": True if random.random() > (fail_chance * 0.01) else False,
                    "Datum": time,
                }
            )

        # for loop- generation of crud tests
        for i in range(crud_tests):
            item = generated_data[i]
            item["Hodnota"] = item["Hodnota"] + 5
            self.create.append(item)
            self.read_delete.append({"Datum": item["Datum"]})
            self.update.append({"Datum": item["Datum"], "Hodnota": item["Hodnota"] + 5})

        self.dump_json(generated_data, thread_id)
        # dump_csv(generated_data, thread_id)

    # Function for generating crud tests based on input parameters
    def create_tests(self, start_time_create, number_of_tests, time_incrementation, name, fail_chance):

        ids = self.get_random_ids(number_of_tests, 1, self.USER_INPUT["data_count"])

        start_time = start_time_create

        generated_data = []
        for i in range(number_of_tests):
            start_time = start_time + \
                datetime.timedelta(milliseconds=time_incrementation)
            generated_data.append(
                {   
                    "Nazov": name,
                    "Hodnota": self.get_random_value(),
                    "Kvalita": True if random.random() > (fail_chance * 0.01) else False,
                    "Datum": start_time,
                })
        for i in range(number_of_tests):
            item = generated_data[i]
            #item["Hodnota"] = item["Hodnota"] + 5
            self.update_pk.append({"Id": ids[i], "Hodnota": item["Hodnota"] + 5})
            self.read_pk.append({"Id": ids[i]})
            self.delete_pk.append({"Id": ids[i]})

        self.dump_json2(generated_data, "file_create")
        self.dump_json2(self.update_pk,"Pk_update")
        self.dump_json2(self.read_pk, "Pk_read")
        self.dump_json2(self.delete_pk, "Pk_delete")
        pass


    # function for generating ids
    def get_random_ids(self, number_of_id, min, max):
        ids = random.sample(range(min, max+1), number_of_id)
        return ids

    # function for input data
    def get_user_input(self):
        data_name = input("Insert the name for source of data: ") or "Sensor1"
        fail_probability = float(
            input("Enter probability of error during reading data (1/x): ") or 5
        )
        data_count = int(input("Enter number for generating data: ") or 20000)
        date_input = str(
            input("Set starting date(DD-MM-YYYY H-M-S-mS):") or "04-10-2020 10-00-00-00"
        )
        data_date = datetime.datetime.strptime(date_input, "%d-%m-%Y %H-%M-%S-%f")
        data_increment_time = int(
            input("Enter data time incrementation(1000 = 1 second): ") or 100
        )
        number_of_tests = int(
            input("Enter number of generated CRUD tests data:") or 20)
        return {
            "data_name": data_name,
            "fail_probability": fail_probability,
            "data_count": data_count,
            "data_date": data_date,
            "increment_time": data_increment_time,
            "number_of_tests": number_of_tests,
        }


    # function for dumping data into csv
    # def dump_csv(data, i):
    #     header = ["Nazov", "Hodnota", "Kvalita", "Datum"]
    #     with open(
    #         f"{OUTPUT_DIRECTORY}\gendata_{i}.csv", "w", encoding="UTF8", newline=""
    #     ) as f:
    #         writer = csv.writer(f)
    #         writer.writerow(header)
    #         for entry in data:
    #             writer.writerow(entry.values())


    # function for dumping generated data json
    def dump_json(self, inp_data, i):
        with open(f"{self.OUTPUT_DIRECTORY}\gendat_{i}.json", "w") as outfile:
            json.dump(inp_data, outfile, default=str)

    # function for dumping generated tests
    def dump_json2(self, inp_data, name):
        with open(f"{name}.json", "w") as outfile:
            json.dump(inp_data, outfile, default=str)

    def run(self):
    # clear/create output directory
        os.makedirs(self.OUTPUT_DIRECTORY, exist_ok=True)
        if os.path.exists(self.OUTPUT_DIRECTORY):
            shutil.rmtree(self.OUTPUT_DIRECTORY)
            os.makedirs(self.OUTPUT_DIRECTORY)
        else:
            os.makedirs(self.OUTPUT_DIRECTORY)

        self.USER_INPUT = self.get_user_input()
        self.result = self.get_random_ids(self.USER_INPUT["number_of_tests"], 1, self.USER_INPUT["data_count"])
        self.start_threads(self.USER_INPUT)

        self.dump_json2(self.read_delete, "value_read")
        self.dump_json2(self.read_delete, "value_delete")
        self.dump_json2(self.update, "value_update")

generator = DataGenerator()
generator.run()

