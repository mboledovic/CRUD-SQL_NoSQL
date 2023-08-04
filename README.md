# CRUD-SQL_NoSQL

Time comparison of CRUD queries (primary key & value) for chosen databases running locally in docker containers done by Python programs.


## About Project

Goal of project is to select proper database to store data from process controll aplications. In project i am comparing different SQL and NoSQL databases. Databases run locally in docker containers. CRUD testing of database is done by python programs.


## How to generate data

1. Open file Data_generator.py in ide or text editor supporting .py files

2. Insert for generating:
   
   - Name of source
   - Fail_probability
   - Number of generated data
   - Starting date for data generation
   - Period of data generation
   - Number for CRUD tests

  You can change the number of threads which will generate data, the number of data generated in each file and range for random float value

3. Generate data

4. Generated data will be in directory "Output_results",
   Tests will be generated in 7 json files in parent directory


## How to run database

1. Install Docker engine with Docker Commpose

2. Start Docker engine

3. Open cmd/powershell cli or install extension of Docker to your text editor

4. Change directory of database you wish to test "cd <Name_directory>"

5. Launch docker container with command:
```
docker-compose up -d
```

6. After the testing is done, use command to turn off database:
```
docker-compose down
```


## How to fill database container with data

1. After launching selected database open file "import.py" located in the directory with the same name as running database

2. Install connectors for database.

3. After launching program "import.py", a schema for data storageis created in the docker database and data imported from the directory "Output_results" 

4. When the data insertion is complete, the console will display the data insertion message was successful and the number of records stored in the database will be displayed.
   In the same directory, a "Volumes" folder will be created, which will contain stored data.

5. Create copy of "Volumes" and move it to new directory. (for replication of results)


## How to test database

1. Open file "testing_by_pk" and run the program

2. After completing the test, console will print avarage time for executing SQL query (micro-seconds/ nano-seconds)

3. Record result times

4. In cmd/powershell cli or docker extention in your editor run command to stop container
```
docker-compose down
```

5. Delete directory "Volumes" and replace it with saved volumes file for replication

6. Run command "docker-compose up -d" in cmd/powershell cli or in text editor/IDE
```
docker-compose up -d
```
8. Open file "testing_by_value" and run the program

9. Repeat 2. step

10. Repeat 3. step

11. Repeat 4. step


## Testing environment

Parameters of tested machine:

   - 8GB RAM DDR4 2400 MHz
   - 6 core CPU (base clock: 2,2GHz)
   - SSD NVMe PCIe Gen3


## Software used in project

Chosen databases were running as Docker containers. Link for [Docker engine](https://docs.docker.com/engine/install/ubuntu/) to install on Linux OS.(You will need Docker engine and Docker compose)

For importing data files into databases you will need code editor or IDE to run .py programs. Link for [VS Code](https://code.visualstudio.com/download).  
