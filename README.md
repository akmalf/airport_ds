# airport_ds
 Take-home project for a Senior Data Scientist role 

# Objectives and Key Results

**Objective**: To design and build a scalable system for processing and analyzing real-time airport and flight data. The solution must efficiently handle large datasets from sources like ADSB and OAG, enabling effective analysis to support operational and strategic needs.

**Results**:

1. The average origin airport speed is

|Origin Airport|avg(Speed)|
| --- | ----------- |
|   GUA|     170.0|
|   DOH|     250.0|
|   SGN|     234.2|
|   IAD|     170.0|


2. The average destination airport speed is

|Destination Airport|avg(Speed)|
| --- | ----------- |
|        BED|     170.0|
|        SYZ|     250.0|
|        ICN|     234.2|
|        MIA|     170.0|

3. The total of departure and arrival data are:

|sum(departure_delay)|sum(arrival_delay)|
| --- | ----------- |
|                   5|                 4|

4. The most recent entry of each flights are:

|FlightId|         LastUpdate|
| --- | -------------------- |
|  AAL476|2023-10-03 23:36:00|
|   BA484|2023-10-03 06:47:00|
|  LXJ476|2023-10-03 23:25:20|
|   QR476|2023-10-03 06:12:15|

The data visualisation are available in `results` folder


# How to Replicate the Result

The instruction below is for Windows-based installation. If you use other OS, your mileage may vary.

## Install Apache Spark

1. Install Java JRE 1.8 (for example, you will install `jre1.8.0_abc`) from the [following link](https://www.oracle.com/id/java/technologies/javase/javase8u211-later-archive-downloads.html). Ensure you know where the installation folder is located -- you will need it later.

2. Download Apache Spark from [official website](https://spark.apache.org/downloads.html). Extract the `spark-3.x.y-bin-hadoop3.tgz` file to a folder of your choice. (for example, in `C:\spark\`)

## Python installation
1. Install Python 3.9 using your preferred package manager or download the installation file from https://www.python.org/downloads/.

2. On a folder of your choice, create `airport_ds` virtual environment in Python 3.9 by running
```
python -m venv airport_ds
```

3. Activate the environment
```
.\airport_ds\Scripts\activate
```

4. Install all required packages using the available `requirements.txt`. This will install, among others, the `pyspark` package.
```
python -m ensurepip
python -m pip install -r requirements.txt
```

## Running the project

1. On the project folder, create `.env` file with the following contents:
```
SPARK_HOME=path\to\spark\spark-3.x.y-bin-hadoop3\
JAVA_HOME=path\to\jre1.8.0_abc\
``` 
If the program won't run, add the following variables to user's environment variables:

| Variable | Value |
| --- | ----------- |
| SPARK_HOME | `path\to\spark\spark-3.x.y-bin-hadoop3\` |
| JAVA_HOME | `path\to\jre1.8.0_abc\` |

2. Run the `project.py`
```
python project.py
```
3. The results will be stored in `.\results\`

