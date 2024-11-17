# airport_ds
 Take-home project for Senior Data Scientist role 

# Objectives and Key Results

**Objective**: To design and build a scalable system for processing and analyzing real-time airport and flight data. The solution must efficiently handle large datasets from sources like ADSB and OAG, enabling effective analysis to support operational and strategic needs.

**Key Results**:

1. xxx
2. xxx
3. xxx


# How to Replicate the Result

The instruction below is for Windows-based installation. If you use other OS, your mileage may vary.

## Install Apache Spark

1. Install Java JDK 1.8 from the [following link](https://www.oracle.com/id/java/technologies/javase/javase8u211-later-archive-downloads.html). Ensure you know where the installation folder is located -- you will need it later.

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
JAVA_HOME=path\to\jre1.8.0_431\
```

2. Run the `project.py`
```
python project.py
```
3. The results will be stored in `.\results\`

