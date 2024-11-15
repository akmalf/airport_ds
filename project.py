import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode 

# Define OS environments
os.environ['SPARK_HOME'] = os.getenv('SPARK_HOME')
os.environ['JAVA_HOME'] = os.getenv('JAVA_HOME')

# Initialize SparkSession
spark = SparkSession.builder.appName("AirportKPI").getOrCreate()

# Read the ADSB dataset
adsb_path = "./data/adsb.json"
adsb_df = spark.read.option("multiline","true").json(adsb_path).show()

def parse_adsb(spark, file_path):
    try:
        # Read the JSON file into a DataFrame
        df = spark.read.option("multiline","true").json(file_path)
        return df
    except Exception as e:
        print(f"Error parsing ADSB file: {e}")
        return None
        

# Read the OAG dataset
oag_path = "./data/oag.json"
oag_df = spark.read.option("multiline","true").json(oag_path)
oag_df = oag_df.withcolumn("iata_aircraft", col("data.aircraftType")).show()