import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# Define OS environments
os.environ['SPARK_HOME'] = os.getenv('SPARK_HOME')
os.environ['JAVA_HOME'] = os.getenv('JAVA_HOME')

def parse_adsb(spark, file_path):
    try:
        # Read the JSON file into a DataFrame
        df = spark.read.option("multiline","true").json(file_path)
        return df
    except Exception as e:
        print(f"Error parsing ADSB file: {e}")
        return None

def parse_oag(spark, file_path):
    try:
        df = spark.read.option("multiline","true").json(file_path)
        df = df.select(explode(col("data")).alias("D"))
        df = df.select(
            col("D.carrier.iata").alias("iata_carrier_cd"),
            col("D.carrier.icao").alias("icao_carrier_cd"),
            col("D.serviceSuffix").alias("service_suffix"),
            col("D.flightNumber").alias("flight_num"),
            col("D.sequenceNumber").alias("seq_num"),
            col("D.flightType").alias("flight_type"),
            col("D.departure.airport.iata").alias("departure_port_cde"),
            col("D.departure.terminal").alias("departure_terminal"),
            col("D.departure.date.utc").alias("departure_day"),
            col("D.departure.time.utc").alias("departure_time"),
            col("D.arrival.airport.iata").alias("arrival_port_cde"),
            col("D.arrival.terminal").alias("arrival_terminal"),
            col("D.arrival.date.utc").alias("arrival_day"),
            col("D.arrival.time.utc").alias("arrival_time"),
            col("D.elapsedTime").alias("elapsed_time"),
            col("D.aircraftType.iata").alias("iata_aircraft_typ"),
            col("D.serviceType.iata").alias("iata_service_typ"),
            col("D.segmentInfo.numberOfStops").alias("stops_cnt"),
            col("D.segmentInfo.intermediateAirports.iata").alias("iata_int_port"),
            col("D.scheduleInstanceKey").alias('schedule_inst_key'),
            col("D.statusKey").alias('status_key'),
            explode(col("D.statusDetails.state")).alias("stat_state"),
            explode(col("D.statusDetails.updatedAt")).alias("stat_updated_at"),
            explode(col("D.statusDetails.departure.estimatedTime.outGateTimeliness")).alias("departure_est_outgte_timely"),
            explode(col("D.statusDetails.departure.estimatedTime.outGateVariation")).alias("departure_est_outgte_var"),
            explode(col("D.statusDetails.departure.estimatedTime.outGate.utc")).alias("departure_est_outgte_time"),
            explode(col("D.statusDetails.departure.actualTime.outGateTimeliness")).alias("departure_act_outgte_timely"),
            explode(col("D.statusDetails.departure.actualTime.outGateVariation")).alias("departure_act_outgte_var"),
            explode(col("D.statusDetails.departure.actualTime.outGate.utc")).alias("departure_act_outgte_time"),
            explode(col("D.statusDetails.departure.actualTime.offGround.utc")).alias("departure_act_offgr_time"),
            explode(col("D.statusDetails.departure.airport.iata")).alias("stat_departure_iata_cde"),
            explode(col("D.statusDetails.departure.actualTerminal")).alias("stat_departure_act_terminal"),
            explode(col("D.statusDetails.departure.gate")).alias("stat_departure_gte"),
            explode(col("D.statusDetails.departure.checkInCounter")).alias("stat_departure_chckin_cntr"),
            explode(col("D.statusDetails.arrival.estimatedTime.inGateTimeliness")).alias("arrival_est_ingte_timely"),
            explode(col("D.statusDetails.arrival.estimatedTime.inGateVariation")).alias("arrival_est_ingte_var"),
            explode(col("D.statusDetails.arrival.estimatedTime.inGate.utc")).alias("arrival_est_ingte_time"),
            explode(col("D.statusDetails.arrival.actualTime.inGateTimeliness")).alias("arrival_act_ingte_timely"),
            explode(col("D.statusDetails.arrival.actualTime.inGateVariation")).alias("arrival_act_ingte_var"),
            explode(col("D.statusDetails.arrival.actualTime.inGate.utc")).alias("arrival_act_ingte_time"),
            explode(col("D.statusDetails.arrival.actualTime.onGround.utc")).alias("arrival_act_ongr_time"),
            explode(col("D.statusDetails.arrival.airport.iata")).alias("stat_arrival_iata_cde"),
            explode(col("D.statusDetails.arrival.actualTerminal")).alias("stat_arrival_act_terminal"),
            explode(col("D.statusDetails.arrival.baggage")).alias("stat_arrival_bgge")
        )

        return df
    
    except Exception as e:
        print(f"Error parsing ADSB file: {e}")
        return None

if __name__ == "__main__":

    # Initialize SparkSession
    spark = SparkSession.builder.appName("AirportKPI").getOrCreate()

    # Parse ADSB JSON into Spark DataFrame
    adsb_filepath = '.\\data\\adsb.json'
    adsb_df = parse_adsb(spark, adsb_filepath)

    # Parse OAG JSON into Spark DataFrame
    oag_filepath = '.\\data\\oag.json'
    oag_df = parse_oag(spark, oag_filepath)

    


    
