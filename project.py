import os
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, mean, row_number, desc, to_timestamp
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType

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

    ##############################################################
    # Calculate basic airport KPI
    ##############################################################

    # Airport Speed
    avg_airport_speed = {
        'by_origin' : adsb_df.groupBy("Origin").agg(mean("Speed")),
        'by_destination' : adsb_df.groupBy("Destination").agg(mean("Speed"))
    }

    speed_origin = avg_airport_speed['by_origin']
    speed_destination = avg_airport_speed['by_destination']

    # Number of delayed flights by arrival delays and departure delays
    delay_key = 'Delayed'
    dep_arr_cols = {
        'departure': 'departure_act_outgte_timely',
        'arrival':  'arrival_act_ingte_timely'
    }
    delays = oag_df.select([(col(t2) == delay_key).cast("int").alias(f"{t1}_delay") 
                   for (t1,t2) in dep_arr_cols.items()]).groupBy().sum()

    print("The total non-delays are as follows:")
    non_delays = oag_df.select([(col(t2) != delay_key).cast("int").alias(f"{t1}_non_delay") 
                   for (t1,t2) in dep_arr_cols.items()]).groupBy().sum()


    ##############################################################
    # Filtering and transforming DataFrame
    ##############################################################

    # Filter the DataFrame to retain only the most recent entry 
    # (the one with the largest LastUpdate) for each FlightId.
    # Return a DataFrame containing only the FlightId and the 
    # corresponding latest LastUpdate.
    w_lastupdate = Window.partitionBy("Flight").orderBy(desc("LastUpdate"))
    transforms = {
        "row" : row_number().over(w_lastupdate),
        "lastUpdate" : to_timestamp(adsb_df.LastUpdate.cast(dataType=TimestampType()))
    }
    entry = adsb_df.withColumns(transforms)\
        .filter(col("row") == 1)\
        .drop("row")\
        .select(col('Flight').alias('FlightId'),col('LastUpdate'))
    
    ##############################################################
    # Plotting the result and save it in .\results\
    ##############################################################

    dest_folder = ".\\results\\"

    # Airport speed (origin)
    speed_origin_pd = speed_origin.toPandas()
    plt.figure(figsize=(8,6))
    plt.bar(speed_origin_pd["Origin"], speed_origin_pd["avg(Speed)"])
    plt.title('Speed by Origin Airport')
    plt.xlabel('Airport')
    plt.ylabel('Average Speed')
    # Save the plot as a JPG file
    plt.savefig(f"{dest_folder}speed_origin.jpg", format='jpg', dpi=300)  # dpi=300 for high resolution

    # Airport speed (destination)
    speed_destination_pd = speed_destination.toPandas()
    plt.figure(figsize=(8,6))
    plt.bar(speed_destination_pd["Destination"], speed_origin_pd["avg(Speed)"])
    plt.title('Speed by Destination Airport')
    plt.xlabel('Airport')
    plt.ylabel('Average Speed')
    # Save the plot as a JPG file
    plt.savefig(f"{dest_folder}speed_destination.jpg", format='jpg', dpi=300)  # dpi=300 for high resolution
    
    # Delays
    delays_pd = delays.toPandas()
    non_delays_pd = non_delays.toPandas()
    fig, (ax1,ax2) = plt.subplots(2)
    ax1.axis('tight')
    ax1.axis('off')
    ax2.axis('tight')
    ax2.axis('off')
    fig.suptitle("Delays and Non-delays")
    ax1.table(cellText=delays_pd.values, colLabels=delays_pd.columns, loc='center', cellLoc='center')
    ax2.table(cellText=non_delays_pd.values, colLabels=non_delays_pd.columns, loc='center', cellLoc='center')
    # Save the plot as a JPG file
    plt.savefig(f"{dest_folder}delays.jpg", format='jpg', dpi=300)  # dpi=300 for high resolution

    # Last_entry
    entry_pd = entry.toPandas()
    fig, ax = plt.subplots(1)
    ax.axis('tight')
    ax.axis('off')
    fig.suptitle("Most recent entry of each flights")
    ax.table(cellText=entry_pd.values, colLabels=entry_pd.columns, loc='center', cellLoc='center')
    # Save the plot as a JPG file
    plt.savefig(f"{dest_folder}last_entry.jpg", format='jpg', dpi=300)  # dpi=300 for high resolution












    
