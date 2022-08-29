import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
#add following line for CML legacy engine
from pyspark_llap.sql.session import HiveWarehouseSession

storage = os.getenv("STORAGE")
storage = "/tmp"

spark = SparkSession\
  .builder\
  .appName("Airline Data Exploration")\
  .config("spark.security.credentials.hiveserver2.enabled","false")\
  .config("spark.datasource.hive.warehouse.read.jdbc.mode", "client")\
  .config("spark.jars", "/home/cdsw/hive-warehouse-connector-assembly-1.0.0.7.1.7.1000-141.jar")\
  .config("spark.sql.hive.hiveserver2.jdbc.url","jdbc:hive2://hs2-hiveflight.apps.ecs1.cdpkvm.cldr/flight;transportMode=http;httpPath=cliservice;socketTimeout=60;ssl=true;retries=3;user=ldapuser1;password=ldapuser1")\
  .config("spark.executor.memory","8g")\
  .config("spark.executor.cores","4")\
  .config("spark.driver.memory","20g")\
  .config("spark.executor.instances","4")\
  .config("spark.yarn.access.hadoopFileSystems",storage)\
  .getOrCreate()


hive = HiveWarehouseSession.session(spark).build()
flight_df = hive.sql("select * from flight.flights_data_all")
flight_df.persist()

flight_df.printSchema()

sample_normal_flights = flight_df\
  .filter("CANCELLED == 0")\
  .sample(withReplacement=False, fraction=0.03, seed=3)
  
cancelled_flights = flight_df\
  .filter("CANCELLED == 1")
  
all_flight_data = cancelled_flights.union(sample_normal_flights)
all_flight_data.persist()
#all_flight_data = all_flight_data.withColumn("date",to_date(concat_ws("-","year","month","dayofmonth"))).withColumn("week",weekofyear("date"))

all_flight_data = all_flight_data\
  .withColumn(
      'HOUR', 
      substring(    
          when(length(col("CRS_DEP_TIME")) == 4,col("CRS_DEP_TIME")).otherwise(concat(lit("0"),col("CRS_DEP_TIME")))
      ,1,2).cast('integer')

  )\
  .withColumn(
    'WEEK', weekofyear('FL_DATE')
  )

smaller_all_flight_data = all_flight_data.select( 
  "FL_DATE",
  "OP_CARRIER",
  "OP_CARRIER_FL_NUM",
  "ORIGIN",
  "DEST",
  "CRS_DEP_TIME",
  "CRS_ARR_TIME",
  "CANCELLED",
  "CRS_ELAPSED_TIME",
  "DISTANCE",
  "HOUR",
  "WEEK"
)



smaller_all_flight_data.printSchema()
#smaller_all_flight_data.write.csv(storage + "/datalake/data/airlines/csv/all_data",mode='overwrite',header=True)
#smaller_all_flight_data = spark.read.csv(storage + "/datalake/data/airlines/csv/all_data",inferSchema=True,header=True)

smaller_all_flight_data_pd = smaller_all_flight_data.toPandas()
smaller_all_flight_data_pd.to_csv('data/all_flight_data_spark.csv', index=False )
spark.stop()
