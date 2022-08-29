import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
#add following line for CML legacy engine
from pyspark_llap.sql.session import HiveWarehouseSession

storage = "/tmp"

spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .config("spark.security.credentials.hiveserver2.enabled","false")\
    .config("spark.datasource.hive.warehouse.read.jdbc.mode", "client")\
    .config("spark.jars", "/home/cdsw/hive-warehouse-connector-assembly-1.0.0.7.1.7.1000-141.jar")\
    .config("spark.sql.hive.hiveserver2.jdbc.url","jdbc:hive2://hs2-hiveflight.apps.ecs1.cdpkvm.cldr/flight;transportMode=http;httpPath=cliservice;socketTimeout=60;ssl=true;retries=3;user=ldapuser1;password=ldapuser1")\
    .config("spark.executor.memory","8g")\
    .config("spark.executor.cores","2")\
    .config("spark.driver.memory","6g")\
    .config("spark.executor.instances","2")\
    .config("spark.yarn.access.hadoopFileSystems",storage)\
    .getOrCreate()


set_1_schema = StructType(
    [
      StructField("month", DoubleType(), True),
      StructField("dayofmonth", DoubleType(), True),
      StructField("dayofweek", DoubleType(), True),
      StructField("deptime", DoubleType(), True),
      StructField("crsdeptime", DoubleType(), True),
      StructField("arrtime", DoubleType(), True),
      StructField("crsarrtime", DoubleType(), True),
      StructField("uniquecarrier", StringType(), True),
      StructField("flightnum", DoubleType(), True),
      StructField("tailnum", StringType(), True),
      StructField("actualelapsedtime", DoubleType(), True),
      StructField("crselapsedtime", DoubleType(), True),
      StructField("airtime", DoubleType(), True),
      StructField("arrdelay", DoubleType(), True),
      StructField("depdelay", DoubleType(), True),
      StructField("origin", StringType(), True),
      StructField("dest", StringType(), True),
      StructField("distance", DoubleType(), True),
      StructField("taxiin", DoubleType(), True),
      StructField("taxiout", DoubleType(), True),
      StructField("cancelled", DoubleType(), True),
      StructField("cancellationcode", StringType(), True),
      StructField("diverted", DoubleType(), True),
      StructField("carrierdelay", DoubleType(), True),
      StructField("weatherdelay", DoubleType(), True),
      StructField("nasdelay", DoubleType(), True),
      StructField("securitydelay", DoubleType(), True),
      StructField("lateaircraftdelay", DoubleType(), True),
      StructField("year", DoubleType(), True)
    ]
)

# Now we can read in the data from Cloud Storage into Spark...


flights_data_1 = spark.read.csv(
    "{}/datalake/data/flight_data/set_1/".format(
        storage),
    header=True,
    #inferSchema = True,
    schema=set_1_schema,
    sep=','
)

# ...and inspect the data.

flights_data_1.show()

flights_data_1.printSchema()

#```
#FL_DATE,OP_CARRIER,OP_CARRIER_FL_NUM,ORIGIN,DEST,CRS_DEP_TIME,DEP_TIME,DEP_DELAY,TAXI_OUT,WHEELS_OFF,WHEELS_ON,TAXI_IN,CRS_ARR_TIME,ARR_TIME,ARR_DELAY,CANCELLED,CANCELLATION_CODE,DIVERTED,CRS_ELAPSED_TIME,ACTUAL_ELAPSED_TIME,AIR_TIME,DISTANCE,CARRIER_DELAY,WEATHER_DELAY,NAS_DELAY,SECURITY_DELAY,LATE_AIRCRAFT_DELAY,Unnamed: 27
#2009-01-01,XE,1204,DCA,EWR,1100,1058.0,-2.0,18.0,1116.0,1158.0,8.0,1202,1206.0,4.0,0.0,,0.0,62.0,68.0,42.0,199.0,,,,,,
#2009-01-01,XE,1206,EWR,IAD,1510,1509.0,-1.0,28.0,1537.0,1620.0,4.0,1632,1624.0,-8.0,0.0,,0.0,82.0,75.0,43.0,213.0,,,,,,
#```

set_2_schema = StructType([StructField("FL_DATE", DateType(), True),
    StructField("OP_CARRIER", StringType(), True),
    StructField("OP_CARRIER_FL_NUM", StringType(), True),
    StructField("ORIGIN", StringType(), True),
    StructField("DEST", StringType(), True),
    StructField("CRS_DEP_TIME", StringType(), True),
    StructField("DEP_TIME", StringType(), True),
    StructField("DEP_DELAY", DoubleType(), True),
    StructField("TAXI_OUT", DoubleType(), True),
    StructField("WHEELS_OFF", StringType(), True),
    StructField("WHEELS_ON", StringType(), True),
    StructField("TAXI_IN", DoubleType(), True),
    StructField("CRS_ARR_TIME", StringType(), True),
    StructField("ARR_TIME", StringType(), True),
    StructField("ARR_DELAY", DoubleType(), True),
    StructField("CANCELLED", DoubleType(), True),
    StructField("CANCELLATION_CODE", StringType(), True),
    StructField("DIVERTED", DoubleType(), True),
    StructField("CRS_ELAPSED_TIME", DoubleType(), True),
    StructField("ACTUAL_ELAPSED_TIME", DoubleType(), True),
    StructField("AIR_TIME", DoubleType(), True),
    StructField("DISTANCE", DoubleType(), True),
    StructField("CARRIER_DELAY", DoubleType(), True),
    StructField("WEATHER_DELAY", DoubleType(), True),
    StructField("NAS_DELAY", DoubleType(), True),
    StructField("SECURITY_DELAY", DoubleType(), True),
    StructField("LATE_AIRCRAFT_DELAY", DoubleType(), True)])


flights_data_2 = spark.read.csv(
    "{}/datalake/data/flight_data/set_2/".format(
        storage),
    schema=set_2_schema,
    header=True,
    sep=',',
    nullValue='NA'
)

flights_data_2.show()

flights_data_1 = flights_data_1\
  .withColumn("FL_DATE",
              to_date(
                concat_ws('-',col("year"),col("month"),col("dayofmonth")),
              'yyyy.0-MM.0-dd.0')
                )

flights_data_1 = flights_data_1\
.withColumnRenamed("deptime","DEP_TIME")\
.withColumnRenamed("crsdeptime","CRS_DEP_TIME")\
.withColumnRenamed("arrtime","ARR_TIME")\
.withColumnRenamed("crsarrtime","CRS_ARR_TIME")\
.withColumnRenamed("uniquecarrier","OP_CARRIER")\
.withColumnRenamed("flightnum","OP_CARRIER_FL_NUM")\
.withColumnRenamed("actualelapsedtime","ACTUAL_ELAPSED_TIME")\
.withColumnRenamed("crselapsedtime","CRS_ELAPSED_TIME")\
.withColumnRenamed("airtime","AIR_TIME")\
.withColumnRenamed("arrdelay","ARR_DELAY")\
.withColumnRenamed("depdelay","DEP_DELAY")\
.withColumnRenamed("origin","ORIGIN")\
.withColumnRenamed("dest","DEST")\
.withColumnRenamed("distance","DISTANCE")\
.withColumnRenamed("taxiin","TAXI_IN")\
.withColumnRenamed("taxiout","TAXI_OUT")\
.withColumnRenamed("cancelled","CANCELLED")\
.withColumnRenamed("cancellationcode","CANCELLATION_CODE")\
.withColumnRenamed("diverted","DIVERTED")\
.withColumnRenamed("carrierdelay","CARRIER_DELAY")\
.withColumnRenamed("weatherdelay","WEATHER_DELAY")\
.withColumnRenamed("nasdelay","NAS_DELAY")\
.withColumnRenamed("securitydelay","SECURITY_DELAY")\
.withColumnRenamed("lateaircraftdelay","LATE_AIRCRAFT_DELAY")


flights_data_1 = flights_data_1.select(["FL_DATE","DEP_TIME","CRS_DEP_TIME","ARR_TIME","CRS_ARR_TIME","OP_CARRIER","OP_CARRIER_FL_NUM","ACTUAL_ELAPSED_TIME","CRS_ELAPSED_TIME","AIR_TIME","ARR_DELAY","DEP_DELAY","ORIGIN","DEST","DISTANCE","TAXI_IN","TAXI_OUT","CANCELLED","CANCELLATION_CODE","DIVERTED","CARRIER_DELAY","WEATHER_DELAY","NAS_DELAY","SECURITY_DELAY","LATE_AIRCRAFT_DELAY"])
flights_data_2 = flights_data_2.select(["FL_DATE","DEP_TIME","CRS_DEP_TIME","ARR_TIME","CRS_ARR_TIME","OP_CARRIER","OP_CARRIER_FL_NUM","ACTUAL_ELAPSED_TIME","CRS_ELAPSED_TIME","AIR_TIME","ARR_DELAY","DEP_DELAY","ORIGIN","DEST","DISTANCE","TAXI_IN","TAXI_OUT","CANCELLED","CANCELLATION_CODE","DIVERTED","CARRIER_DELAY","WEATHER_DELAY","NAS_DELAY","SECURITY_DELAY","LATE_AIRCRAFT_DELAY"])

flights_data_all = flights_data_1.unionByName(flights_data_2)

hive = HiveWarehouseSession.session(spark).build()
hive.showDatabases().show()
hive.setDatabase("flight")
hive.showTables().show()
flights_data_all.show

flights_data_all.write.format(HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR).mode("ignore").option("table", "flight.flights_data_all").save()
