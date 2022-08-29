## Part 0: Bootstrap File
# Use CML legacy engine

# Install the requirements
!pip3 install --progress-bar off -r requirements.txt

# Create the directories and upload data
from cmlbootstrap import CMLBootstrap
import os
import xml.etree.ElementTree as ET

# Set the setup variables needed by CMLBootstrap
HOST = os.getenv("CDSW_API_URL").split(
    ":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split(
    "/")[6]  # args.username  # "vdibia"
API_KEY = os.getenv("CDSW_API_KEY") 
PROJECT_NAME = os.getenv("CDSW_PROJECT")  

# Instantiate API Wrapper
cml = CMLBootstrap(HOST, USERNAME, API_KEY, PROJECT_NAME)

# Set the STORAGE environment variable
# hard code the storage path
STORAGE = "/tmp"

# Upload the data to the cloud storage
!mkdir data
!cp all_flight_data.tgz data
!cd data && tar xjvf all_flight_data.tgz

!hdfs dfs -mkdir -p $STORAGE/datalake
!hdfs dfs -mkdir -p $STORAGE/datalake/data
!hdfs dfs -mkdir -p $STORAGE/datalake/data/flight_data
!hdfs dfs -mkdir -p $STORAGE/datalake/data/flight_data/set_1
!hdfs dfs -mkdir -p $STORAGE/datalake/data/flight_data/set_2

!curl https://cdp-demo-data.s3-us-west-2.amazonaws.com/all_flight_data.zip | zcat | hadoop fs -put - $STORAGE/datalake/data/flight_data/set_1/flight_data_1.csv
!for i in $(seq 2009 2018); do curl https://cdp-demo-data.s3-us-west-2.amazonaws.com/$i.csv | hadoop fs -put - $STORAGE/datalake/data/flight_data/set_2/$i.csv; done

