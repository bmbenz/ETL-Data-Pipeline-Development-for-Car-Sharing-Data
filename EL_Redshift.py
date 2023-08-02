
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
dim_group = glueContext.create_dynamic_frame.from_catalog(database='dim_carsharing', table_name='dim_group')
dim_promotion = glueContext.create_dynamic_frame.from_catalog(database='dim_carsharing', table_name='dim_promotion')
dim_station = glueContext.create_dynamic_frame.from_catalog(database='dim_carsharing', table_name='dim_station')
dim_user = glueContext.create_dynamic_frame.from_catalog(database='dim_carsharing', table_name='dim_user')
dim_vehicle = glueContext.create_dynamic_frame.from_catalog(database='dim_carsharing', table_name='dim_vehicle')
fac_reservation = glueContext.create_dynamic_frame.from_catalog(database='dim_carsharing', table_name='fac_reservation')
def load_data_to_redshift(table_name,df):
    my_conn_options = {
        "dbtable": f"public.{table_name}",
        "database": "dev"
    }

    redshift_results = glueContext.write_dynamic_frame.from_jdbc_conf(
        frame = df,
        catalog_connection = "S3_Redshift",
        connection_options = my_conn_options,
        redshift_tmp_dir = "s3://car-sharing-bucket-haup/redshift_temp_folder/")
#dimgroup
load_data_to_redshift("dimgroup",dim_group)
#dimpromotion
load_data_to_redshift("dimpromotion",dim_promotion)
#dimstation
load_data_to_redshift("dimstation",dim_station)
#dimuser
load_data_to_redshift("dimuser",dim_user)
#dimvehicle
load_data_to_redshift("dimvehicle",dim_vehicle)
#facreservation
load_data_to_redshift("facreservation",fac_reservation)
job.commit()