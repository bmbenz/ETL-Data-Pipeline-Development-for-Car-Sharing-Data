
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions 
from pyspark.sql.functions import when
from awsglue.dynamicframe import DynamicFrame
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
distance_table = glueContext.create_dynamic_frame_from_options(connection_type='s3',connection_options={'paths':["s3://car-sharing-bucket/data/distance/"]},
                                            format='csv', format_options={"withHeader":True,"optimizePerformance":True}).toDF()
group_table = glueContext.create_dynamic_frame_from_options(connection_type='s3',connection_options={'paths':["s3://car-sharing-bucket/data/group/"]},
                                            format='csv', format_options={"withHeader":True,"optimizePerformance":True}).toDF()
promotion_table = glueContext.create_dynamic_frame_from_options(connection_type='s3',connection_options={'paths':["s3://car-sharing-bucket/data/promotion/"]},
                                            format='csv', format_options={"withHeader":True,"optimizePerformance":True}).toDF()
reservation_table = glueContext.create_dynamic_frame_from_options(connection_type='s3',connection_options={'paths':["s3://car-sharing-bucket/data/reservation/"]},
                                            format='csv', format_options={"withHeader":True,"optimizePerformance":True}).toDF()
station_table = glueContext.create_dynamic_frame_from_options(connection_type='s3',connection_options={'paths':["s3://car-sharing-bucket/data/station/"]},
                                            format='csv', format_options={"withHeader":True,"optimizePerformance":True}).toDF()
user_table = glueContext.create_dynamic_frame_from_options(connection_type='s3',connection_options={'paths':["s3://car-sharing-bucket/data/user/"]},
                                            format='csv', format_options={"withHeader":True,"optimizePerformance":True}).toDF()
vehicle_table = glueContext.create_dynamic_frame_from_options(connection_type='s3',connection_options={'paths':["s3://car-sharing-bucket/data/vehicle/"]},
                                            format='csv', format_options={"withHeader":True,"optimizePerformance":True}).toDF()
vehiclesize_table = glueContext.create_dynamic_frame_from_options(connection_type='s3',connection_options={'paths':["s3://car-sharing-bucket/data/vehiclesize/"]},
                                            format='csv', format_options={"withHeader":True,"optimizePerformance":True}).toDF()
dim_group = group_table
dim_group.printSchema()
dim_group = dim_group.withColumn("groupid",dim_group['groupid'].cast("int"))
dim_group.printSchema()
dim_promotion = promotion_table.drop('master_promotionid','require_promotionid')
dim_promotion = dim_promotion.na.drop(how='any',subset=['promotioncode'])
dim_promotion.printSchema()
dim_promotion = dim_promotion.withColumn("promotionid",dim_promotion['promotionid'].cast("int"))
dim_promotion = dim_promotion.withColumn("effectedtime",functions.to_timestamp(dim_promotion['effectedtime'], 'yyyy-MM-dd HH:mm:ss'))
dim_promotion = dim_promotion.withColumn("expiredtime",functions.to_timestamp(dim_promotion['expiredtime'], 'yyyy-MM-dd HH:mm:ss'))
dim_promotion.printSchema()
dim_station = station_table.drop('zone','availableparking','district_th','province_th','address')
dim_station = dim_station.withColumnRenamed('province_en\r','province_en')
dim_station.printSchema()
dim_station = dim_station.withColumn("stationid",dim_station['stationid'].cast("int"))
dim_station.printSchema()
dim_user = user_table.drop('last_login\r')
dim_user.printSchema()
dim_user = dim_user.withColumn("userid",dim_user['userid'].cast("int"))
dim_user.printSchema()
vehiclesize = vehiclesize_table.select('vehicleid','brand','model','size','type').withColumnRenamed('vehicleid','_vehicleid')
dim_vehicle = vehicle_table.join(vehiclesize, vehicle_table.vehicleid==vehiclesize._vehicleid, 'left').drop('_vehicleid')
dim_vehicle.printSchema()
dim_vehicle = dim_vehicle.withColumn("vehicleid",dim_vehicle['vehicleid'].cast("int"))
dim_vehicle.printSchema()
distance = distance_table.withColumnRenamed('reservationno','_reservationno').withColumnRenamed('distance\r','distance')
fac_reservation = reservation_table.select('reservationno','reservationstate','userid','groupid','vehicleid','stationid','promotionid','reservestarttime','reservestoptime','reservehours','discount','totalprice','chargetotal','category')
fac_reservation = fac_reservation.filter((fac_reservation.groupid!=0)&(fac_reservation.stationid!=0))
fac_reservation = fac_reservation.join(distance, fac_reservation.reservationno==distance._reservationno, 'left').drop('_reservationno')
fac_reservation.printSchema()
fac_reservation = fac_reservation.withColumn("reservationno",fac_reservation['reservationno'].cast("int"))
fac_reservation = fac_reservation.withColumn("userid",fac_reservation['userid'].cast("int"))
fac_reservation = fac_reservation.withColumn("groupid",fac_reservation['groupid'].cast("int"))
fac_reservation = fac_reservation.withColumn("vehicleid",fac_reservation['vehicleid'].cast("int"))
fac_reservation = fac_reservation.withColumn("stationid",fac_reservation['stationid'].cast("int"))
fac_reservation = fac_reservation.withColumn("promotionid",fac_reservation['promotionid'].cast("int"))
fac_reservation = fac_reservation.withColumn("reservestarttime",functions.to_timestamp(fac_reservation['reservestarttime'], 'yyyy-MM-dd HH:mm:ss'))
fac_reservation = fac_reservation.withColumn("reservestoptime",functions.to_timestamp(fac_reservation['reservestoptime'], 'yyyy-MM-dd HH:mm:ss'))
fac_reservation = fac_reservation.withColumn("reservehours",fac_reservation['reservehours'].cast("int"))
fac_reservation = fac_reservation.withColumn("totalprice",fac_reservation['totalprice'].cast("double"))
fac_reservation = fac_reservation.withColumn("discount",fac_reservation['discount'].cast("double"))
fac_reservation = fac_reservation.withColumn("chargetotal",fac_reservation['chargetotal'].cast("double"))
fac_reservation = fac_reservation.withColumn("distance",fac_reservation['distance'].cast("double"))
fac_reservation.printSchema()
def load_data_s3(df,path,database_name,table_name,df_name):
    s3output = glueContext.getSink(
      path=path,
      connection_type="s3",
      updateBehavior="UPDATE_IN_DATABASE",
      partitionKeys=[],
      compression="snappy",
      enableUpdateCatalog=True,
      transformation_ctx="s3output",
    )
    s3output.setCatalogInfo(catalogDatabase=database_name, catalogTableName=table_name)
    s3output.setFormat("glueparquet")
    s3output.writeFrame(DynamicFrame.fromDF(df, glueContext, df_name))
#dim_group
load_data_s3(dim_group,'s3://car-sharing-bucket-haup/transformed_data/dim_group',"dim_carsharing","dim_group","dim_group")
#dim_promotion
load_data_s3(dim_promotion,'s3://car-sharing-bucket-haup/transformed_data/dim_promotion',"dim_carsharing","dim_promotion","dim_promotion")
#dim_station
load_data_s3(dim_station,'s3://car-sharing-bucket-haup/transformed_data/dim_station',"dim_carsharing","dim_station","dim_station")
#dim_user
load_data_s3(dim_user,'s3://car-sharing-bucket-haup/transformed_data/dim_user',"dim_carsharing","dim_user","dim_user")
#dim_vehicle
load_data_s3(dim_vehicle,'s3://car-sharing-bucket-haup/transformed_data/dim_vehicle',"dim_carsharing","dim_vehicle","dim_vehicle")
#fac_reservation
load_data_s3(fac_reservation,'s3://car-sharing-bucket-haup/transformed_data/fac_reservation',"dim_carsharing","fac_reservation","fac_reservation")
job.commit()