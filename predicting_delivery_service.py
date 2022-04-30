# Databricks notebook source
# MAGIC %md
# MAGIC #### IESEG School of Management - MSc in Big Data Analytics for Business
# MAGIC #### Academic Year 2022
# MAGIC 
# MAGIC ##### Group Project for Big Data Tools by:  Inderpreet Rana, Mui Han Ma & Vinay Rajagopalan

# COMMAND ----------

parsed_business_path = "/FileStore/tables/parsed_business.json"
parsed_checkin_path = "/FileStore/tables/parsed_checkin.json"
parsed_tip_path = "/FileStore/tables/parsed_tip.json"
parsed_review_path = "/FileStore/tables/parsed_review.json"
parsed_covid_path = "/FileStore/tables/parsed_covid.json"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import json_tuple
import pandas

# COMMAND ----------

# DBTITLE 1,Business Parking Schema
business_parking_schema = StructType([
          StructField("garage", BooleanType(), True),
          StructField("street", BooleanType(), True),
          StructField("validated", BooleanType(), True),
          StructField("lot", BooleanType(), True),
          StructField("valet", BooleanType(), True)
    ])

# COMMAND ----------

# DBTITLE 1,Business Ambience Schema
ambience_schema = StructType([
          StructField("touristy", BooleanType(), True),
          StructField("hipster", BooleanType(), True),
          StructField("romantic", BooleanType(), True),
          StructField("intimate", BooleanType(), True),
          StructField("trendy", BooleanType(), True),
          StructField("upscale", BooleanType(), True),
          StructField("classy", BooleanType(), True),
          StructField("casual", BooleanType(), True),
    ])

# COMMAND ----------

# DBTITLE 1,Business Good for Meal Schema
good_for_meal_schema = StructType([
          StructField("dessert", BooleanType(), True),
          StructField("latenight", BooleanType(), True),
          StructField("lunch", BooleanType(), True),
          StructField("dinner", BooleanType(), True),
          StructField("brunch", BooleanType(), True),
          StructField("breakfast", BooleanType(), True)
    ])

# COMMAND ----------

# DBTITLE 1,Business Best Nights Schema
best_nights_schema = StructType([
          StructField("monday", BooleanType(), True),
          StructField("tuesday", BooleanType(), True),
          StructField("wednesday", BooleanType(), True),
          StructField("thursday", BooleanType(), True),
          StructField("friday", BooleanType(), True),
          StructField("saturday", BooleanType(), True),
          StructField("sunday", BooleanType(), True),
    ])

# COMMAND ----------

# DBTITLE 1,Business Music Schema
music_schema = StructType([
          StructField("dj", BooleanType(), True),
          StructField("background_music", BooleanType(), True),
          StructField("no_music", BooleanType(), True),
          StructField("jukebox", BooleanType(), True),
          StructField("live", BooleanType(), True),
          StructField("video", BooleanType(), True),
          StructField("karaoke", BooleanType(), True),
    ])

# COMMAND ----------

# DBTITLE 1,Business Schema
business_schema = StructType([
  StructField("business_id", StringType(), True),
  StructField("name", StringType(), True),
  StructField("address", StringType(), True),
  StructField("city", StringType(), True),
  StructField("state", StringType(), True),
  StructField("postal_code", StringType(), True),
  StructField("latitude", FloatType(), True),
  StructField("longitude", FloatType(), True),
  StructField("stars", FloatType(), True),
  StructField("review_count", IntegerType(), True),
  StructField("is_open", IntegerType(), True),
  StructField("categories", StringType(), True),
  StructField("attributes.BikeParking", StringType(), True),
  StructField("attributes.GoodForKids", StringType(), True),
  StructField("attributes.BusinessParking", StringType(), True),
  StructField("attributes.ByAppointmentOnly", StringType(), True),
  StructField("attributes.RestaurantsPriceRange2", StringType(), True),
  StructField("hours.Monday", StringType(), True),
  StructField("hours.Tuesday", StringType(), True),
  StructField("hours.Wednesday", StringType(), True),
  StructField("hours.Thursday", StringType(), True),
  StructField("hours.Friday", StringType(), True),
  StructField("hours.Saturday", StringType(), True),
  StructField("hours.Sunday", StringType(), True),
  StructField("attributes.WiFi", StringType(), True),
  StructField("attributes.RestaurantsAttire", StringType(), True),
  StructField("attributes.RestaurantsTakeOut", StringType(), True),
  StructField("attributes.NoiseLevel", StringType(), True),
  StructField("attributes.RestaurantsReservations", StringType(), True),
  StructField("attributes.RestaurantsGoodForGroups", StringType(), True),
  StructField("attributes.HasTV", StringType(), True),
  StructField("attributes.Alcohol", StringType(), True),
  StructField("attributes.RestaurantsDelivery", StringType(), True),
  StructField("attributes.OutdoorSeating", StringType(), True),
  StructField("attributes.Caters", StringType(), True),
  StructField("attributes.Ambience", StringType(), True),
  StructField("attributes.RestaurantsTableService", StringType(), True),
  StructField("attributes.GoodForMeal", StringType(), True),
  StructField("attributes.BusinessAcceptsCreditCards", StringType(), True),
  StructField("attributes.WheelchairAccessible", StringType(), True),
  StructField("attributes.BusinessAcceptsBitcoin", StringType(), True),
  StructField("attributes.DogsAllowed", StringType(), True),
  StructField("attributes.HappyHour", StringType(), True),
  StructField("attributes.GoodForDancing", StringType(), True),
  StructField("attributes.CoatCheck", StringType(), True),
  StructField("attributes.BestNights", StringType(), True),
  StructField("attributes.Music", StringType(), True),
  StructField("attributes.Smoking", StringType(), True),
  StructField("attributes.DriveThru", StringType(), True),
  StructField("attributes.AcceptsInsurance", StringType(), True),
  StructField("attributes.BYOBCorkage", StringType(), True),
  StructField("attributes.HairSpecializesIn", StringType(), True),
  StructField("attributes.Corkage", StringType(), True),
  StructField("attributes.AgesAllowed", StringType(), True),
  StructField("attributes.BYOB", StringType(), True),
  StructField("attributes.DietaryRestrictions", StringType(), True),
  StructField("attributes.RestaurantsCounterService", StringType(), True),
  StructField("attributes.Open24Hours", StringType(), True),
])

# COMMAND ----------

# DBTITLE 1,Read Business Json with Manual Schema
business = spark\
.read\
.format("json")\
.option("header","true")\
.schema(business_schema)\
.load(parsed_business_path)


# COMMAND ----------

display(business)

# COMMAND ----------

business.printSchema()

# COMMAND ----------

# DBTITLE 1,Replace attributes in colnames 
for col in business.columns:
    if 'attributes' in col:
        business = business.withColumnRenamed(col,col.replace('attributes.', ''))

# COMMAND ----------

display(business)

# COMMAND ----------

display(business.describe())

# COMMAND ----------

# DBTITLE 1,Evaluate Json field Business Parking for valid strings and append them as columns
import json
import ast
import pyspark.sql.functions as F

dict_to_json = F.udf(lambda x: json.dumps(ast.literal_eval(str(x))))

df = business.withColumn("BusinessParking", F.from_json(dict_to_json("BusinessParking"),business_parking_schema)).select("business_id","BusinessParking.*")
joinType="inner"
lf = business.join(df,["business_id"],joinType)
lf = lf.drop("BusinessParking")


# COMMAND ----------

joinType="inner"
lf = business.join(df,["business_id"],joinType)
lf = lf.drop("BusinessParking")

# COMMAND ----------

# DBTITLE 1,Evaluate Json field Ambience for valid strings and append them as columns
dict_to_json = F.udf(lambda x: json.dumps(ast.literal_eval(str(x))))

df = business.withColumn("Ambience", F.from_json(dict_to_json("Ambience"),ambience_schema)).select("business_id","Ambience.*")
joinType="inner"
lf = lf.join(df,["business_id"],joinType)
lf = lf.drop("Ambience")

# COMMAND ----------

# DBTITLE 1,Evaluate Json field Good for Meal for valid strings and append them as columns
dict_to_json = F.udf(lambda x: json.dumps(ast.literal_eval(str(x))))

df = business.withColumn("GoodForMeal", F.from_json(dict_to_json("GoodForMeal"),good_for_meal_schema)).select("business_id","GoodForMeal.*")
joinType="inner"
lf = lf.join(df,["business_id"],joinType)
lf = lf.drop("GoodForMeal")

# COMMAND ----------

# DBTITLE 1,Evaluate Json field Best Nights for valid strings and append them as columns
dict_to_json = F.udf(lambda x: json.dumps(ast.literal_eval(str(x))))

df = business.withColumn("BestNights", F.from_json(dict_to_json("BestNights"),best_nights_schema)).select("business_id","BestNights.*")
joinType="inner"
lf = lf.join(df,["business_id"],joinType)
lf = lf.drop("BestNights")

# COMMAND ----------

display(lf)

# COMMAND ----------

# DBTITLE 1,Evaluate Json field Music for valid strings and append them as columns
dict_to_json = F.udf(lambda x: json.dumps(ast.literal_eval(str(x))))

df = business.withColumn("Music", F.from_json(dict_to_json("Music"),music_schema)).select("business_id","Music.*")
joinType="inner"
lf = lf.join(df,["business_id"],joinType)
lf = lf.drop("Music")

# COMMAND ----------

print((lf.count(), len(lf.columns)))
print((business.count(), len(business.columns)))

# COMMAND ----------

display(lf.summary())

# COMMAND ----------

# DBTITLE 1,Remove escape characters and qoutes from string value
from pyspark.sql.functions import *

lf = lf.withColumn('NoiseLevel', regexp_replace('NoiseLevel', 'u\'', ''))
lf = lf.withColumn('NoiseLevel', regexp_replace('NoiseLevel', '\'', ''))

# COMMAND ----------

lf = lf.withColumn('RestaurantsAttire', regexp_replace('RestaurantsAttire', 'u\'', ''))
lf = lf.withColumn('RestaurantsAttire', regexp_replace('RestaurantsAttire', '\'', ''))

# COMMAND ----------

lf = lf.withColumn('WiFi', regexp_replace('WiFi', 'u\'', ''))
lf = lf.withColumn('WiFi', regexp_replace('WiFi', '\'', ''))

# COMMAND ----------

lf = lf.withColumn('Alcohol', regexp_replace('Alcohol', 'u\'', ''))
lf = lf.withColumn('Alcohol', regexp_replace('Alcohol', '\'', ''))

# COMMAND ----------

lf = lf.withColumn('AgesAllowed', regexp_replace('AgesAllowed', 'u\'', ''))
lf = lf.withColumn('AgesAllowed', regexp_replace('AgesAllowed', '\'', ''))


# COMMAND ----------

lf = lf.withColumn('Smoking', regexp_replace('Smoking', 'u\'', ''))
lf = lf.withColumn('Smoking', regexp_replace('Smoking', '\'', ''))

# COMMAND ----------

# DBTITLE 1,Change datatype for columns to Boolean
lf = lf.withColumn("BikeParking",lf.BikeParking.cast(BooleanType()))
lf = lf.withColumn("GoodForKids",lf.GoodForKids.cast(BooleanType()))
lf = lf.withColumn("ByAppointmentOnly",lf.ByAppointmentOnly.cast(BooleanType()))
lf = lf.withColumn("RestaurantsTakeOut",lf.RestaurantsTakeOut.cast(BooleanType()))
lf = lf.withColumn("RestaurantsReservations",lf.RestaurantsReservations.cast(BooleanType()))
lf = lf.withColumn("RestaurantsGoodForGroups",lf.RestaurantsGoodForGroups.cast(BooleanType()))
lf = lf.withColumn("HasTV",lf.HasTV.cast(BooleanType()))
lf = lf.withColumn("RestaurantsDelivery",lf.RestaurantsDelivery.cast(BooleanType()))
lf = lf.withColumn("OutdoorSeating",lf.OutdoorSeating.cast(BooleanType()))
lf = lf.withColumn("Caters",lf.Caters.cast(BooleanType()))
lf = lf.withColumn("RestaurantsTableService",lf.RestaurantsTableService.cast(BooleanType()))
lf = lf.withColumn("BusinessAcceptsCreditCards",lf.BusinessAcceptsCreditCards.cast(BooleanType()))
lf = lf.withColumn("WheelchairAccessible",lf.WheelchairAccessible.cast(BooleanType()))
lf = lf.withColumn("BusinessAcceptsBitcoin",lf.BusinessAcceptsBitcoin.cast(BooleanType()))
lf = lf.withColumn("DogsAllowed",lf.DogsAllowed.cast(BooleanType()))
lf = lf.withColumn("HappyHour",lf.HappyHour.cast(BooleanType()))
lf = lf.withColumn("GoodForDancing",lf.GoodForDancing.cast(BooleanType()))
lf = lf.withColumn("CoatCheck",lf.CoatCheck.cast(BooleanType()))
lf = lf.withColumn("DriveThru",lf.DriveThru.cast(BooleanType()))
lf = lf.withColumn("Corkage",lf.Corkage.cast(BooleanType()))

# COMMAND ----------

display(lf)

# COMMAND ----------

business =  lf
business.printSchema()

# COMMAND ----------

# DBTITLE 1,Filter business to include only resturants
business_restaurants = business.filter(business.categories.contains('Restaurants')) 

# COMMAND ----------

print((business_restaurants.count(), len(business_restaurants.columns)))

# COMMAND ----------

display(business_restaurants)

# COMMAND ----------

# DBTITLE 1,Checkin Schema
checkin_schema =  StructType([
          StructField("business_id", StringType(), True),
          StructField("date", TimestampType(), True),
    ])

# COMMAND ----------

# DBTITLE 1,Read Business Json with Manual Schema
checkin = spark\
.read\
.format("json")\
.option("header","true")\
.schema(checkin_schema)\
.load(parsed_checkin_path)

# COMMAND ----------

checkin.printSchema()

# COMMAND ----------

display(checkin)

# COMMAND ----------

min_date, max_date = checkin.select(min("date"), max("date")).first()
min_date, max_date

# COMMAND ----------

import datetime
cutoffdate = datetime.datetime(2020, 6, 1, 0, 0, 0)

# COMMAND ----------

# DBTITLE 1,Filter checkin greater than timeline
checkin_filter = checkin.where( checkin.date < cutoffdate)

# COMMAND ----------

# DBTITLE 1,Exclude checkin not in business_restaurants
joinType="left_semi"

#Semi join using DF
checkin_restaurants = checkin_filter.join(business_restaurants,["business_id"],joinType)

# COMMAND ----------

print((checkin.count(), len(checkin.columns)))
print((checkin_filter.count(), len(checkin_filter.columns)))
print((checkin_restaurants.count(), len(checkin_restaurants.columns)))

# COMMAND ----------

# DBTITLE 1,Aggreagte Results
checkin_count = checkin_restaurants.groupBy("business_id").agg(count("date").alias('checkin_count'))
joinType="left_outer"
business_restaurants = business_restaurants.join(checkin_count,["business_id"],joinType)

# COMMAND ----------

from pyspark.sql.functions import col, max as max_

checkin_count = checkin_restaurants.groupBy("business_id").agg(max_("date").alias('last_checkin'))
joinType="left_outer"
business_restaurants = business_restaurants.join(checkin_count,["business_id"],joinType)

# COMMAND ----------

print((business_restaurants.count(), len(business_restaurants.columns)))

# COMMAND ----------

display(business_restaurants)

# COMMAND ----------

# DBTITLE 1,Tip Schema
tip_schema =  StructType([
          StructField("business_id", StringType(), True),
          StructField("compliment_count", IntegerType(), True),
          StructField("date", TimestampType(), True),
          StructField("text", StringType(), True),
          StructField("user_id", StringType(), True),
    ])

# COMMAND ----------

# DBTITLE 1,Read Tip Json with Manual Schema
tip = spark\
.read\
.format("json")\
.option("header","true")\
.schema(tip_schema)\
.load(parsed_tip_path)

# COMMAND ----------

tip.printSchema()

# COMMAND ----------

display(tip)

# COMMAND ----------

min_date, max_date = tip.select(min("date"), max("date")).first()
min_date, max_date

# COMMAND ----------

# DBTITLE 1,Filter Tip for cutoff date 
tip_filter = tip.where( tip.date < cutoffdate)

# COMMAND ----------

# DBTITLE 1,Exclude tip not in business restaurants
joinType="left_semi"

#Semi join using DF
tip_restaurants = tip_filter.join(business_restaurants,["business_id"],joinType)

# COMMAND ----------

print((tip.count(), len(tip.columns)))
print((tip_filter.count(), len(tip_filter.columns)))
print((tip_restaurants.count(), len(tip_restaurants.columns)))

# COMMAND ----------

# DBTITLE 1,Aggregate results
tip_count = tip_restaurants.groupBy("business_id").agg(count("date").alias('tip_count'))
joinType="left_outer"
business_restaurants = business_restaurants.join(tip_count,["business_id"],joinType)

# COMMAND ----------

tip_count = tip_restaurants.groupBy("business_id").agg(max_("date").alias('last_tip_date'))
joinType="left_outer"
business_restaurants = business_restaurants.join(tip_count,["business_id"],joinType)

# COMMAND ----------

from pyspark.sql.functions import countDistinct

tip_count = tip_restaurants.groupBy("business_id").agg(countDistinct(("user_id")).alias('number_of_users_tip'))
joinType="left_outer"
business_restaurants = business_restaurants.join(tip_count,["business_id"],joinType)

# COMMAND ----------

tip_count = tip_restaurants.groupBy("business_id").agg(sum(("compliment_count")).alias('tip_compliments_count'))
joinType="left_outer"
business_restaurants = business_restaurants.join(tip_count,["business_id"],joinType)

# COMMAND ----------

from pyspark.sql.functions import collect_list
grouped_df = tip_restaurants.groupBy("business_id").agg(collect_list('text').alias("text_tip"))
joinType="left_outer"
business_restaurants = business_restaurants.join(grouped_df,["business_id"],joinType)

# COMMAND ----------

display(business_restaurants)

# COMMAND ----------

# DBTITLE 1,Review Schema
review_schema =  StructType([
          StructField("business_id", StringType(), True),
          StructField("cool", IntegerType(), True),
          StructField("date", TimestampType(), True),
          StructField("funny", IntegerType(), True),
          StructField("review_id", StringType(), True),
          StructField("stars", IntegerType(), True),
          StructField("text", StringType(), True),
          StructField("useful", IntegerType(), True),
          StructField("user_id", StringType(), True),
    ])

# COMMAND ----------

# DBTITLE 1,Read Review Json with Manual Schema
review = spark\
.read\
.format("json")\
.option("header","true")\
.schema(review_schema)\
.load(parsed_review_path)

# COMMAND ----------

review.printSchema()

# COMMAND ----------

min_date, max_date = review.select(min("date"), max("date")).first()
min_date, max_date

# COMMAND ----------

# DBTITLE 1,Filter review for cut off date
review_filter = review.where(review.date < cutoffdate)

# COMMAND ----------

# DBTITLE 1,Exclude review not in business restuarants
joinType="left_semi"

#Semi join using DF
review_restaurants = review_filter.join(business_restaurants,["business_id"],joinType)

# COMMAND ----------

print((review.count(), len(review.columns)))
print((review_filter.count(), len(review_filter.columns)))
print((review_restaurants.count(), len(review_restaurants.columns)))

# COMMAND ----------

# DBTITLE 1,Aggregate Results
grouped_df = review_restaurants.groupBy("business_id").agg(collect_list('text').alias("text_review"))
joinType="left_outer"
business_restaurants = business_restaurants.join(grouped_df,["business_id"],joinType)

# COMMAND ----------

review_count = review_restaurants.groupBy("business_id").agg(avg(("stars")).alias('review_avg_stars'))
joinType="left_outer"
business_restaurants = business_restaurants.join(review_count,["business_id"],joinType)

# COMMAND ----------

review_count = review_restaurants.groupBy("business_id").agg(countDistinct(("user_id")).alias('number_of_users_reviews'))
joinType="left_outer"
business_restaurants = business_restaurants.join(review_count,["business_id"],joinType)

# COMMAND ----------

review_count = review_restaurants.groupBy("business_id").agg(sum(("useful")).alias('review_useful_count'))
joinType="left_outer"
business_restaurants = business_restaurants.join(review_count,["business_id"],joinType)

# COMMAND ----------

review_count = review_restaurants.groupBy("business_id").agg(sum(("funny")).alias('review_funny_count'))
joinType="left_outer"
business_restaurants = business_restaurants.join(review_count,["business_id"],joinType)

# COMMAND ----------

review_count = review_restaurants.groupBy("business_id").agg(sum(("cool")).alias('review_cool_count'))
joinType="left_outer"
business_restaurants = business_restaurants.join(review_count,["business_id"],joinType)

# COMMAND ----------

review_count = tip_restaurants.groupBy("business_id").agg(max_("date").alias('last_review_date'))
joinType="left_outer"
business_restaurants = business_restaurants.join(review_count,["business_id"],joinType)

# COMMAND ----------

display(business_restaurants)

# COMMAND ----------

print((business_restaurants.count(), len(business_restaurants.columns)))

# COMMAND ----------

# DBTITLE 1,Read the COVID json
#

# COMMAND ----------

covid = spark.read.json(parsed_covid_path)

# COMMAND ----------

covid.printSchema()

# COMMAND ----------

print((covid.count(), len(covid.columns)))

# COMMAND ----------

joinType="left_outer"
business_restaurants = business_restaurants.join(covid,["business_id"],joinType)

# COMMAND ----------

print((business_restaurants.count(), len(business_restaurants.columns)))

# COMMAND ----------

display(business_restaurants)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save processed data for faster retreval later

# COMMAND ----------

#save data
business_restaurants.write.json("/dbfs/FileStore/business_restaurants.json")

# COMMAND ----------

#load saved data
bt_path = "/dbfs/FileStore/business_restaurants.json"
bt = spark\
.read\
.format("json")\
.option("header","true")\
.load(bt_path)


# COMMAND ----------

bt.display(2)

# COMMAND ----------

from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import col, lit, split

# COMMAND ----------

# rename target columns
bt = bt.select("*", col("delivery or takeout").alias("Target"))

#remove space and dots from column names and replace with underscore
NewColumns=(column.replace(' ', '_') for column in bt.columns)
bt = bt.toDF(*NewColumns)
NewColumns=(column.replace('.', '_') for column in bt.columns)
bt = bt.toDF(*NewColumns)

# COMMAND ----------

#create categories for state and city using string indexer

stringIndexer_state = StringIndexer(inputCol="state", outputCol="state_cat")
stringIndexer_state.setHandleInvalid("error")
model_state = stringIndexer_state.fit(bt)
model_state.setHandleInvalid("error")
td = model_state.transform(bt)

stringIndexer_city = StringIndexer(inputCol="city", outputCol="city_cat")
stringIndexer_city.setHandleInvalid("error")
model_city = stringIndexer_city.fit(td)
model_city.setHandleInvalid("error")
td = model_city.transform(td)

td.display()

# COMMAND ----------

#calculate number of hours the businesses open
td = td.withColumn('H_M_S',when(col('hours_Monday')==0,lit(0)).otherwise(split(td['hours_Monday'], '-').getItem(0))) 
td = td.withColumn('H_M_E',when(col('hours_Monday')==0,lit(0)).otherwise(split(td['hours_Monday'], '-').getItem(1)))
td = td.withColumn('H_M_S_H',when(col('H_M_S')==0,lit(0)).otherwise(split(td['H_M_S'], ':').getItem(0)).cast(IntegerType())) 
td = td.withColumn('H_M_E_H',when(col('H_M_E')==0,lit(0)).otherwise(split(td['H_M_E'], ':').getItem(0)).cast(IntegerType()))
td = td.withColumn('H_M_S_M',when(col('H_M_S')==0,lit(0)).otherwise(split(td['H_M_S'], ':').getItem(1)).cast(IntegerType())) 
td = td.withColumn('H_M_E_M',when(col('H_M_E')==0,lit(0)).otherwise(split(td['H_M_E'], ':').getItem(1)).cast(IntegerType()))
td = td.withColumn('Monday_DT',when(col('H_M_S_H')<12,lit(1)).when(col('H_M_S_H').between(12,18),lit(2)).otherwise(lit(3)).cast(IntegerType()))
td = td.withColumn('H_M_S_H',col('H_M_S_H')*60+col('H_M_S_M')) 
td = td.withColumn('H_M_E_H',col('H_M_E_H')*60+col('H_M_E_M'))

td = td.withColumn('H_M_S',when(col('hours_Tuesday')==0,lit(0)).otherwise(split(td['hours_Tuesday'], '-').getItem(0))) 
td = td.withColumn('H_M_E',when(col('hours_Tuesday')==0,lit(0)).otherwise(split(td['hours_Tuesday'], '-').getItem(1)))
td = td.withColumn('H_M_S_H',when(col('H_M_S')==0,lit(0)).otherwise(split(td['H_M_S'], ':').getItem(0)).cast(IntegerType())) 
td = td.withColumn('H_M_E_H',when(col('H_M_E')==0,lit(0)).otherwise(split(td['H_M_E'], ':').getItem(0)).cast(IntegerType()))
td = td.withColumn('H_M_S_M',when(col('H_M_S')==0,lit(0)).otherwise(split(td['H_M_S'], ':').getItem(1)).cast(IntegerType())) 
td = td.withColumn('H_M_E_M',when(col('H_M_E')==0,lit(0)).otherwise(split(td['H_M_E'], ':').getItem(1)).cast(IntegerType()))
td = td.withColumn('Tuesday_DT',when(col('H_M_S_H')<12,lit(1)).when(col('H_M_S_H').between(12,18),lit(2)).otherwise(lit(3)).cast(IntegerType()))
td = td.withColumn('H_M_S_H',col('H_M_S_H')*60+col('H_M_S_M')) 
td = td.withColumn('H_M_E_H',col('H_M_E_H')*60+col('H_M_E_M'))
td=td.na.fill(0)


td = td.withColumn('H_M_S',when(col('hours_Wednesday')==0,lit(0)).otherwise(split(td['hours_Wednesday'], '-').getItem(0))) 
td = td.withColumn('H_M_E',when(col('hours_Wednesday')==0,lit(0)).otherwise(split(td['hours_Wednesday'], '-').getItem(1)))
td = td.withColumn('H_M_S_H',when(col('H_M_S')==0,lit(0)).otherwise(split(td['H_M_S'], ':').getItem(0)).cast(IntegerType())) 
td = td.withColumn('H_M_E_H',when(col('H_M_E')==0,lit(0)).otherwise(split(td['H_M_E'], ':').getItem(0)).cast(IntegerType()))
td = td.withColumn('H_M_S_M',when(col('H_M_S')==0,lit(0)).otherwise(split(td['H_M_S'], ':').getItem(1)).cast(IntegerType())) 
td = td.withColumn('H_M_E_M',when(col('H_M_E')==0,lit(0)).otherwise(split(td['H_M_E'], ':').getItem(1)).cast(IntegerType()))
td = td.withColumn('Wednesday_DT',when(col('H_M_S_H')<12,lit(1)).when(col('H_M_S_H').between(12,18),lit(2)).otherwise(lit(3)).cast(IntegerType()))
td = td.withColumn('H_M_S_H',col('H_M_S_H')*60+col('H_M_S_M')) 
td = td.withColumn('H_M_E_H',col('H_M_E_H')*60+col('H_M_E_M'))
td=td.na.fill(0)

td = td.withColumn('H_M_S',when(col('hours_Thursday')==0,lit(0)).otherwise(split(td['hours_Thursday'], '-').getItem(0))) 
td = td.withColumn('H_M_E',when(col('hours_Thursday')==0,lit(0)).otherwise(split(td['hours_Thursday'], '-').getItem(1)))
td = td.withColumn('H_M_S_H',when(col('H_M_S')==0,lit(0)).otherwise(split(td['H_M_S'], ':').getItem(0)).cast(IntegerType())) 
td = td.withColumn('H_M_E_H',when(col('H_M_E')==0,lit(0)).otherwise(split(td['H_M_E'], ':').getItem(0)).cast(IntegerType()))
td = td.withColumn('H_M_S_M',when(col('H_M_S')==0,lit(0)).otherwise(split(td['H_M_S'], ':').getItem(1)).cast(IntegerType())) 
td = td.withColumn('H_M_E_M',when(col('H_M_E')==0,lit(0)).otherwise(split(td['H_M_E'], ':').getItem(1)).cast(IntegerType()))
td = td.withColumn('Thursday_DT',when(col('H_M_S_H')<12,lit(1)).when(col('H_M_S_H').between(12,18),lit(2)).otherwise(lit(3)).cast(IntegerType()))
td = td.withColumn('H_M_S_H',col('H_M_S_H')*60+col('H_M_S_M')) 
td = td.withColumn('H_M_E_H',col('H_M_E_H')*60+col('H_M_E_M'))
td=td.na.fill(0)

td = td.withColumn('H_M_S',when(col('hours_Friday')==0,lit(0)).otherwise(split(td['hours_Friday'], '-').getItem(0))) 
td = td.withColumn('H_M_E',when(col('hours_Friday')==0,lit(0)).otherwise(split(td['hours_Friday'], '-').getItem(1)))
td = td.withColumn('H_M_S_H',when(col('H_M_S')==0,lit(0)).otherwise(split(td['H_M_S'], ':').getItem(0)).cast(IntegerType())) 
td = td.withColumn('H_M_E_H',when(col('H_M_E')==0,lit(0)).otherwise(split(td['H_M_E'], ':').getItem(0)).cast(IntegerType()))
td = td.withColumn('H_M_S_M',when(col('H_M_S')==0,lit(0)).otherwise(split(td['H_M_S'], ':').getItem(1)).cast(IntegerType())) 
td = td.withColumn('H_M_E_M',when(col('H_M_E')==0,lit(0)).otherwise(split(td['H_M_E'], ':').getItem(1)).cast(IntegerType()))
td = td.withColumn('Friday_DT',when(col('H_M_S_H')<12,lit(1)).when(col('H_M_S_H').between(12,18),lit(2)).otherwise(lit(3)).cast(IntegerType()))
td = td.withColumn('H_M_S_H',col('H_M_S_H')*60+col('H_M_S_M')) 
td = td.withColumn('H_M_E_H',col('H_M_E_H')*60+col('H_M_E_M'))
td=td.na.fill(0)

td = td.withColumn('H_M_S',when(col('hours_Saturday')==0,lit(0)).otherwise(split(td['hours_Saturday'], '-').getItem(0))) 
td = td.withColumn('H_M_E',when(col('hours_Saturday')==0,lit(0)).otherwise(split(td['hours_Saturday'], '-').getItem(1)))
td = td.withColumn('H_M_S_H',when(col('H_M_S')==0,lit(0)).otherwise(split(td['H_M_S'], ':').getItem(0)).cast(IntegerType())) 
td = td.withColumn('H_M_E_H',when(col('H_M_E')==0,lit(0)).otherwise(split(td['H_M_E'], ':').getItem(0)).cast(IntegerType()))
td = td.withColumn('H_M_S_M',when(col('H_M_S')==0,lit(0)).otherwise(split(td['H_M_S'], ':').getItem(1)).cast(IntegerType())) 
td = td.withColumn('H_M_E_M',when(col('H_M_E')==0,lit(0)).otherwise(split(td['H_M_E'], ':').getItem(1)).cast(IntegerType()))
td = td.withColumn('Saturday_DT',when(col('H_M_S_H')<12,lit(1)).when(col('H_M_S_H').between(12,18),lit(2)).otherwise(lit(3)).cast(IntegerType()))
td = td.withColumn('H_M_S_H',col('H_M_S_H')*60+col('H_M_S_M')) 
td = td.withColumn('H_M_E_H',col('H_M_E_H')*60+col('H_M_E_M'))
td=td.na.fill(0)


td = td.withColumn('H_M_S',when(col('hours_Sunday')==0,lit(0)).otherwise(split(td['hours_Sunday'], '-').getItem(0))) 
td = td.withColumn('H_M_E',when(col('hours_Sunday')==0,lit(0)).otherwise(split(td['hours_Sunday'], '-').getItem(1)))
td = td.withColumn('H_M_S_H',when(col('H_M_S')==0,lit(0)).otherwise(split(td['H_M_S'], ':').getItem(0)).cast(IntegerType())) 
td = td.withColumn('H_M_E_H',when(col('H_M_E')==0,lit(0)).otherwise(split(td['H_M_E'], ':').getItem(0)).cast(IntegerType()))
td = td.withColumn('H_M_S_M',when(col('H_M_S')==0,lit(0)).otherwise(split(td['H_M_S'], ':').getItem(1)).cast(IntegerType())) 
td = td.withColumn('H_M_E_M',when(col('H_M_E')==0,lit(0)).otherwise(split(td['H_M_E'], ':').getItem(1)).cast(IntegerType()))
td = td.withColumn('Sunday_DT',when(col('H_M_S_H')<12,lit(1)).when(col('H_M_S_H').between(12,18),lit(2)).otherwise(lit(3)).cast(IntegerType()))
td = td.withColumn('H_M_S_H',col('H_M_S_H')*60+col('H_M_S_M')) 
td = td.withColumn('H_M_E_H',col('H_M_E_H')*60+col('H_M_E_M'))
td=td.na.fill(0)

# COMMAND ----------

#create a view to for final base table processing
td.createOrReplaceTempView("base_table")

# COMMAND ----------

#fill nan values with 0 and change bools to binary
#here we also create some new features using spark sql case statements

final_base_table = spark.sql("""select business_id,
case when AcceptsInsurance = True then 1 else 0 end as AcceptsInsurance,
case when AgesAllowed = '21plus' then 1 else 0 end as 21plus,
case when (AgesAllowed = 'allages') or (AgesAllowed is null) then 1 else 0 end as allages,
case when Alcohol = 'full_bar' then 1 else 0 end as Alcohol,
case when Alcohol = 'beer_and_wine' then 1 else 0 end as beer_and_wine,
case when BusinessAcceptsBitcoin = True then 1 else 0 end as BusinessAcceptsBitcoin,
case when BusinessAcceptsCreditCards = True then 1 else 0 end as BusinessAcceptsCreditCards,
case when ByAppointmentOnly = True then 1 else 0 end as ByAppointmentOnly,
case when Call_To_Action_enabled = True then 1 else 0 end as Call_To_Action_enabled,
case when Caters = True then 1 else 0 end as Caters,
case when Covid_Banner = False then 0 else 1 end as Covid_Banner,
case when DietaryRestrictions = True then 1 else 0 end as DietaryRestrictions,
case when DriveThru = True then 1 else 0 end as DriveThru,
case when GoodForDancing = True then 1 else 0 end as GoodForDancing,
case when GoodForKids = True then 1 else 0 end as GoodForKids,
case when Grubhub_enabled = True then 1 else 0 end as Grubhub_enabled,
case when HappyHour = True then 1 else 0 end as HappyHour,
case when NoiseLevel = 'average' then 1 else 0 end as average_noise,
case when NoiseLevel = 'loud' then 1 else 0 end as loud_noise,
case when NoiseLevel = 'quiet' then 1 else 0 end as quiet_noise,
case when NoiseLevel = 'very_loud' then 1 else 0 end as very_loud_noise,
case when Open24Hours = True then 1 else 0 end as Open24Hours,
case when OutdoorSeating = True then 1 else 0 end as OutdoorSeating,
case when Request_a_Quote_Enabled = True then 1 else 0 end as Request_a_Quote_Enabled,
case when RestaurantsAttire = 'formal' then 1 else 0 end as Attire_formal,
case when RestaurantsAttire = 'casual' then 1 else 0 end as Attire_casual,
case when RestaurantsAttire = 'dressy' then 1 else 0 end as Attire_dressy,
case when RestaurantsCounterService = True then 1 else 0 end as RestaurantsCounterService,
case when RestaurantsDelivery = True then 1 else 0 end as RestaurantsDelivery,
case when RestaurantsGoodForGroups = True then 1 else 0 end as RestaurantsGoodForGroups,
case when (RestaurantsPriceRange2 = 'null') or (RestaurantsPriceRange2 = 'None') then 0 else RestaurantsPriceRange2 end as RestaurantsPriceRange,
case when RestaurantsReservations = True then 1 else 0 end as RestaurantsReservations,
case when RestaurantsTableService = True then 1 else 0 end as RestaurantsTableService,
case when RestaurantsTakeOut = True then 1 else 0 end as RestaurantsTakeOut,
case when Temporary_Closed_Until = True then 1 else 0 end as Temporary_Closed_Until,
case when Virtual_Services_Offered = True then 1 else 0 end as Virtual_Services_Offered,
case when background_music = True then 1 else 0 end as background_music,
case when breakfast = True then 1 else 0 end as breakfast,
case when brunch = True then 1 else 0 end as brunch,
case when casual = True then 1 else 0 end as casual,
checkin_count,
state_cat,
case when classy = True then 1 else 0 end as classy,
case when dessert = True then 1 else 0 end as dessert,
case when dinner = True then 1 else 0 end as dinner,
case when dj = True then 1 else 0 end as dj,
case when garage = True then 1 else 0 end as garage,
Case when (monday = True) or (tuesday = True) or (wednesday = True) or (thursday = True) or (friday = True) then 1 else 0 end as Weekday,
Case when (sunday = True) or (saturday = True) then 1 else 0 end as Weekend,
case when Monday_DT is null then 0 else Monday_DT end as Monday_DT,
case when Tuesday_DT is null then 0 else Tuesday_DT end as Tuesday_DT,
case when Wednesday_DT is null then 0 else Wednesday_DT end as Wednesday_DT,
case when Thursday_DT is null then 0 else Thursday_DT end as Thursday_DT,
case when Friday_DT is null then 0 else Friday_DT end as Friday_DT,
case when Saturday_DT is null then 0 else Saturday_DT end as Saturday_DT,
case when Sunday_DT is null then 0 else Sunday_DT end as Sunday_DT,
case when (intimate=True) or  (romantic=True) then 1 else 0 end as intimate_romantic,
case when (trendy=True) or  (hipster=True) then 1 else 0 end as trendy_hipster,
case when touristy = True then 1 else 0 end as touristy,
case when upscale = True then 1 else 0 end as upscale,
case when is_open = True then 1 else 0 end as is_open,
case when jukebox = True then 1 else 0 end as jukebox,
case when karaoke = True then 1 else 0 end as karaoke,
last_checkin,
last_review_date,
last_tip_date,
case when latenight = True then 1 else 0 end as latenight,
case when live = True then 1 else 0 end as live,
case when lot = True then 1 else 0 end as lot_parking,
case when lunch = True then 1 else 0 end as lunch,
case when no_music = True then 1 else 0 end as no_music,
number_of_users_reviews,
number_of_users_tip,
review_avg_stars,
review_cool_count,
review_count,
review_funny_count,
review_useful_count,
stars,
tip_compliments_count,
tip_count,take
case when valet = True then 1 else 0 end as valet,
case when validated = True then 1 else 0 end as validated,
case when Target = True then 1 else 0 end as label
from base_table""")

# COMMAND ----------

#save the final base table for eaeier load next time
final_base_table.write.json("/dbfs/FileStore/final_base_table_v2.json")

# COMMAND ----------

# MAGIC %pip install spark_ml_utils

# COMMAND ----------

#read final base table
bt_path_f = "/dbfs/FileStore/final_base_table_v2.json"
btf = spark\
.read\
.format("json")\
.option("header","true")\
.load(bt_path_f)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Modeling Experiments

# COMMAND ----------

#load ML libraries
from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, GBTClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, CrossValidatorModel

import matplotlib.pyplot as plt
import numpy as np
import itertools
from sklearn.metrics import confusion_matrix

# COMMAND ----------

#Reference: https://runawayhorse001.github.io/LearningApacheSpark/classification.html
def plot_confusion_matrix(cm, classes,
                          normalize=False,
                          title='Confusion matrix',
                          cmap=plt.cm.Blues):
    """
    This function prints and plots the confusion matrix.
    Normalization can be applied by setting `normalize=True`.
    """
    if normalize:
        cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
        print("Normalized confusion matrix")
    else:
        print('Confusion matrix, without normalization')

    print(cm)

    plt.imshow(cm, interpolation='nearest', cmap=cmap)
    plt.title(title)
    plt.colorbar()
    tick_marks = np.arange(len(classes))
    plt.xticks(tick_marks, classes, rotation=45)
    plt.yticks(tick_marks, classes)

    fmt = '.2f' if normalize else 'd'
    thresh = cm.max() / 2.
    for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
        plt.text(j, i, format(cm[i, j], fmt),
                 horizontalalignment="center",
                 color="white" if cm[i, j] > thresh else "black")

    plt.tight_layout()
    plt.ylabel('True label')
    plt.xlabel('Predicted label')

# COMMAND ----------

#Select features and convert to vectors
assembler = VectorAssembler()\
.setInputCols (['21plus', 'AcceptsInsurance', 'Alcohol', 'BusinessAcceptsBitcoin', 'BusinessAcceptsCreditCards',\
 'ByAppointmentOnly', 'Call_To_Action_enabled', 'Caters', 'Covid_Banner', 'DietaryRestrictions', 'DriveThru',  'GoodForKids',\
  'HappyHour', 'Open24Hours', 'Request_a_Quote_Enabled', 'RestaurantsCounterService',\
 'RestaurantsGoodForGroups',  'RestaurantsReservations', 'RestaurantsTableService', 'RestaurantsTakeOut', \
 'Sunday_DT', 'Temporary_Closed_Until', 'Virtual_Services_Offered',  'Weekday', 'Weekend', 'allages',\
 'average_noise', 'background_music', 'beer_and_wine', 'breakfast', 'brunch',  'casual', 'checkin_count', 'classy', \
 'dessert', 'dinner', 'intimate_romantic', 'is_open',  \
 'latenight', 'loud_noise', 'lunch', 'number_of_users_reviews', 'number_of_users_tip', 'quiet_noise',\
 'review_avg_stars', 'review_cool_count', 'review_count', 'review_funny_count', 'review_useful_count', 'stars', \
 'tip_compliments_count', 'tip_count', 'touristy', 'trendy_hipster', 'upscale', 'validated', 'very_loud_noise'])\
.setOutputCol ("features")

assembler_df=assembler.transform(btf)
#assembler_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Reduced feature list after stepwise feature selecton

# COMMAND ----------

#Select features and convert to vectors
assembler_sel_f = VectorAssembler()\
.setInputCols (['upscale', 'lunch','dessert', 'dinner',  'Attire_casual', 'Attire_dressy',\
                'Attire_formal','number_of_users_reviews','checkin_count','review_avg_stars', 'review_cool_count',\
                'review_count', 'review_funny_count', 'review_useful_count', 'stars', 'state_cat','tip_compliments_count', 'tip_count'])\
.setOutputCol ("features")
assembler_sel_f_df = assembler_sel_f.transform(btf)

# COMMAND ----------

#train test split
train, test = assembler_sel_f_df.randomSplit([0.85, 0.15], seed = 42)
print("Training Dataset Count: " + str(train.count()))
print("Test Dataset Count: " + str(test.count()))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Logistic Regression

# COMMAND ----------

lr = LogisticRegression()
lr = LogisticRegression(featuresCol = 'features', labelCol = 'label', maxIter=1000)
lrModel = lr.fit(train)
predictions_lr = lrModel.transform(test)
predictions_train_lr = lrModel.transform(train)

summary = lrModel.summary
print("Accuracy: ", summary.accuracy)

evaluator = BinaryClassificationEvaluator()
print("Train Area Under ROC: " + str(evaluator.evaluate(predictions_train_lr, {evaluator.metricName: "areaUnderROC"})))
print("Test Area Under ROC: " + str(evaluator.evaluate(predictions_lr, {evaluator.metricName: "areaUnderROC"})))

# COMMAND ----------

lr = LogisticRegression()
lr = LogisticRegression(featuresCol = 'features', labelCol = 'label', maxIter=1000)

cv = CrossValidator(estimator=lr, evaluator=evaluator, numFolds=10)
# Run cross validations.  This can take about 6 minutes since it is training over 20 trees!
cvModel = cv.fit(train)
predictions_train = lrModel.transform(train)
predictions = cvModel.transform(test)

#evaluator.evaluate(predictions)
evaluator = BinaryClassificationEvaluator()
print("Train Area Under ROC: " + str(evaluator.evaluate(predictions_train, {evaluator.metricName: "areaUnderROC"})))
print("Test Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))

# COMMAND ----------

gbt = GBTClassifier(maxIter=10)
gbtModel = gbt.fit(train)
prediction_train = gbtModel.transform(train)
predictions = gbtModel.transform(test)
predictions.select('label', 'features', 'rawPrediction', 'prediction', 'probability').show(10)

evaluator = BinaryClassificationEvaluator()
print("Train Area Under ROC: " + str(evaluator.evaluate(prediction_train, {evaluator.metricName: "areaUnderROC"})))

evaluator = BinaryClassificationEvaluator()
print("Test Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Accuracy = %g" % (accuracy))

# COMMAND ----------

gbtModel

# COMMAND ----------

y_true = predictions.select("label")
y_true = y_true.toPandas()

y_pred = predictions.select("prediction")
print(y_pred)
y_pred = y_pred.toPandas()

cnf_matrix = confusion_matrix(y_true, y_pred)
cnf_matrix

# COMMAND ----------

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
paramGrid = (ParamGridBuilder()
             .addGrid(gbt.maxDepth, [2, 4, 6])
             .addGrid(gbt.maxBins, [10, 20])
             .addGrid(gbt.maxIter, [10, 20])
             .build())
cv = CrossValidator(estimator=gbt, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=10)

cvModel = cv.fit(train)
predictions = cvModel.transform(test)
evaluator.evaluate(predictions)

# COMMAND ----------

cvModel.bestModel

# COMMAND ----------

#Define model and pipeline
lr = LogisticRegression()
lr = LogisticRegression(featuresCol = 'features', labelCol = 'label', maxIter=1000)
pipeline = Pipeline().setStages([lr])
#params = ParamGridBuilder().addGrid(lr.regParam, [0.01, 0.5]).addGrid(lr.elasticNetParam, [0.5, 1.0]).addGrid(lr.maxIter, [500]).build()
params = ParamGridBuilder().addGrid(lr.maxIter, [1000]).build()

#A RegressionEvaluator() needs two columns: prediction and label  
evaluator = BinaryClassificationEvaluator()\
  .setMetricName("areaUnderROC")\
  .setRawPredictionCol("prediction")\
  .setLabelCol("label")

#Validation
cv = CrossValidator()\
  .setEstimator(pipeline)\
  .setEvaluator(evaluator)\
  .setEstimatorParamMaps(params)\
  .setNumFolds(10)

#Fitting the model
lrmodel_tun1 = cv.fit(train)

# COMMAND ----------

#reference: https://github.com/xinyongtian/py_spark_ml_utils
import spark_ml_utils.LogisticRegressionModel_util as lu

lu.feature_importance(lrm_model=lrModel, trainDF=train, trainFeatures='features', nonzero_only=True ).head(20)

# COMMAND ----------

lrmodel_tun1.bestModel

# COMMAND ----------

from pyspark.mllib.evaluation import BinaryClassificationMetrics

out = lrmodel_tun1.transform(assembler_sel_f_df)\
  .select("prediction", "label")\
  .rdd.map(lambda x: (float(x[0]), float(x[1])))

metrics = BinaryClassificationMetrics(out)

print(metrics.areaUnderPR)
print("AUC: " + str(metrics.areaUnderROC))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Random Forest

# COMMAND ----------

#set up the string indexer for target
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(assembler_df)

#set up the string indexer for features
featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(assembler_df)

#initialize Random Forest model
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)

# Train model with Training Data
rfModel = rf.fit(trainingData)

# Make predictions on test data using the Transformer.transform() method.
predictions = rfModel.transform(testData)

# Make predictions on train data using the Transformer.transform() method.
predictions_train = rfModel.transform(trainingData)

# View model's predictions and probabilities of each prediction class
selected = predictions.select("label", "prediction", "probability")

#display(selected)

# COMMAND ----------

#evaluate performance
rfs = rfModel.summary
rfs.areaUnderROC

evaluator = BinaryClassificationEvaluator()
print("Accuracy: ",rfs.accuracy)
print("Train Area Under ROC: " + str(evaluator.evaluate(predictions_train, {evaluator.metricName: "areaUnderROC"})))
print("Test Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))

# COMMAND ----------

#check the confusion matrix
y_true = predictions.select("label")
y_true = y_true.toPandas()

y_pred = predictions.select("prediction")
y_pred = y_pred.toPandas()

cnf_matrix = confusion_matrix(y_true, y_pred)
cnf_matrix

# COMMAND ----------

# Create ParamGrid for Cross Validation
rf_cv = RandomForestClassifier(labelCol="label", featuresCol="features")

paramGrid = (ParamGridBuilder()
             .addGrid(rf_cv.maxDepth, [2, 4, 6])
             .addGrid(rf_cv.maxBins, [20, 60])
             .addGrid(rf_cv.numTrees, [20, 40])
             .build())

# Create 5-fold CrossValidator
cv = CrossValidator(estimator=rf_cv, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=10)

# Run cross validations.  This can take about 8-15 minutes since it is training over 20 trees!
# 15-30 minutes for 40 trees
cvModel = cv.fit(trainingData)

# COMMAND ----------

import statistics
print("Mean AUC: ", statistics.mean(cvModel.avgMetrics)

# COMMAND ----------

# Use test set here so we can measure the accuracy of our model on new data
predictions = cvModel.transform(testData)

# cvModel uses the best model found from the Cross Validation
# Evaluate best model
evaluator.evaluate(predictions)

# View Best model's predictions and probabilities of each prediction class
selected = predictions.select("label", "prediction", "probability")
#display(selected)

bestModel = cvModel.bestModel

# Generate predictions for entire dataset
finalPredictions = bestModel.transform(testData)
finalPredictions_train = bestModel.transform(trainingData)

evaluator.evaluate(finalPredictions)

rf_sum = bestModel.summary

print("Accuracy: ", rf_sum.accuracy) 
print("Train Area Under ROC: " + str(evaluator.evaluate(finalPredictions_train, {evaluator.metricName: "areaUnderROC"})))
print("Test Area Under ROC: " + str(evaluator.evaluate(finalPredictions, {evaluator.metricName: "areaUnderROC"})))

# COMMAND ----------


