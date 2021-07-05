# Databricks notebook source


def getloggingSession():
  logger=logging.getLogger()
  logger.setLevel(logging.INFO)
  
  #create console handler and set level to debug
  console_handler = logging.StreamHandler()
  console_handler.setLevel(logging.INFO)
  
  #create formatter
  formatter = logging.Formatter('%(asctime)s %(levelname)s: ' + 'Line - ' + '%(lineno)d' + ':' +  '%(message)s',"%Y%m%d%H%M%S")
  
  # add formatter to handler
  console_handler.setFormatter(formatter)
  
  #add handlers to loggers 
  logger.addHandler(console_handler)
  
  return logger

# COMMAND ----------

import os
import logging 
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col,udf,lit,lower,unix_timestamp
import time
import datetime
import re
from pyspark.rdd import PythonEvalType
from pyspark.sql.column import Column as col
from pyspark.sql.column import _to_java_column
from pyspark.sql.column import Column, _to_java_column, _to_seq, _create_column_from_literal
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType, DataType
from pyspark.sql import *
from pyspark.sql.dataframe import *
from pyspark.sql.types import *
#from pyspark.sql.column import _to_java_column
#from pyspark.sql.column import _to_seq
import pyspark.sql.functions as F
from pyspark.sql.functions import col,udf,lit,lower,unix_timestamp
from pyspark.sql.types import *


timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
#Getting spark session,application and logger

# COMMAND ----------



def getSparkSession():
  """
  creating spark session
  for Hello Fresh recipe
  ETL
  """
  try:
    logger=getloggingSession()
    spark=SparkSession.builder.appName('Hello fresh recipe').getOrCreate()
    logger.info("sparkSession.py  ->  Completed Successfully")
    return spark
  except Exception as e:
    logger.info(" Failed to Create Spark Session !!!")
    logger.exception("Error in getSparkSession function " + str(e))
    sys.exit(400)
    
spark=getSparkSession()
logger=getloggingSession()

# COMMAND ----------

class recipe_file_transfomed:
  def file_read(input_path):
    try:
      logger.info("Reading Input Json File ...!!!")
      input_file=spark.read.json(input_path)
      dfPersist=input_file.persist()
      logger.info("Input File Read Successfully")
      return dfPersist
    except Exception as e:
      logger.info("Failed to Read Input File!!!")
      logger.exception("Error in extract input file function " + str(e))
      sys.exit(400)
      
  def transform_input_file(dfPersist):
    """Transform Hello fresh recipe file
    input file is dataframe and output file is transformed dataframe"""
    try:
      logger.info("Performing the Transformation on the input file ...")
      recipe_only_beef_df=dfPersist.filter(lower(col('ingredients')).contains("beef"))
      recipe_total_average_time=recipe_only_beef_df.withColumn('cookTime', F.regexp_replace(F.col("cookTime"), "PT", ""))\
      .withColumn('prepTime', F.regexp_replace(F.col("prepTime"), "PT", ""))\
      .withColumn('cookhrs', F.split(col('cookTime'),'H').getItem(0))\
      .withColumn('cookmins', F.split(col('cookTime'),'H').getItem(1))\
      .withColumn('prephrs', F.split(col('prepTime'),'H').getItem(0))\
      .withColumn('prepmins', F.split(col('prepTime'),'H').getItem(1))\
      .withColumn('cook_mins1', F.when(col('cookhrs').contains('M'), col('cookhrs')).otherwise(None))\
      .withColumn('prep_mins1', F.when(col('prephrs').contains('M'), col('prephrs')).otherwise(None))\
      .withColumn('cookmins', F.coalesce(col('cookmins'),col('cook_mins1')))\
      .withColumn('prepmins', F.coalesce(col('prepmins'),col('prep_mins1')))\
      .withColumn('cookhrs', F.when(col('cookhrs').contains('M'), None)
            .otherwise(col('cookhrs'))).withColumn('prephrs', F.when(col('prephrs').contains('M'),
                                                                   None).otherwise(col('prephrs')))\
      .drop('cook_mins1').drop('prep_mins1')\
      .withColumn('cook_hrs_to_mins',(col('cookhrs').cast(IntegerType()) * 60))\
      .withColumn('prep_hrs_to_mins', (col('prephrs').cast(IntegerType()) * 60))\
      .withColumn('cookmins', F.split(col('cookmins'),'M').getItem(0).cast(IntegerType()))\
      .withColumn('prepmins', F.split(col('prepmins'),'M').getItem(0).cast(IntegerType())).fillna(0)\
      .withColumn('total_cook_mins', col('cookmins') + col('cook_hrs_to_mins'))\
      .withColumn('total_prep_mins', col('prepmins') + col('prep_hrs_to_mins'))\
      .drop('cookhrs').drop('cookmins').drop('prephrs').drop('prepmins').drop('cook_hrs_to_mins').drop('prep_hrs_to_mins')\
      .withColumn('dificulty_level',F.when((col("total_cook_mins") + col("total_prep_mins") >= 60),F.lit("HARD"))\
                  .otherwise(F.when((col("total_cook_mins") + col("total_prep_mins") >= 30),F.lit("MEDIUM"))\
                             .otherwise(F.when((col("total_cook_mins") + col("total_prep_mins") <30) & (col("total_cook_mins") + col("total_prep_mins") >0),F.lit("EASY"))\
                                        .otherwise(F.lit("UNKNOWN")))))\
      .groupBy('dificulty_level').agg((F.avg(col("total_cook_mins") + col("total_prep_mins"))).alias('avg_total_cooking_time'))
      logger.info("Successfully Transfomed the dataframe")
      return recipe_total_average_time
    except Exception as e:
      logger.info("Failed to transform Input File!!!")
      logger.exception("Error in transform_input_file function " + str(e))
      sys.exit(400)
    
  def load_recipe_data(input_dataframe,output_path):
    """
    Write transformed recipe
    data frame to output CSV folder
    :param input dataframe and output
    file path
    """
    try:
      logger.info(" Loading the transformed recipe data to external folder ...")
      input_dataframe.write.mode("overwrite").csv(output_path)
    except Exception as e:
      logger.info("Failed to load transformed dataframe!!!")
      logger.exception("Error in load_input_file function " + str(e))
      sys.exit(400)
  


# COMMAND ----------

def main():
  """
  Instancetiating recipe_file_transfomed object
  and calling the defined method from main
  programme
  """
  input_file_path="in/recipes.json"
  output_file_path="out/recipeoutput/"
  dataframe=recipe_file_transfomed.file_read(input_file_path)
  recipe_total_average_time=recipe_file_transfomed.transform_input_file(dataframe)
  recipe_file_transfomed.load_recipe_data(recipe_total_average_time,output_file_path)

# COMMAND ----------

if __name__ == "__main__":
  """
  Running pyspark job from main programme
  """
  try:
    logger.info("Hello Fresh recipe process job started...")
    main()
  except Exception as e:
    logger.info("Job Failed to process input file")
    logger.exception("Error in main function " + str(e))
    sys.exit(400)
