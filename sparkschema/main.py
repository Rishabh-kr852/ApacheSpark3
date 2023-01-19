from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType

from logger import Log4j

from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Spark Schema") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)

    # for csv file types
    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    # for json file types
    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
              ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
              WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

    flightTimeCsvDF = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(flightSchemaStruct) \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "M/d/y") \
        .load("data/flight*.csv")

    flightTimeCsvDF.show(5)
    logger.info(flightTimeCsvDF.schema.simpleString())

    flightTimeJsonDF = spark.read \
        .format("json") \
        .schema(flightSchemaDDL) \
        .option("dateFormat", "M/d/y") \
        .load(r"data/flight*.json")

    flightTimeCsvDF.show(5)
    logger.info(flightTimeJsonDF.schema.simpleString())

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load(r"data/flight*.parquet")

    flightTimeCsvDF.show(5)
    logger.info(flightTimeJsonDF.schema.simpleString())
