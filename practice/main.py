import sys

from pyspark.sql import *

from logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Practice") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)


    surveyDF = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/sample.csv")

    surveyDF.createOrReplaceTempView("survey_tbl")
    countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age<40 group by Country")
    countDF.show()