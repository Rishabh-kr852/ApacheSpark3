from pyspark.sql import *

from logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("Hello spark") \
        .getOrCreate()

    logger = Log4j(spark)

    survey_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("sql.shuffle.partitions", 2) \
        .load("data/sample.csv")

    partitioned_df = survey_df.repartition(2)
    input("enter a number")

