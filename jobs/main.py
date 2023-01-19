from pyspark.sql import *

from logger import Log4j


def count_by_country(survey_df):
    return survey_df.filter("Age < 40") \
        .select("Age", "Gender", "Country", "state") \
        .groupBy("Country") \
        .count()


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
    count_df = count_by_country(partitioned_df)
    count_df.show()
    input("enter a number")
