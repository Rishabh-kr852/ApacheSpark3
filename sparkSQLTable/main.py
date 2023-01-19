from pyspark.sql import SparkSession

from logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("SparkSQLTable") \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/")

    spark.sql(
        "CREATE DATABASE IF NOT EXISTS AIRLINE_DB")  # making sure that AIRLINE_DB database is already there in spark warehouse
    spark.catalog.setCurrentDatabase(
        "AIRLINE_DB")  # default name of directory is DEFAULT, however we are naming it as "AIRLINE_DB"

    flightTimeParquetDF.write \
        .format("csv") \
        .mode("overwrite") \
        .bucketBy(5, "OP_CARRIER", "ORIGIN") \
        .sortBy("OP_CARRIER", "ORIGIN") \
        .saveAsTable("flight_data_tbl")

    logger.info(spark.catalog.listTables("AIRLINE_DB"))
