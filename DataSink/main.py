from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("Data Sink Writer") \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/flight*.parquet")

    flightTimeParquetDF.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "dataSink/avro/") \
        .save()

    logger.info("Num of partitions before: " + str(flightTimeParquetDF.rdd.getNumPartitions()))
    flightTimeParquetDF.groupby(spark_partition_id()).count().show()

    partitionedDF = flightTimeParquetDF.repartition(5)

    logger.info("Num of partitions before: " + str(partitionedDF.rdd.getNumPartitions()))
    partitionedDF.groupby(spark_partition_id()).count().show()

    '''
    partitionedDF.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "dataSink/avro/") \
        .save()
    '''

    flightTimeParquetDF.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "dataSink/json/") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("maxRecordPerFile", 10000) \
        .save()
