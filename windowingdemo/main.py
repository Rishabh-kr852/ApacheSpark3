from pyspark.sql import *
from pyspark.sql import functions as f

from logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Windowing") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)

    summary_df = spark.read.load("data/summary.parquet")

    running_total_window = Window.partitionBy("Country") \
        .orderBy("WeekNumber") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    summary_df.withColumn("RunningTotal", f.sum("InvoiceValue").over(running_total_window)).show()
