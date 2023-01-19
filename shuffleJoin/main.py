from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

from logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Shuffle Join Demo") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)

    flight_time_df1 = spark.read.json("data/d1/")
    flight_time_df2 = spark.read.json("data/d2/")

    spark.conf.set("spark.sql.shuffle.partitions", 3)

    # broadcast is used when a large dataset is combined with small dataset 
    join_expr = flight_time_df1.id == flight_time_df2.id
    join_df = flight_time_df1.join(broadcast(flight_time_df2), join_expr, "inner")

    join_df.collect()
    input("press a key to stop...")
