from pyspark.sql import *
from pyspark.sql.functions import monotonically_increasing_id, expr, col, when, to_date
from pyspark.sql.types import IntegerType

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Misc Transformation") \
        .master("local[3]") \
        .getOrCreate()

    data_list = [("Name1", "23", "9", "2001"),
                 ("Name2", "31", "10", "6"),  # 2006
                 ("Name3", "1", "1", "91"),  # 1991
                 ("Name1", "23", "9", "2001")]

    raw_df = spark.createDataFrame(data_list).toDF("Name", "Day", "Month", "Year").repartition(2)
    raw_df.printSchema()

    new_df = raw_df.withColumn("id", monotonically_increasing_id())
    new_df.show()

    # problem is that year became 2006.0, 1991.0
    df2 = new_df.withColumn("year", expr("""
    case when year < 21 then year + 2000
    when year < 100 then year + 1900
    else year
    end"""))
    df2.show()

    # to fix the issue we can use inline cast or change the schema
    # using inline cast
    df3 = new_df.withColumn("year", expr("""
    case when year < 21 then cast(year as int)+ 2000
    when year < 100 then cast(year as int) + 1900
    else year
    end"""))
    df3.show()

    # changing the schema
    df4 = new_df.withColumn("year", expr("""
        case when year < 21 then cast(year as int)+ 2000
        when year < 100 then cast(year as int) + 1900
        else year
        end""").cast(IntegerType()))
    df4.show()
    df4.printSchema()

    df5 = new_df.withColumn("day", col("day").cast(IntegerType())) \
        .withColumn("month", col("month").cast(IntegerType())) \
        .withColumn("year", col("year").cast(IntegerType()))

    df6 = df5.withColumn("year", expr("""
    case when year < 21 then year + 2000
    when year < 100 then year + 1900
    else year
    end"""))
    df6.show()

    df7 = df5.withColumn("year", \
                         when(col("year") < 21, col("year") + 2000) \
                         .when(col("year") < 100, col("year") + 1900) \
                         .otherwise(col("year")))
    df7.show()

    df8 = df7.withColumn("dob", expr("to_date(concat(day,'/',month,'/',year), 'd/M/y')"))
    df8.show()

    df9 = df7.withColumn("dob", to_date(expr("concat(day,'/',month,'/',year)"), 'd/M/y')) \
        .drop("day", "month", "year") \
        .dropDuplicates(["name","dob"]) \
        .sort(expr("dob desc"))
    df9.show()
