from pyspark.sql import *
from pyspark.sql import functions as f

from logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Aggregate functions") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)

    invoice_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/invoices.csv")

    # simple aggregation - when it returns single row as the result
    invoice_df.select(f.count("*").alias("Count *"),
                      f.sum("Quantity").alias("TotalQuantity"),
                      f.avg("UnitPrice").alias("AvgPrices"),
                      f.countDistinct("InvoiceNo").alias("CountDistinct")).show()

    # using SQL expressions

    invoice_df.selectExpr(
        "count(1) as `count 1`",
        "count(StockCode) as `count field`",
        "avg(Quantity) as TotalQuantity",
        "avg(UnitPrice) as AvgPrice"
    ).show()

    invoice_df.createOrReplaceTempView("sales")
    summary_sql = spark.sql("""
          SELECT Country, InvoiceNo,
                sum(Quantity) as TotalQuantity,
                round(sum(Quantity*UnitPrice),2) as InvoiceValue
          FROM sales
          GROUP BY Country, InvoiceNo""")
    summary_sql.show()
    # or
    summary_sql = invoice_df \
        .groupby("Country", "InvoiceNo") \
        .agg(f.sum("Quantity").alias("TotalQuantity"),
             f.expr("round(sum(Quantity*UnitPrice),2) as InvoiceValue"))
    summary_sql.show()

    NumInvoices = f.countDistinct("InvoiceNo").alias("NumInvoices")
    TotalQuantity = f.sum("Quantity").alias("TotalQuantity")
    InvoiceValue = f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValueExpr")
    ex_summary = invoice_df \
        .withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"), "dd-MM-yyyy H.mm")) \
        .withColumn("WeekNumber", f.weekofyear(f.col("InvoiceDate"))) \
        .groupby("Country", "WeekNumber") \
        .agg(NumInvoices, TotalQuantity, InvoiceValue)

    ex_summary.coalesce(1) \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .save("output")
    ex_summary.sort("Country", "WeekNumber").show()
