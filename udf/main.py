import re

from pyspark.sql import *
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import StringType

from logger import Log4j

# manipulation of column values using regular expression
# def parse_gender(gender):
#     female_pattern = r"^f$|f.m|w.m"
#     male_pattern = r"^m$|ma|ml"
#     if re.search(female_pattern, gender.lower()):
#         return "Female"
#     elif re.search(male_pattern, gender.lower()):
#         return "Male"
#     return "Unknown"

# changing the column entries without regular expression
def parse_gender(gender):
    if "female" == gender.lower() or "f" == gender.lower():
        return "Female"
    elif "male" == gender.lower() or "m" == gender.lower():
        return "Male"
    return "Unknown"

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("UDF") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    survey_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/survey.csv")

    survey_df.show(10)

    # column transformation
    parse_gender_udf = udf(parse_gender, StringType())
    survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
    survey_df2.show(10)

    # SQL expression
    spark.udf.register("parse_gender_udf", parse_gender, StringType())
    logger.info("Catalog Entry: ")
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    survey_df3.show(10)
