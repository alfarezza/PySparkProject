from pyspark.sql import *
from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("HelloSpark")\
        .master("local[3]")\
        .getOrCreate()

    logger = Log4J(spark)

    logger.info("Starting Hello Spark")


    logger.info("Finished Hello Spark")

    # spark.stop()