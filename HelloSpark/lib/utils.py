# import configparser

# from pyspark import SparkConf


# def get_spark_app_config():
#     spark_conf = SparkConf()
#     config = configparser.ConfigParser()
#     config.read("spark.conf")

#     for (key, val) in config.items("SPARK_APP_CONFIGS"):
#         spark_conf.set(key, val)
#     return spark_conf

import configparser
from pyspark import SparkConf

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    print(config.sections())  # Add this line to print the sections

    if config.has_section("SPARK_APP_CONFIGS"):
        for (key, val) in config.items("SPARK_APP_CONFIGS"):
            spark_conf.set(key, val)
    else:
        raise configparser.NoSectionError("SPARK_APP_CONFIGS")
    
    return spark_conf