from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext


def spark_session_runner(runner: callable, app_name: str, **kwargs):
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .appName(f'{app_name}') \
        .getOrCreate()

    runner(spark, **kwargs)


def spark_context_runner(runner: callable, app_name: str, **kwargs):
    conf = SparkConf().setMaster("local").setAppName(f'{app_name}')
    sc = SparkContext(conf=conf)
    runner(sc=sc, **kwargs)
