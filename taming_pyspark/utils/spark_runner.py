from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext


def spark_session_runner(runner: callable, app_name: str, **options):
    spark = SparkSession \
        .builder \
        .master(f"{options.get('build_config', {}).get('master', 'local')}") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .appName(f'{app_name}') \
        .getOrCreate()

    if 'build_config' in options:
        options.pop('build_config')

    return runner(spark, **options)


def spark_context_runner(runner: callable, app_name: str, **kwargs):
    conf = SparkConf().setMaster("local").setAppName(f'{app_name}')
    sc = SparkContext(conf=conf)
    runner(sc=sc, **kwargs)
