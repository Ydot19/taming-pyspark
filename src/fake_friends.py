from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import round, mean, sum
from config import BaseConfig


def avg_friends_by_age(spark: SparkSession):
    """
    Reads a csv with index, first_name_, age, and number of friends
    as fields. Functions reads and prints out two maps
    First map/dict has age as a key and a tuple
        tuple (total_friends, number of people with key age)

    Second map/dict has age has the key and average number of friends
    as the value
    :param spark: SparkContext object
    :return: None
    """
    data_file = f'{BaseConfig.DATA_FOLDER}/{BaseConfig.FRIENDS_DATASET}/fakefriends.csv'
    schema = StructType([
        StructField("index", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("age", IntegerType(), nullable=False),
        StructField("numberOfFriends", IntegerType(), nullable=False)
    ])
    df = spark.read.csv(path=data_file, schema=schema, enforceSchema=True)
    # display both using dataframes
    df.groupby("age") \
        .agg(
            sum("numberOfFriends").alias("Total_Friends"),
            round(mean("numberOfFriends")).cast('integer').alias("Avg_Number_Of_Friends")
        ) \
        .show()


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .appName("Fake Friends") \
        .getOrCreate()

    avg_friends_by_age(spark)
