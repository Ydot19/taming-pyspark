from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from taming_pyspark.utils.spark_runner import spark_session_runner
from taming_pyspark.config import BaseConfig


def most_watched(spark: SparkSession):
    """
    Analyzes a data set with the following columns that are tab separate
    schema:
        user_id - Integer type
        movie_id - Integer type
        rating - Integer type between 1-5
        timestamp - Integer Type

    Movie is most popular based on its frequency in how many times it was rated
    :param spark: Spark Session
    :return: None
    """
    data_file = f'{BaseConfig.DATA_FOLDER}/{BaseConfig.MOVIE_LENS_FOLDERS}/u.data'
    custom_schema = StructType([
        StructField("user_id", IntegerType(), nullable=False),
        StructField("movie_id", IntegerType(), nullable=False),
        StructField("rating", IntegerType(), nullable=False),
        StructField("timestamp", IntegerType(), nullable=False)
    ])

    df = spark.read.csv(path=data_file, schema=custom_schema, sep="\t", header=False)
    results = df.groupby("movie_id").count().sort("count", ascending=False)
    print(results.show(5))


if __name__ == "__main__":
    spark_session_runner(runner=most_watched, app_name="Most_Watch_Movies_By_ID")
