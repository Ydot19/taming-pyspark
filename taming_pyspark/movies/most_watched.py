import codecs
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import udf, col, count, first
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
    movie_index_file = f'{BaseConfig.DATA_FOLDER}/{BaseConfig.MOVIE_LENS_FOLDERS}/u.item'
    # broadcast file to the executor nodes to reference
    movie_broadcast = spark.sparkContext.broadcast(load_movie_names(movie_index_file))

    # below is the logic
    custom_schema = StructType([
        StructField("user_id", IntegerType(), nullable=False),
        StructField("movie_id", IntegerType(), nullable=False),
        StructField("rating", IntegerType(), nullable=False),
        StructField("timestamp", IntegerType(), nullable=False)
    ])

    df = spark.read.csv(path=data_file, schema=custom_schema, sep="\t", header=False)

    df = df.withColumn("movie_title",
                       lookup_movie_name(movie_broadcast)(col("movie_id"))
                       )

    results = df.groupby("movie_id").agg(count("movie_id").alias("count"), first("movie_title")
                                         .alias("movie_title")).sort("count", ascending=False)

    print(results.show(5))


def lookup_movie_name(mapping):
    """
    Takes a column with movie id and looks up the names from the
    :param mapping: Lookup dictionary of movie id to movie title
    :return: pyspark udf of the name
    """
    def id_to_name(movie_id):
        """
        udf function that helps return the movie name
        :param movie_id:
        :return: str
        """
        return mapping.value.get(movie_id)
    return udf(id_to_name)


def load_movie_names(file_path: str) -> dict[int, str]:
    """
    Uses the codec module to read the movie name based on the movie id
    :param file_path: str to the file path in the virtual environment
    :return: movie_names as a dictionary
    """
    movie_names = dict()
    with codecs.open(filename=file_path, mode="r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]

    return movie_names


if __name__ == "__main__":
    spark_session_runner(runner=most_watched, app_name="Most_Watch_Movies_By_ID")
