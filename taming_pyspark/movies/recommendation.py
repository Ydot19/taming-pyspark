import sys
# import time
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from taming_pyspark.utils.spark_runner import spark_session_runner
from taming_pyspark.config import BaseConfig


class RecommendationSystem:

    def __init__(self, spark: SparkSession, data_path: str, movie_name_data_path: str):
        self.__data_df: DataFrame = self.spark_read(
            spark=spark,
            file_path=data_path,
            **dict({
                "schema": self.data_schema(),
                "delimiter": "\t"
            })
        )
        self.__movie_df: DataFrame = self.spark_read(
            spark=spark,
            file_path=movie_name_data_path,
            **dict({
                "schema": self.movie_schema(),
                "delimiter": "|",
                "charset": "ISO-8859-1"
            })
        )

        self.__sc: SparkSession = spark

    @staticmethod
    def spark_read(spark: SparkSession, file_path: str, **kwargs) -> DataFrame:
        custom_schema = kwargs.get("schema")
        delimiter = kwargs.get("delimiter")
        charset = kwargs.get("charset", "utf-8")
        return spark.read.csv(path=file_path, schema=custom_schema, sep=delimiter, encoding=charset)

    @staticmethod
    def movie_schema():
        return StructType([
            StructField(name="movie_id", dataType=IntegerType(), nullable=True),
            StructField(name="movie_title", dataType=StringType(), nullable=True)
        ])

    @staticmethod
    def data_schema():
        return StructType([
            StructField(name="user_id", dataType=IntegerType(), nullable=True),
            StructField(name="movie_id", dataType=IntegerType(), nullable=True),
            StructField(name="rating", dataType=IntegerType(), nullable=True),
            StructField(name="timestamp", dataType=LongType(), nullable=True)
        ])

    @staticmethod
    def compute_similarity(data: DataFrame):
        pair_scores: DataFrame = data \
            .withColumn("xx", func.col("rating_1") * func.col("rating_1")) \
            .withColumn("yy", func.col("rating_2") * func.col("rating_2")) \
            .withColumn("xy", func.col("rating_1") * func.col("rating_2"))

        """
        similarity = ( xy ) / ( sqrt( xx ) * sqrt( yy ) ) 
        """
        calculate_similarity = pair_scores \
            .groupby("movie_id_1", "movie_id_2") \
            .agg(
                func.sum(func.col("xy")).alias("numerator"),
                (func.sqrt(func.sum("xx")) * func.sqrt(func.sum("yy"))).alias("denominator"),
                func.count("xy").alias("pairing_occurrences")
            )

        ret: DataFrame = calculate_similarity \
            .withColumn("score",
                        func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator"))
                        .otherwise(0)
                        ).select("movie_id_1", "movie_id_2", "score", "pairing_occurrences")
        return ret

    # noinspection SqlNoDataSourceInspection
    def main(self):
        self.__data_df.createTempView("Ratings")
        movie_pairs_df = self.__sc.sql(
            "SELECT r1.movie_id as movie_id_1, r1.rating as rating_1, r2.movie_id as movie_id_2, r2.rating as rating_2 "
            "FROM  Ratings r1 JOIN Ratings r2 on r1.user_id == r2.user_id WHERE r1.movie_id > r2.movie_id")

        movie_pair_similarities = self.compute_similarity(movie_pairs_df).cache()

        if len(sys.argv) > 1:
            threshold_score = 0.97
            threshold_occurrences = 50
            movie_id = int(sys.argv[1])
            # filter on criteria
            filter_results_df = movie_pair_similarities.filter(
                ((func.col("movie_id_1") == movie_id) | (func.col("movie_id_2") == movie_id)) &
                ((func.col("score") > threshold_score) & (func.col("pairing_occurrences") > threshold_occurrences))
            )

            # Sort on score
            results = filter_results_df.sort("score", ascending=False).take(10)
            print(f"\nTop 10 Similar Movies To Recommend for "
                  f"{self.lookup_df(self.__movie_df, movie_id, 'movie_id', 'movie_title')}", end="\n\n")

            for result in results:
                similar_movie_id = result.movie_id_1

                if similar_movie_id == movie_id:
                    similar_movie_id = result.movie_id_2

                print(f"{self.lookup_df(self.__movie_df, similar_movie_id, 'movie_id', 'movie_title')}"
                      f"\n\t\tscore: {round(result.score, 4)}"
                      f"\t\tviewed-together: {result.pairing_occurrences}", end="\n")

    @staticmethod
    def lookup_df(df: DataFrame, value, search_column: str, ret_column: str):
        """
        Filter a dataframe for the first occurrence of value and return this value
        :param df: Dataframe
        :param value: value to search
        :param search_column: Column to search
        :param ret_column: Column value to return from the search
        :return: String of the movie
        """
        return df.filter(func.col(search_column) == value) \
            .select(ret_column) \
            .collect()[0][0]


if __name__ == '__main__':
    options = dict({
        'data_path': f'{BaseConfig.DATA_FOLDER}/{BaseConfig.MOVIE_LENS_FOLDERS}/u.data',
        'movie_name_data_path': f'{BaseConfig.DATA_FOLDER}/{BaseConfig.MOVIE_LENS_FOLDERS}/u.item'
    })

    session: RecommendationSystem = spark_session_runner(runner=RecommendationSystem,
                                                         app_name="Recommendations",
                                                         **options)
    print(session.main())
    # time module used to view spark ui
    # command in the terminal
    # export PYSPARK_PYTHON=$HOME/.cache/pypoetry/virtualenvs/{env_name}/bin/python; spark-submit taming_pyspark/movies/recommendation.py 50
    # time.sleep(1000)
