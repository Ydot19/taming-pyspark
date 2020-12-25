from pyspark import SparkConf, SparkContext
from taming_pyspark.config import BaseConfig
from taming_pyspark.utils.spark_runner import spark_context_runner
from collections import OrderedDict


def read_ratings(sc: SparkContext):
    """
    Reads ratings and prints to screen
    :param sc: reads the rating for the data folder
    :return: None
    """
    data_file = f'{BaseConfig.DATA_FOLDER}/{BaseConfig.MOVIE_LENS_FOLDERS}/u.data'
    lines = sc.textFile(data_file)
    ratings = lines.map(lambda x: x.split()[2])
    result = ratings.countByValue()
    sorted_results = OrderedDict(sorted(result.items()))
    # spark results
    for key, value in sorted_results.items():
        print(f'{key}: {value}')


if __name__ == '__main__':
    spark_context_runner(read_ratings, app_name="RatingsHistogram")
