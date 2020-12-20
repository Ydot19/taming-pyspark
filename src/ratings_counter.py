from pyspark import SparkConf, SparkContext
from config import BaseConfig
from collections import OrderedDict


def read_ratings(sc: SparkContext):
    """
    Reads ratings and prints to screen
    :param sc: reads the rating for the data folder
    :return: None
    """
    data_file = f'{BaseConfig.DATA_FOLDER}/{BaseConfig.MOVIE_LENS_FOLDERS}/u.data'
    data: list = sc.textFile(data_file).collect()

    print(data)
    # # result = ratings.countByValue()
    # #
    # # sorted_results = OrderedDict(sorted(result.items()))
    # #
    # # for key, value in sorted_results:
    # #     print(f'{key}: {value}')


if __name__ == '__main__':
    conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
    sc = SparkContext(conf=conf)

    read_ratings(sc=sc)
