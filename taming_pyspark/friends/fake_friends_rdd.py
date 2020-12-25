from pyspark import SparkConf, SparkContext
from taming_pyspark.config import BaseConfig
from taming_pyspark.utils.rdd_line_parser import csv_line_to_len_2_tuple as csv_parser


def avg_friends_by_age(sc: SparkContext, parser):
    """
    Reads a csv with index, first_name_, age, and number of friends
    as fields. Functions reads and prints out two maps
    First map/dict has age as a key and a tuple
        tuple (total_friends, number of people with key age)

    Second map/dict has age has the key and average number of friends
    as the value
    :param sc: Spark context application
    :param parser: rdd line parser
    :return:
    """
    data_file = f'{BaseConfig.DATA_FOLDER}/{BaseConfig.FRIENDS_DATASET}/fakefriends.csv'
    lines = sc.textFile(data_file)
    rdd = lines.map(parser)
    totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    averagesByAge = totalsByAge.mapValues(lambda x: round(x[0] / x[1]))
    results = averagesByAge.collect()

    for result in results:
        print(result)


def spark_runner():
    conf = SparkConf().setMaster("local").setAppName("FakFriends")
    sc = SparkContext(conf=conf)
    avg_friends_by_age(sc=sc, parser=csv_parser)


if __name__ == '__main__':
    spark_runner()
