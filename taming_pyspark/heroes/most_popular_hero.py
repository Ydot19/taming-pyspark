import codecs
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf, col
from taming_pyspark.config import BaseConfig
from taming_pyspark.utils.spark_runner import spark_session_runner


def most_popular_hero(spark: SparkSession):
    """
    Takes a structure data file and determines the most popular hero based on how many
    uniquely different hours they have had an encounter with
    schema:
        hero_id: IntegerType - hero id
        peers: List of hero_id 's
    :param spark: Spark Session
    :return: None
    """
    data_file = f"{BaseConfig.DATA_FOLDER}/{BaseConfig.HEROES}/Marvel_Graph.txt"

    rdd = spark.sparkContext.textFile(name=data_file)\
        .map(lambda x: x.split()).map(df_creator)
    df = rdd.toDF(["hero_id", "peers"])
    peer_count_udf = udf(lambda x: len(x), returnType=IntegerType())
    peer_count_df = df.withColumn("number_of_peers", peer_count_udf(df['peers']))\
        .drop("peers")
    # Broad cast hero_id to name dictionary
    hero_id_name_file = f"{BaseConfig.DATA_FOLDER}/{BaseConfig.HEROES}/Marvel_Names.txt"
    hero_id_name: dict = read_hero_id_name_file(file=hero_id_name_file)
    bc = spark.sparkContext.broadcast(hero_id_name)
    hero_name_peer_count_df = peer_count_df.withColumn("hero_name", lookup_hero_name(bc)(col("hero_id")))\
        .sort("number_of_peers", ascending=True)

    hero_name_peer_count_df = hero_name_peer_count_df[["hero_id", "hero_name", "number_of_peers"]]

    print(hero_name_peer_count_df.show())


def df_creator(a: list[str]) -> tuple[int, list[str]]:
    """
    Returns a tuple of length 2
    :param a: list of strings integer values
    :return:
    """

    if len(a) == 1:
        return int(a[0]), []

    return int(a[0]), a[1:]


def read_hero_id_name_file(file: str) -> dict[int, str]:
    """
    Reads the hero to id name file path
    :param file: string
    :return: None
    """
    id_names = dict()

    with codecs.open(filename=file, mode="r", encoding='ISO-8859-1', errors='ignore') as f:
        for hero in f:
            details: list[str, str] = hero.split(maxsplit=1)
            id_names.update({int(details[0]): details[1].replace('"', '').replace('\n', '')})

    return id_names


def lookup_hero_name(mapping):
    """
    Takes a broadcast variable with dictionary of hero_id to their name
    :param mapping: Broadcast variable with id to hero name
    :return:
    """
    def id_to_name(hero_id):
        """
        udf function that helps return the hero name
        :param hero_id: Integer type that takes
        :return: str of hero name
        """
        return mapping.value.get(hero_id, "")

    return udf(id_to_name)


if __name__ == "__main__":
    spark_session_runner(most_popular_hero, app_name="MOST_POPULAR_HERO")
