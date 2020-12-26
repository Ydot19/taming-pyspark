from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import udf
from taming_pyspark.utils.spark_runner import spark_session_runner
from taming_pyspark.config import BaseConfig


def count_word_occurrence_df(spark: SparkSession):
    """
    Takes a text and shows unique words and its frequency
    :param spark: Spark Session Class
    :return:
    """
    data_file = f'{BaseConfig.DATA_FOLDER}/{BaseConfig.WORD_COUNT}/Book.txt'
    line_schema: StructType = StructType().add(StructField("line", StringType(), nullable=True))
    lines_df: DataFrame = spark.read.csv(path=data_file, schema=line_schema, header=False)
    words_row = Row("word")
    words_df = lines_df.rdd.map(list).flatMap(lambda row: row[0].split()).map(words_row).toDF()
    word_count = words_df.groupBy("word").count().sort("count", ascending=False)
    print(word_count.show())


if __name__ == '__main__':
    spark_session_runner(count_word_occurrence_df, app_name="Count_Word_DF")
