from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType
from taming_pyspark.utils.spark_runner import spark_session_runner
from taming_pyspark.utils.regular_expressions import normalize_words
from taming_pyspark.config import BaseConfig


def count_word_occurrence_regex_rdd(spark: SparkSession):
    """
    Takes a text and shows unique words and its frequency
    Uses regex to remove special characters
    :param spark: Spark Session Class
    :return: None
    """
    data_file = f'{BaseConfig.DATA_FOLDER}/{BaseConfig.WORD_COUNT}/Book.txt'
    line_schema: StructType = StructType().add(StructField("line", StringType(), nullable=True))
    lines_df: DataFrame = spark.read.csv(path=data_file, schema=line_schema, header=False)
    words_row = Row("word")
    # Note: row returns text, count
    words_df = lines_df.rdd.map(list).flatMap(lambda row: row[0].split()).flatMap(normalize_words).map(words_row).toDF()
    words_count = words_df.groupBy("word").count().sort("count", ascending=False)
    print(words_count.show())


if __name__ == "__main__":
    spark_session_runner(count_word_occurrence_regex_rdd, app_name="Count_Word_RegEx_DF")

