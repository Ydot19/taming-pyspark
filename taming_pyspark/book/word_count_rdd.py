from pyspark import SparkContext
from taming_pyspark.utils.spark_runner import spark_context_runner
from taming_pyspark.config import BaseConfig


def count_word_occurrence(sc: SparkContext):
    """
    Takes a text and shows unique words and its frequency
    :param sc: Spark instance
    :return: None
    """
    data_file = f'{BaseConfig.DATA_FOLDER}/{BaseConfig.WORD_COUNT}/Book.txt'
    lines = sc.textFile(data_file)
    all_words = lines.flatMap(lambda sentence: sentence.split())
    wordCount = all_words.countByValue()

    for word, count in wordCount.items():
        cleaned_word = word.encode('ascii', 'ignore')
        if cleaned_word:
            print(f'{cleaned_word.decode("ascii")}: {count}')


if __name__ == '__main__':
    spark_context_runner(count_word_occurrence, app_name="Word_Frequency")
