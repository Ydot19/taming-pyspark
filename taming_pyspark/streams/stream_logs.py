from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract
from pyspark.sql.types import IntegerType
from pyspark.streaming import StreamingContext
from taming_pyspark.utils.spark_runner import spark_session_runner
import time


def stream_log_files(spark: SparkSession, folder: str):
    """
    Takes a log directory, detects and reads when new log files are created
    The data for the log files are in the data folder. Add the log files one at a
    time to see how spark stream captures this information
    :param spark: Spark session object
    :param folder: log directory to monitor
    :return:
    """
    # return a data schema with 'value' as the column header
    access_lines = spark.readStream.text(path=folder)

    # Parse out the common log format to a DataFrame
    content_size_exp = r'\s(\d+)$'
    status_exp = r'\s(\d{3})\s'
    general_exp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
    time_exp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
    host_exp = r'(^\S+\.[\S+\.]+\S+)\s'

    logs_df = access_lines.select(
                regexp_extract('value', host_exp, 1).alias('host'),
                regexp_extract('value', time_exp, 1).alias('timestamp'),
                regexp_extract('value', general_exp, 1).alias('method'),
                regexp_extract('value', general_exp, 2).alias('endpoint'),
                regexp_extract('value', general_exp, 3).alias('protocol'),
                regexp_extract('value', status_exp, 1).cast(IntegerType()).alias('status'),
                regexp_extract('value', content_size_exp, 1).cast(IntegerType()).alias('content_size')
            )

    # get count of each status
    status_count_df = logs_df.groupby('status').count()

    # kick off streaming query to the console
    query = status_count_df.writeStream.outputMode('complete').format('console').queryName('counts').start()

    # terminate after 80 seconds
    query.awaitTermination(timeout=80)
    # stop query afterwards
    query.stop()


def create_directory(folder: str):
    import pathlib
    pathlib.Path(folder).mkdir(parents=True, exist_ok=True)


def delete_directory(folder: str):
    from shutil import rmtree
    rmtree(folder)


if __name__ == '__main__':
    options = dict({
        'folder': 'logs'
    })
    create_directory(folder=options.get('folder'))

    spark_session_runner(runner=stream_log_files, app_name='StructuredStreaming', **options)

    delete_directory(folder=options.get('folder'))
