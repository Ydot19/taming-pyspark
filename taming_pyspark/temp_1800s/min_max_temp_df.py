from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from taming_pyspark.config import BaseConfig


def show_min_or_max_temps(spark: SparkSession, show_max: bool = False):
    """
    Shows min weather temperature observed for each weather station by default
    Setting show_max, will instead show the max temperature recorded
    Data:
        weather_station_id, date_identifier, observation_type, measurement, other... data

    date_identifier = f'{4-digit-year}{2-digit-month}-(2-digit-date-integer)'
    observation_type =
        TMIN - minimum temperature
        TMAX - maximum temperature
        PRCP - precipitation %
    :param show_max: Boolean to should the max temp recorded at a station instead of the min
    :param spark: SparkSession class instance
    :return: None
    """
    data_file = f'{BaseConfig.DATA_FOLDER}/{BaseConfig.TEMP_1800S}/1800.csv'
    schema = StructType() \
        .add(StructField('weather_station_identifier', StringType(), False)) \
        .add(StructField('date_identifier', StringType(), False)) \
        .add(StructField('observation_type', StringType(), False)) \
        .add(StructField('measurement', IntegerType(), False))
    # Dataframe
    df = spark.read.csv(path=data_file, schema=schema)

    if not show_max:
        df.filter(df["observation_type"] == "TMIN") \
            .groupby("weather_station_identifier") \
            .min("measurement") \
            .show()
    else:
        df.filter(df["observation_type"] == "TMAX") \
            .groupby("weather_station_identifier") \
            .max("measurement") \
            .show()


def run_min_temp():
    """
    Runs spark job in a function callable to reduce global variable namespace pollution
    :return:
    """
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("1800s_Min_or_Max_Temp") \
        .getOrCreate()

    show_min_or_max_temps(spark=spark, show_max=True)


if __name__ == '__main__':
    run_min_temp()
