from pyspark import SparkConf, SparkContext


def show_min_temps(sc: SparkContext):
    """
    Shows min weather temperature observed for each weather station
    Data:
        weather_station_id, date_identifier, observation_type, measurement, other... data

    date_identifier = f'{4-digit-year}{2-digit-month}-(2-digit-date-integer)'
    observation_type =
        TMIN - minimum temperature
        TMAX - maximum temperature
        PRCP - precipitation %
    :param sc: SparkSession class instance
    :return: None
    """