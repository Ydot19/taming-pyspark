from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, DecimalType
from pyspark.sql.functions import sum
from taming_pyspark.utils.spark_runner import spark_session_runner
from taming_pyspark.config import BaseConfig


def spending_df(spark: SparkSession):
    """
    Analyzes total spend of consumers by id.
    Data layout:
        customer_id,    item_id,    spend

    customer_id is an integer
    item_id is an integer
    spend is a float
    :param spark: Spark session
    :return:
    """
    data_file = f'{BaseConfig.DATA_FOLDER}/{BaseConfig.CONSUMER_SPENDING}/customer_orders.csv'
    custom_schema = StructType([
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("item_id", IntegerType(), nullable=False),
        StructField("spend", DecimalType(8, 2), nullable=False)
    ])
    df = spark.read.csv(path=data_file, header=False, schema=custom_schema, sep=",")
    spend = df.groupby("customer_id").agg(sum("spend").cast(DecimalType(8, 2)).alias("total_spend")).sort("total_spend")
    print(spend.show())


if __name__ == "__main__":
    spark_session_runner(spending_df, app_name="Consumer_Spend_DF")
