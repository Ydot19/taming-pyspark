from pyspark import SparkContext
from taming_pyspark.utils.spark_runner import spark_context_runner
from taming_pyspark.config import BaseConfig


def spending_rdd(sc: SparkContext):
    """
    Analyzes total spend of consumers by id.
    Data layout:
        customer_id,    item_id,    spend

    customer_id is an integer
    item_id is an integer
    spend is a float
    :param sc: Spark Context
    :return: None
    """
    data_file = f'{BaseConfig.DATA_FOLDER}/{BaseConfig.CONSUMER_SPENDING}/customer_orders.csv'
    data_rdd = sc.textFile(data_file)
    data_delimited_rdd = data_rdd.map(lambda x: x.split(',')).map(lambda x: (x[0], float(x[2])))
    consumer_spending = data_delimited_rdd.reduceByKey(lambda a, b: round(a + b, 2))

    for customer_id, spend in consumer_spending.collect():
        print("Customer ID: {0: <4} \tTotal Spend: ${1}".format(customer_id, spend), end="\n")


if __name__ == '__main__':
    spark_context_runner(spending_rdd, app_name="Customer_Spending")
