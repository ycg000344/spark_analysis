from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, DataFrame


def __spark_instance__(app_name='defaultAppName'):
    spark_conf = SparkConf() \
        .setAppName(app_name) \
        .set("spark.ui.showConsoleProgress", "false")
    sc = SparkContext(conf=spark_conf)
    sc.setLogLevel("ERROR")
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    return sc, spark


class Analysis:
    def __init__(self, app_name='defaultAppName'):
        self.__sc__, self.__spark__ = __spark_instance__(app_name)

    def start(self):
        try:
            self.__run__()
        finally:
            self.__sc__.stop()
            self.__spark__.stop()

    def __run__(self):
        pass

    def __read_from_csv__(self, path) -> DataFrame:
        return self.__spark__.read.csv(path, header=True)

    def __save_to_csv__(self, df: DataFrame, path, mode='overwrite'):
        print('target csv: {}'.format(path))
        df.repartition(1).write.csv(path, mode, header=True)

    def __save_to_json__(self, df: DataFrame, path, mode='overwrite'):
        print('target json: {}'.format(path))
        df.repartition(1).write.json(path, mode)
