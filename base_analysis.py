from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, DataFrame

from constants import Config


def __spark_instance__(app_name='defaultAppName'):
    spark_conf = SparkConf() \
        .setAppName(app_name) \
        .set("spark.ui.showConsoleProgress", "false")
    sc = SparkContext(conf=spark_conf)
    sc.setLogLevel("ERROR")
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    return sc, spark


class Analysis:
    """

    """

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

    def __save_to_db__(self, df: DataFrame, table, mode, jdbc: Config = None):
        """
        :param jdbc:
        :param df:
        :param table:
        :param mode:
            * `append`: Append contents of this :class:`DataFrame` to existing data.
            * `overwrite`: Overwrite existing data.
            * `error` or `errorifexists`: Throw an exception if data already exists.
            * `ignore`: Silently ignore this operation if data already exists.
        :return:
        """
        print('target table: {}'.format(table))
        if jdbc is None:
            raise Exception("jdbc is none.")
        df.write.jdbc(url=jdbc.url(), table=table, mode=mode, properties=jdbc.properties())

    def __read_from_db__(self, table, jdbc: Config = None) -> DataFrame:
        if jdbc is None:
            raise Exception("jdbc is none.")
        return self.__spark__.read.jdbc(url=jdbc.url(), table=table, properties=jdbc.properties())

    def __read_from_csv__(self, path) -> DataFrame:
        return self.__spark__.read.csv(path)

    def __save_to_csv__(self, df: DataFrame, path, mode='overwrite'):
        print('target csv: {}'.format(path))
        df.repartition(1).write.csv(path, mode, header=True)

    def __save_to_json__(self, df: DataFrame, path, mode='overwrite'):
        print('target json: {}'.format(path))
        df.repartition(1).write.json(path, mode)
