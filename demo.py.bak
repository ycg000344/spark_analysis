from base_analysis import Analysis
from constants import Config

DEV_CONSTANT = Config("db_type", "ip", "port", "db", "user", "passwd")


class Demo(Analysis):

    def __run__(self):
        # self.__spark__.sql('select 1 from dual').createOrReplaceTempView('data')
        pass


if __name__ == '__main__':
    Demo(DEV_CONSTANT).start()
