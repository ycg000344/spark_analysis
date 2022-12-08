from base_analysis import Analysis


class Demo(Analysis):

    def __run__(self):
        # self.__spark__.sql('select 1 from dual').createOrReplaceTempView('data')
        print('>>>>> start.')
        print('>>>>> finish.')


if __name__ == '__main__':
    Demo().start()
