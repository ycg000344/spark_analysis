class Config(object):

    def __init__(self, db_type, ip, port, db, user, passwd):
        """
        :param db_type:
            * mysql
            * oracle
            * postgresql
        :param ip:
        :param port:
        :param db:
        :param user:
        :param passwd:
        """
        self.__db_type__ = db_type
        self.__ip__ = ip
        self.__port__ = port
        self.__db__ = db
        self.__user__ = user
        self.__passwd__ = passwd
        self.__url_format__ = url_format.get(db_type)

    def url(self):
        return self.__url_format__.format(self.__ip__, self.__port__, self.__db__)

    def properties(self):
        return {'user': self.__user__,
                'password': self.__passwd__,
                'driver': driver.get(self.__db_type__),
                }


url_format = {
    'mysql': 'jdbc:mysql://{}:{}/{}?useUnicode=true&characterEncoding=UTF-8',
    'oracle': 'jdbc:oracle:thin:@{}:{}:{}',
    'postgresql': 'jdbc:postgresql://{}:{}/{}?useUnicode=true&characterEncoding=UTF-8',
}

driver = {
    'mysql': 'com.mysql.cj.jdbc.Driver',
    'oracle': 'oracle.jdbc.driver.OracleDriver',
    'postgresql': 'org.postgresql.Driver',
}

