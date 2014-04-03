import mysql.connector

_MYSQL_SERVER_ADDR = '127.0.0.1'
_MYSQL_USER = 'monsit'
_MYSQL_PASSWORD = 'monsitpass'
_MYSQL_DB = 'monsit'


def init():
    global _cnx, _cursor

    try:
        _cnx = mysql.connector.connect(user=_MYSQL_USER, password=_MYSQL_PASSWORD,
                                       host=_MYSQL_SERVER_ADDR, database=_MYSQL_DB)
    except mysql.connector.Error as err:
        print err
        return False

    _cursor = _cnx.cursor()

