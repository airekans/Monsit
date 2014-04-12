import mysql.connector
import datetime


_DB_CONFIG = {'host': '127.0.0.1',
              'user': 'monsit',
              'password': 'monsitpass',
              'database': 'monsit'}
_POOL_SIZE = mysql.connector.pooling.CNX_POOL_MAXSIZE


class TableNames(object):
    hosts_tbl = 'hosts'

    @staticmethod
    def get_host_table_name(host_id, field):
        return '%s_%d' % (field, host_id)


def _create_global_tables(cnx):
    cursor = cnx.cursor()
    host_table_stmt = (
        'CREATE TABLE IF NOT EXISTS `%s` ('
        '  `id` int(11) NOT NULL AUTO_INCREMENT,'
        '  `name` varchar(100) NOT NULL,'
        '  PRIMARY KEY (`id`)'
        ') ENGINE=InnoDB'
    ) % TableNames.hosts_tbl
    cursor.execute(host_table_stmt)
    cnx.commit()


def _get_connection():
    try:
        cnx = mysql.connector.connect(pool_size=_POOL_SIZE,
                                      **_DB_CONFIG)
        return cnx
    except mysql.connector.Error as err:
        print err
        return None


def init():
    cnx = _get_connection()
    if cnx is None:
        return False

    _create_global_tables(cnx)
    cnx.close()


class DBConnection(object):

    class ConnError(Exception):
        pass

    def __init__(self):
        cnx = _get_connection()
        if cnx is None:
            raise DBConnection.ConnError

        self.__cnx = cnx

    def close(self):
        self.__cnx.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__cnx.close()

    @staticmethod
    def get_host_table_name(host_id, field):
        return '%s_%d' % (field, host_id)

    def get_all_hosts(self):
        cursor = self.__cnx.cursor()
        host_select_stmt = 'SELECT id, name FROM %s' % TableNames.hosts_tbl
        cursor.execute(host_select_stmt)
        return [host_info for host_info in cursor]

    def get_host_info(self, host_name):
        cursor = self.__cnx.cursor()
        host_select_stmt = (
            "SELECT id, name FROM %s WHERE name='%s'"
        ) % (TableNames.hosts_tbl, host_name)
        cursor.execute(host_select_stmt)

        for host_info in cursor:
            return host_info

        return None

    def insert_new_host(self, host_name):
        cursor = self.__cnx.cursor()
        host_insert_stmt = (
            "INSERT INTO %s SET"
            " name='%s'"
        ) % (TableNames.hosts_tbl, host_name)
        cursor.execute(host_insert_stmt)
        self.__cnx.commit()
        return self.get_host_info(host_name)

    def create_host_tables(self, host_id):
        cursor = self.__cnx.cursor()
        cpu_table_stmt = (
            'CREATE TABLE IF NOT EXISTS `%s` ('
            '  `id` int(11) NOT NULL AUTO_INCREMENT,'
            '  `name` varchar(14) NOT NULL,'
            '  `user_count` int(11) NOT NULL,'
            '  `nice_count` int(11) NOT NULL,'
            '  `sys_count` int(11) NOT NULL,'
            '  `idle_count` int(11) NOT NULL,'
            '  `iowait_count` int(11) NOT NULL,'
            '  `total_count` int(11) NOT NULL,'
            '  `datetime` datetime NOT NULL,'
            '  PRIMARY KEY (`id`)'
            ') ENGINE=InnoDB'
        ) % DBConnection.get_host_table_name(host_id, 'cpu')
        cursor.execute(cpu_table_stmt)

        net_table_stmt = (
            'CREATE TABLE IF NOT EXISTS `%s` ('
            '  `id` int(11) NOT NULL AUTO_INCREMENT,'
            '  `name` varchar(14) NOT NULL,'
            '  `ip` varchar(20) NOT NULL,'
            '  `datetime` datetime NOT NULL,'
            '  `recv_byte` bigint NOT NULL,'
            '  `send_byte` bigint NOT NULL,'
            '  PRIMARY KEY (`id`)'
            ') ENGINE=InnoDB'
        ) % DBConnection.get_host_table_name(host_id, 'net')
        cursor.execute(net_table_stmt)

        self.__cnx.commit()

    def insert_host_info(self, host_info, host_id):
        cursor = self.__cnx.cursor()

        report_time = datetime.datetime.fromtimestamp(host_info.datetime)
        report_time = report_time.strftime("%Y-%m-%d %H:%M:%S")
        cpu_tbl_name = self.get_host_table_name(host_id, 'cpu')
        net_tbl_name = self.get_host_table_name(host_id, 'net')

        cpu_insert_stmt = (
            "INSERT INTO %s SET"
            " name='%s',"
            " user_count=%d,"
            " nice_count=%d,"
            " sys_count=%d,"
            " idle_count=%d,"
            " iowait_count=%d,"
            " total_count=%d,"
            " datetime='%s'"
        )
        for cpu_info in host_info.cpu_infos:
            stmt = cpu_insert_stmt % (cpu_tbl_name,
                                      cpu_info.name,
                                      cpu_info.user_count,
                                      cpu_info.nice_count,
                                      cpu_info.sys_count,
                                      cpu_info.idle_count,
                                      cpu_info.iowait_count,
                                      cpu_info.total_count,
                                      report_time)
            try:
                cursor.execute(stmt)
            except mysql.connector.Error as err:
                print err.msg
                self.__cnx.rollback()
                return False

        net_insert_stmt = (
            "INSERT INTO %s SET"
            " name='%s',"
            " ip='%s',"
            " datetime='%s',"
            " recv_byte=%d,"
            " send_byte=%d"
        )
        for net_info in host_info.net_infos:
            stmt = net_insert_stmt % (net_tbl_name,
                                      net_info.name,
                                      net_info.ip,
                                      report_time,
                                      net_info.recv_byte,
                                      net_info.send_byte)
            try:
                cursor.execute(stmt)
            except mysql.connector.Error as err:
                print err.msg
                self.__cnx.rollback()
                return False

        self.__cnx.commit()
        return True


