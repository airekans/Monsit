import mysql.connector
import datetime

_MYSQL_SERVER_ADDR = '127.0.0.1'
_MYSQL_USER = 'monsit'
_MYSQL_PASSWORD = 'monsitpass'
_MYSQL_DB = 'monsit'

# TODO: change to use connection pool
_cnx = None
_cursor = None


def init():
    global _cnx, _cursor

    try:
        _cnx = mysql.connector.connect(user=_MYSQL_USER, password=_MYSQL_PASSWORD,
                                       host=_MYSQL_SERVER_ADDR, database=_MYSQL_DB)
    except mysql.connector.Error as err:
        print err
        return False

    _cursor = _cnx.cursor()


def get_host_table_name(ip, field):
    return field + '_' + ip.replace('.', '_')


def create_host_tables(ip):
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
    ) % get_host_table_name(ip, 'cpu')
    _cursor.execute(cpu_table_stmt)

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
    ) % get_host_table_name(ip, 'net')

    _cursor.execute(net_table_stmt)


def insert_host_info(host_info):
    if len(host_info.net_infos) == 0:
        return False

    host_ip = host_info.net_infos[0].ip
    report_time = datetime.datetime.fromtimestamp(host_info.datetime)
    report_time = report_time.strftime("%Y-%m-%d %H:%M:%S")
    cpu_tbl_name = get_host_table_name(host_ip, 'cpu')
    net_tbl_name = get_host_table_name(host_ip, 'net')

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
            _cursor.execute(stmt)
        except mysql.connector.Error as err:
            print(err.msg)
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
            _cursor.execute(stmt)
        except mysql.connector.Error as err:
            print(err.msg)
            return False

    return True


