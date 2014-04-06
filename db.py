import mysql.connector

_MYSQL_SERVER_ADDR = '127.0.0.1'
_MYSQL_USER = 'monsit'
_MYSQL_PASSWORD = 'monsitpass'
_MYSQL_DB = 'monsit'

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
        '  `usage_rate` int(11) NOT NULL,'
        '  `date` date NOT NULL,'
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
        '  `recv_MB` int(11) NOT NULL,'
        '  `send_MB` int(11) NOT NULL,'
        '  PRIMARY KEY (`id`)'
        ') ENGINE=InnoDB'
    ) % get_host_table_name(ip, 'net')

    _cursor.execute(net_table_stmt)


def insert_host_info(host_info):
    if len(host_info.net_infos) == 0:
        return False

    host_ip = host_info.net_infos[0]
    cpu_tbl_name = get_host_table_name(host_ip, 'cpu')
    net_tbl_name = get_host_table_name(host_ip, 'net')


