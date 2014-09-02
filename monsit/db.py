import datetime
import mysql.connector


_DB_CONFIG = {'host': '127.0.0.1',
              'user': 'monsit',
              'password': 'monsitpass',
              'database': 'monsit'}
_POOL_SIZE = mysql.connector.pooling.CNX_POOL_MAXSIZE


class TableNames(object):
    hosts_tbl = 'hosts'

    @staticmethod
    def get_host_info_table_name(host_id):
        return 'hostinfo_%d' % host_id

    @staticmethod
    def get_stat_table_name(host_id, field_id):
        return 'field_%d_%d' % (host_id, field_id)


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


class ValueType(object):
    Invalid = 0
    Int = 1
    Double = 2
    String = 3

    _mysql_type_str = ['invalid', 'bigint',
                       'double', 'varchar(25)']
    _mysql_value_fmt_str = ['invalid', '%d',
                            '%f', "'%s'"]

    @staticmethod
    def get_mysql_type_str(value_type):
        return ValueType._mysql_type_str[value_type]

    @staticmethod
    def get_mysql_value_fmt_str(value_type):
        return ValueType._mysql_value_fmt_str[value_type]


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

    def commit(self):
        self.__cnx.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__cnx.close()

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

    def get_field_id(self, cursor, stat_name, host_id):
        select_stmt = (
            "SELECT field_id FROM %s"
            " WHERE stat_name='%s'"
        ) % (TableNames.get_host_info_table_name(host_id), stat_name)
        cursor.execute(select_stmt)

        field_id = None
        for res in cursor:
            print 'res type', type(res), len(res)
            field_id = res[0]

        assert field_id is not None
        return field_id

    def get_field_type(self, cursor, host_tbl_name, field_id):
        select_stmt = (
            "SELECT y_value_type FROM %s"
            " WHERE field_id = %d"
        ) % (host_tbl_name, field_id)
        cursor.execute(select_stmt)

        field_type = None
        for res in cursor:
            field_type = res[0]

        assert field_type is not None
        return field_type

    def insert_builtin_fields(self, cursor, host_id):
        builtin_field_configs = [
            ('cpu_total', 'Total Usage', ValueType.Int, '%'),  # 1
            ('network_recv', 'Recv', ValueType.Int, 'KB'),  # 2
            ('network_send', 'Send', ValueType.Int, 'KB'),  # 3
            ('virtual_mem', 'Physical Memory', ValueType.Int, '%'),  # 4
            ('swap_mem', 'Swap Memory', ValueType.Int, '%'),  # 5
            ('disk_io_write', 'Write', ValueType.Int, 'KB'),  # 6
            ('disk_io_read', 'Read', ValueType.Int, 'KB')  # 7
        ]

        hostinfo_tbl_name = TableNames.get_host_info_table_name(host_id)
        insert_stmt_template = (
            "INSERT INTO %s SET"
            " stat_name='%s',"
            " chart_name='%s',"
            " y_value_type=%d,"
            " y_unit='%s'"
        )
        create_stmt_template = (
            'CREATE TABLE IF NOT EXISTS `%s` ('
            '  `id` int(11) NOT NULL AUTO_INCREMENT,'
            '  `series` varchar(25) NOT NULL,'
            '  `y_value` %s NOT NULL,'
            '  `datetime` datetime NOT NULL,'
            '  PRIMARY KEY (`id`)'
            ') ENGINE=InnoDB'
        )
        for config in builtin_field_configs:
            stat_name, chart_name, y_value_type, y_unit = config
            insert_stmt = insert_stmt_template % (
                hostinfo_tbl_name,
                stat_name, chart_name, y_value_type, y_unit
            )
            cursor.execute(insert_stmt)

            field_id = self.get_field_id(cursor, stat_name, host_id)
            stat_tbl_name = TableNames.get_stat_table_name(host_id, field_id)
            create_stmt = create_stmt_template % (
                stat_tbl_name, ValueType.get_mysql_type_str(y_value_type)
            )
            cursor.execute(create_stmt)

    def insert_new_host(self, host_name):
        cursor = self.__cnx.cursor()
        host_insert_stmt = (
            "INSERT INTO %s SET"
            " name='%s'"
        ) % (TableNames.hosts_tbl, host_name)
        cursor.execute(host_insert_stmt)

        host_id, _ = self.get_host_info(host_name)

        create_host_tbl_stmt = (
            'CREATE TABLE IF NOT EXISTS `%s` ('
            '  `field_id` int(11) NOT NULL AUTO_INCREMENT,'
            '  `stat_name` varchar(25) NOT NULL,'
            '  `chart_name` varchar(25) NOT NULL,'
            '  `y_value_type` int(11) NOT NULL,'
            '  `y_unit` varchar(15) NOT NULL,'
            '  PRIMARY KEY (`field_id`)'
            ') ENGINE=InnoDB'
        ) % TableNames.get_host_info_table_name(host_id)
        cursor.execute(create_host_tbl_stmt)

        self.insert_builtin_fields(cursor, host_id)

        self.__cnx.commit()
        return self.get_host_info(host_name)

    def insert_stat(self, stat_req, report_time):
        host_id = stat_req.host_id
        host_tbl_name = TableNames.get_host_info_table_name(host_id)

        cursor = self.__cnx.cursor()
        insert_stmt_template = (
            "INSERT INTO %s SET"
            "  series='%s',"
            "  y_value=%s,"
            "  datetime='%s'"
        )

        for stat in stat_req.stat:
            field_id = stat.id
            field_type = self.get_field_type(cursor, host_tbl_name, field_id)

            stat_tbl_name = TableNames.get_stat_table_name(host_id, field_id)
            for y_value in stat.y_axis_value:
                if field_type == ValueType.Int:
                    stat_value = y_value.num_value
                elif field_type == ValueType.Double:
                    stat_value = y_value.double_value
                elif field_type == ValueType.String:
                    stat_value = y_value.str_value
                else:
                    assert False, 'unrecognized value type %d' % field_type

                insert_stmt = insert_stmt_template % (
                    stat_tbl_name, y_value.name,
                    ValueType.get_mysql_value_fmt_str(field_type) % stat_value,
                    report_time
                )
                cursor.execute(insert_stmt)

    def get_stat_infos(self, cursor, host_id):
        select_stmt = "SELECT * FROM %s" % TableNames.get_host_info_table_name(host_id)
        cursor.execute(select_stmt)

        infos = {}
        for res in cursor:
            field_id = res[0]
            infos[field_id] = {
                'stat_name': res[1],
                'chart_name': res[2],
                'y_value_type': res[3],
                'y_unit': res[4]
            }

        return infos

    _DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

    def get_host_stats(self, host_id, stat_ids):
        LAST_NUM_MIN = 30

        cursor = self.__cnx.cursor()
        stmt_template = ('SELECT * FROM %s' +
                         (' WHERE DATE_SUB(NOW(),INTERVAL %d MINUTE) <= datetime' % LAST_NUM_MIN) +
                         ' ORDER BY datetime ASC')
        stat_infos = self.get_stat_infos(cursor, host_id)
        stats = {}
        for stat_id in stat_ids:
            if stat_id not in stat_infos:
                print 'invalid stat_id', stat_id
                continue

            stat_tbl_name = TableNames.get_stat_table_name(host_id, stat_id)
            this_stat = {}

            stmt = stmt_template % stat_tbl_name
            cursor.execute(stmt)

            for res in cursor:
                series = res[1]
                y_value = res[2]
                this_date = res[3]
                if series not in this_stat:
                    this_stat[series] = {}

                this_stat[series][this_date.strftime(DBConnection._DATE_FORMAT)] = y_value

            stats[stat_id] = {'unit': stat_infos[stat_id]['y_unit'],
                              'data': this_stat}

        #print stats
        return stats

    def get_updated_stats(self, host_id, field_id, last_date):
        try:
            last_datetime = datetime.datetime.strptime(last_date,
                                                       DBConnection._DATE_FORMAT)
        except ValueError, e:
            print 'date parsing error:', e
            return None

        cursor = self.__cnx.cursor()
        stmt_template = ("SELECT * FROM %s"
                         " WHERE datetime > '%s'"
                         " ORDER BY datetime ASC")
        stat_infos = self.get_stat_infos(cursor, host_id)
        if field_id not in stat_infos:
            print 'field_id not in stat_infos', field_id
            return None

        stat_tbl_name = TableNames.get_stat_table_name(host_id, field_id)
        this_stat = {}

        stmt = stmt_template % (stat_tbl_name, last_datetime)
        cursor.execute(stmt)

        for res in cursor:
            series = res[1]
            y_value = res[2]
            this_date = res[3]
            if series not in this_stat:
                this_stat[series] = {}

            this_stat[series][this_date.strftime(DBConnection._DATE_FORMAT)] = y_value

        #print this_stat
        return this_stat
