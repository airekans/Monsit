import datetime
import mysql.connector
import struct
import json


_DB_CONFIG = {'host': '127.0.0.1',
              'user': 'monsit',
              'password': 'monsitpass',
              'database': 'monsit'}
_POOL_SIZE = mysql.connector.pooling.CNX_POOL_MAXSIZE

_DEFAULT_DISPLAY_SETTING = [
    {
        "section_name": "CPU",
        "charts": [{"type": "stat", "id": 1, "name": "Usage"}]
    },
    {
        "section_name": "Network",
        "charts": [{"type": "stat", "id": 2, "name": "Recv"},
                   {"type": "stat", "id": 3, "name": "Send"}]
    },
    {
        "section_name": "Memory",
        "charts": [{"type": "stat", "id": 4, "name": "Physical Memory"},
                   {"type": "stat", "id": 5, "name": "Swap Memory"}]
    },
    {
        "section_name": "Disk",
        "charts": [{"type": "stat", "id": 6, "name": "Write"},
                   {"type": "stat", "id": 7, "name": "Read"}]
    }
]
_DEFAULT_DISPLAY_SETTING_STR = json.dumps(_DEFAULT_DISPLAY_SETTING,
                                          separators=(',', ':'))


class TableNames(object):
    hosts_tbl = 'hosts'
    display_setting_tbl = 'host_display_setting'

    @staticmethod
    def get_host_stat_info_table_name(host_id):
        return 'host_stat_info_%d' % host_id

    @staticmethod
    def get_stat_table_name(host_id, field_id):
        return 'field_%d_%d' % (host_id, field_id)

    @staticmethod
    def get_host_info_table_name(host_id):
        return 'hostinfo_%d' % host_id

    @staticmethod
    def get_host_alarm_table_name(host_id):
        return 'host_alarm_%d' % host_id


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

    host_display_table_stmt = (
        'CREATE TABLE IF NOT EXISTS `%s` ('
        '  `id` int(11) NOT NULL AUTO_INCREMENT,'
        '  `host_id` int(11) NOT NULL,'
        '  `display_json` varchar(60000) NOT NULL,'
        '  PRIMARY KEY (`id`)'
        ') ENGINE=InnoDB'
    ) % TableNames.display_setting_tbl
    cursor.execute(host_display_table_stmt)

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
    value_type_str = [(Int, 'int'), (Double, 'double'),
                      (String, 'string')]

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

    def get_stat_id(self, cursor, stat_name, host_id):
        select_stmt = (
            "SELECT field_id FROM %s"
            " WHERE stat_name='%s'"
        ) % (TableNames.get_host_stat_info_table_name(host_id), stat_name)
        cursor.execute(select_stmt)

        field_id = None
        for res in cursor:
            field_id = res[0]

        assert field_id is not None
        return field_id

    def get_stat_value_type(self, cursor, host_tbl_name, field_id):
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

    def insert_new_stat(self, host_id, stat_name,
                        chart_name, y_value_type, y_unit):
        cursor = self.__cnx.cursor()
        return self.insert_new_stat_with_cursor(cursor, host_id,
                                                stat_name,
                                                chart_name,
                                                y_value_type, y_unit)

    def insert_new_stat_with_cursor(self, cursor, host_id, stat_name,
                chart_name, y_value_type, y_unit):
        hostinfo_tbl_name = TableNames.get_host_stat_info_table_name(host_id)
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

        insert_stmt = insert_stmt_template % (
            hostinfo_tbl_name,
            stat_name, chart_name, y_value_type, y_unit
        )
        cursor.execute(insert_stmt)

        field_id = self.get_stat_id(cursor, stat_name, host_id)
        stat_tbl_name = TableNames.get_stat_table_name(host_id, field_id)
        create_stmt = create_stmt_template % (
            stat_tbl_name, ValueType.get_mysql_type_str(y_value_type)
        )
        cursor.execute(create_stmt)

        cursor.execute('SELECT LAST_INSERT_ID()')
        for res in cursor:
            return res[0]

        assert False

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

        for config in builtin_field_configs:
            stat_name, chart_name, y_value_type, y_unit = config
            self.insert_new_stat_with_cursor(cursor, host_id, stat_name,
                                             chart_name, y_value_type, y_unit)

    def insert_new_info(self, host_id, info_name, chart_name):
        cursor = self.__cnx.cursor()
        return self.insert_new_info_with_cursor(cursor, host_id,
                                                info_name, chart_name)

    def insert_new_info_with_cursor(self, cursor, host_id,
                                    info_name, chart_name):
        host_info_table_name = TableNames.get_host_info_table_name(host_id)
        insert_stmt = (
            "INSERT INTO %s SET"
            " info_name='%s',"
            " chart_name='%s'"
        ) % (host_info_table_name, info_name, chart_name)
        cursor.execute(insert_stmt)

        cursor.execute('SELECT LAST_INSERT_ID()')
        for res in cursor:
            return res[0]

        assert False

    def insert_builtin_infos(self, cursor, host_id):
        builtin_info_configs = [
            ('host_connected', 'Connection')  # 1
        ]

        for config in builtin_info_configs:
            info_name, chart_name = config
            self.insert_new_info_with_cursor(cursor, host_id,
                                             info_name, chart_name)

    def insert_new_host(self, host_name):
        cursor = self.__cnx.cursor()
        host_insert_stmt = (
            "INSERT INTO %s SET"
            " name='%s'"
        ) % (TableNames.hosts_tbl, host_name)
        cursor.execute(host_insert_stmt)

        host_id, _ = self.get_host_info(host_name)

        # insert default display setting.
        display_setting_insert_stmt = (
            "INSERT INTO %s SET"
            " host_id=%d,"
            " display_json='%s'"
        ) % (TableNames.display_setting_tbl, host_id,
             _DEFAULT_DISPLAY_SETTING_STR)
        cursor.execute(display_setting_insert_stmt)

        create_host_stat_info_tbl_stmt = (
            'CREATE TABLE IF NOT EXISTS `%s` ('
            '  `field_id` int(11) NOT NULL AUTO_INCREMENT,'
            '  `stat_name` varchar(25) NOT NULL,'
            '  `chart_name` varchar(25) NOT NULL,'
            '  `y_value_type` int(11) NOT NULL,'
            '  `y_unit` varchar(15) NOT NULL,'
            '  PRIMARY KEY (`field_id`)'
            ') ENGINE=InnoDB'
        ) % TableNames.get_host_stat_info_table_name(host_id)
        cursor.execute(create_host_stat_info_tbl_stmt)

        self.insert_builtin_fields(cursor, host_id)

        create_host_info_tbl_stmt = (
            'CREATE TABLE IF NOT EXISTS `%s` ('
            '  `info_id` int(11) NOT NULL AUTO_INCREMENT,'
            '  `info_name` varchar(25) NOT NULL,'
            '  `chart_name` varchar(25) NOT NULL,'
            '  `info_data` varbinary(64),'
            '  PRIMARY KEY (`info_id`)'
            ') ENGINE=InnoDB'
        ) % TableNames.get_host_info_table_name(host_id)
        cursor.execute(create_host_info_tbl_stmt)

        self.insert_builtin_infos(cursor, host_id)

        create_host_alarm_tbl_stmt = (
            "CREATE TABLE IF NOT EXISTS `%s` ("
            "  `alarm_id` int(11) NOT NULL AUTO_INCREMENT,"
            "  `alarm_name` varchar(25) NOT NULL,"
            "  `type` ENUM('info_alarm', 'stat_alarm') NOT NULL,"
            "  `stat_id` int(11),"
            "  `info_id` int(11),"
            "  `threshold_type` ENUM('int', 'double', 'string', 'json', 'func') NOT NULL,"
            "  `threshold` varbinary(10000) NOT NULL,"
            "  `message` varchar(256),"
            "  `emails` varchar(256),"
            "  PRIMARY KEY (`alarm_id`)"
            ") ENGINE=InnoDB"
        ) % TableNames.get_host_alarm_table_name(host_id)
        cursor.execute(create_host_alarm_tbl_stmt)

        self.__cnx.commit()
        return self.get_host_info(host_name)

    def insert_stat(self, stat_req, report_time):
        host_id = stat_req.host_id
        host_tbl_name = TableNames.get_host_stat_info_table_name(host_id)

        cursor = self.__cnx.cursor()
        insert_stmt_template = (
            "INSERT INTO %s SET"
            "  series='%s',"
            "  y_value=%s,"
            "  datetime='%s'"
        )

        for stat in stat_req.stat:
            field_id = stat.id
            field_type = self.get_stat_value_type(cursor, host_tbl_name, field_id)

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

    def get_display_setting(self, host_id):
        cursor = self.__cnx.cursor()
        select_stmt = (
            "SELECT display_json FROM %s"
            " WHERE host_id=%d"
        ) % (TableNames.display_setting_tbl, host_id)
        cursor.execute(select_stmt)

        for res in cursor:
            return res[0]

        assert False

    def update_display_setting(self, host_id, display_setting):
        cursor = self.__cnx.cursor()
        display_setting_tbl = TableNames.display_setting_tbl
        update_stmt = (
            "UPDATE %s SET"
            " display_json='%s'"
            " WHERE host_id=%d"
        ) % (display_setting_tbl, display_setting, host_id)

        cursor.execute(update_stmt)

    def update_info(self, info_req):
        info_tbl_name = TableNames.get_host_info_table_name(info_req.host_id)
        update_stmt_template = (
            "UPDATE %s SET"
            "  info_data=0x%s"
            "  WHERE info_id=%d"
        )

        cursor = self.__cnx.cursor()
        for info in info_req.basic_infos:
            update_stmt = update_stmt_template % (
                info_tbl_name, info.info.encode('hex_codec'), info.id
            )
            cursor.execute(update_stmt)

    def insert_alarm_setting(self, host_id, alarm_name,
                             alarm_type, stat_or_info_id,
                             threshold_type, threshold,
                             message, emails):
        cursor = self.__cnx.cursor()
        self.insert_alarm_setting_with_cursor(
            cursor, host_id, alarm_name, alarm_type, stat_or_info_id,
            threshold_type, threshold, message, emails)

    def insert_alarm_setting_with_cursor(
            self, cursor, host_id, alarm_name,
            alarm_type, stat_or_info_id,
            threshold_type, threshold, message, emails):
        alarm_setting_tbl = TableNames.get_host_alarm_table_name(host_id)
        if threshold_type == 'int':
            threshold = struct.pack('q', threshold)
        elif threshold_type == 'double':
            threshold = struct.pack('d', threshold)

        insert_stmt = (
            "INSERT INTO %s SET"
            "  alarm_name='%s',"
            "  type='%s',"
            "  %s=%d,"
            "  threshold_type='%s',"
            "  threshold=0x%s,"
            "  message='%s',"
            "  emails='%s'"
        ) % (
            alarm_setting_tbl, alarm_name, alarm_type,
            ('info_id' if alarm_type == 'info_alarm' else 'stat_id'),
            stat_or_info_id, threshold_type,
            threshold.encode('hex_codec'),
            message, emails
        )

        cursor.execute(insert_stmt)

    def get_alarm_settings(self, host_id):
        cursor = self.__cnx.cursor()
        select_stmt = "SELECT * FROM %s" % TableNames.get_host_alarm_table_name(host_id)
        cursor.execute(select_stmt)

        info_alarms = {}
        stat_alarms = {}
        for res in cursor:
            alarm_name = res[1]
            alarm_type = res[2]
            stat_or_info_id = res[3] if alarm_type == 'stat_alarm' else res[4]
            threshold_type = res[5]
            threshold_str = res[6]
            if threshold_type == "int":
                threshold = struct.unpack('q', threshold_str)
            elif threshold_type == "double":
                threshold = struct.unpack('d', threshold_str)
            elif threshold_type == "json":
                threshold = json.loads(threshold_str)
            elif threshold_type == "func":
                local_env = {}
                exec threshold_str in local_env
                threshold = local_env.get('threshold_func')  # TODO: this may fail
            else:
                threshold = threshold_str

            alarm_message = res[7]
            alarm_emails = res[8]

            alarm_setting = {
                'alarm_name': alarm_name,
                'threshold_type': threshold_type,
                'threshold': threshold,
                'message': alarm_message,
                'emails': alarm_emails
            }
            if alarm_type == 'stat_alarm':
                stat_alarms[stat_or_info_id] = alarm_setting
            else:
                info_alarms[stat_or_info_id] = alarm_setting

        return stat_alarms, info_alarms

    def get_stat_infos(self, cursor, host_id):
        select_stmt = "SELECT * FROM %s" % TableNames.get_host_stat_info_table_name(host_id)
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

    def get_updated_stats(self, host_id, stat_ids, last_dates):
        cursor = self.__cnx.cursor()
        stmt_template = ("SELECT * FROM %s"
                         " WHERE datetime > '%s'"
                         " ORDER BY datetime ASC")
        stat_infos = self.get_stat_infos(cursor, host_id)
        stat_infos_set = set(stat_infos)

        stats = {}
        for stat_id, last_date in zip(stat_ids, last_dates):
            if stat_id not in stat_infos_set:
                print 'field_id not in stat_infos', host_id, stat_id
                continue

            try:
                last_datetime = datetime.datetime.strptime(
                    last_date, DBConnection._DATE_FORMAT)
            except ValueError, e:
                print 'date parsing error:', e
                continue

            stat_tbl_name = TableNames.get_stat_table_name(host_id, stat_id)
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

            stats[stat_id] = this_stat

        #print stats
        return stats

    def get_host_infos(self, host_id, info_ids):
        cursor = self.__cnx.cursor()
        stmt_template = ("SELECT * FROM %s"
                         " WHERE %s")
        table_name = TableNames.get_host_info_table_name(host_id)
        condition_clause = ' OR '.join(['info_id=%d' % _i for _i in info_ids])
        stmt = stmt_template % (table_name, condition_clause)
        cursor.execute(stmt)

        infos = {}
        for res in cursor:
            info_id, info_data = res[0], res[3]
            infos[int(info_id)] = info_data

        return infos

