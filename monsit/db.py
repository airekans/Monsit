import datetime

import mysql.connector

from monsit.proto import monsit_pb2


_DB_CONFIG = {'host': '127.0.0.1',
              'user': 'monsit',
              'password': 'monsitpass',
              'database': 'monsit'}
_POOL_SIZE = mysql.connector.pooling.CNX_POOL_MAXSIZE

_VALID_FIELDS = ['cpu', 'net', 'vmem', 'swap',
                 'disk_io', 'disk_usage']


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

    @staticmethod
    def get_mysql_type_str(value_type):
        return ValueType._mysql_type_str[value_type]


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

    def insert_builtin_fields(self, cursor, host_id):
        builtin_field_configs = [
            ('cpu_total', 'Total Usage', ValueType.Int, '%'),
            ('network_recv', 'Recv', ValueType.Int, 'KB'),
            ('network_send', 'Send', ValueType.Int, 'KB')
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

        vmem_table_stmt = (
            'CREATE TABLE IF NOT EXISTS `%s` ('
            '  `id` int(11) NOT NULL AUTO_INCREMENT,'
            '  `total` bigint NOT NULL,'
            '  `available` bigint NOT NULL,'
            '  `used` bigint NOT NULL,'
            '  `percent` int(11) NOT NULL,'
            '  `datetime` datetime NOT NULL,'
            '  PRIMARY KEY (`id`)'
            ') ENGINE=InnoDB'
        ) % DBConnection.get_host_table_name(host_id, 'vmem')
        cursor.execute(vmem_table_stmt)

        swap_table_stmt = (
            'CREATE TABLE IF NOT EXISTS `%s` ('
            '  `id` int(11) NOT NULL AUTO_INCREMENT,'
            '  `total` bigint NOT NULL,'
            '  `free` bigint NOT NULL,'
            '  `used` bigint NOT NULL,'
            '  `percent` int(11) NOT NULL,'
            '  `datetime` datetime NOT NULL,'
            '  PRIMARY KEY (`id`)'
            ') ENGINE=InnoDB'
        ) % DBConnection.get_host_table_name(host_id, 'swap')
        cursor.execute(swap_table_stmt)

        disk_io_table_stmt = (
            'CREATE TABLE IF NOT EXISTS `%s` ('
            '  `id` int(11) NOT NULL AUTO_INCREMENT,'
            '  `device_name` varchar(14) NOT NULL,'
            '  `read_count` bigint NOT NULL,'
            '  `write_count` bigint NOT NULL,'
            '  `read_bytes` bigint NOT NULL,'
            '  `write_bytes` bigint NOT NULL,'
            '  `read_time` bigint NOT NULL,'
            '  `write_time` bigint NOT NULL,'
            '  `datetime` datetime NOT NULL,'
            '  PRIMARY KEY (`id`)'
            ') ENGINE=InnoDB'
        ) % DBConnection.get_host_table_name(host_id, 'disk_io')
        cursor.execute(disk_io_table_stmt)

        disk_usage_table_stmt = (
            'CREATE TABLE IF NOT EXISTS `%s` ('
            '  `id` int(11) NOT NULL AUTO_INCREMENT,'
            '  `device_name` varchar(14) NOT NULL,'
            '  `total` bigint NOT NULL,'
            '  `used` bigint NOT NULL,'
            '  `free` bigint NOT NULL,'
            '  `percent` int(11) NOT NULL,'
            '  `datetime` datetime NOT NULL,'
            '  PRIMARY KEY (`id`)'
            ') ENGINE=InnoDB'
        ) % DBConnection.get_host_table_name(host_id, 'disk_usage')
        cursor.execute(disk_usage_table_stmt)

        self.__cnx.commit()

    def insert_host_info(self, host_info, host_id):
        cursor = self.__cnx.cursor()

        report_time = datetime.datetime.fromtimestamp(host_info.datetime)
        report_time = report_time.strftime("%Y-%m-%d %H:%M:%S")
        cpu_tbl_name = self.get_host_table_name(host_id, 'cpu')
        net_tbl_name = self.get_host_table_name(host_id, 'net')
        vmem_tbl_name = self.get_host_table_name(host_id, 'vmem')
        swap_tbl_name = self.get_host_table_name(host_id, 'swap')
        disk_io_tbl_name = self.get_host_table_name(host_id, 'disk_io')
        disk_usage_tbl_name = self.get_host_table_name(host_id, 'disk_usage')

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

        vmem_info = host_info.mem_info.virtual_mem
        vmem_insert_stmt = (
            "INSERT INTO %s SET"
            " total=%d,"
            " available=%d,"
            " used=%d,"
            " percent=%d,"
            " datetime='%s'"
        ) % (vmem_tbl_name, vmem_info.total, vmem_info.available,
             vmem_info.used, vmem_info.percent, report_time)
        try:
            cursor.execute(vmem_insert_stmt)
        except mysql.connector.Error as err:
            print err.msg
            self.__cnx.rollback()
            return False

        swap_info = host_info.mem_info.swap_mem
        swap_insert_stmt = (
            "INSERT INTO %s SET"
            " total=%d,"
            " free=%d,"
            " used=%d,"
            " percent=%d,"
            " datetime='%s'"
        ) % (swap_tbl_name, swap_info.total, swap_info.free,
             swap_info.used, swap_info.percent, report_time)
        try:
            cursor.execute(swap_insert_stmt)
        except mysql.connector.Error as err:
            print err.msg
            self.__cnx.rollback()
            return False

        disk_io_insert_stmt = (
            "INSERT INTO %s SET"
            " device_name='%s',"
            " read_count=%d,"
            " write_count=%d,"
            " read_bytes=%d,"
            " write_bytes=%d,"
            " read_time=%d,"
            " write_time=%d,"
            " datetime='%s'"
        )
        disk_usage_insert_stmt = (
            "INSERT INTO %s SET"
            " device_name='%s',"
            " total=%d,"
            " used=%d,"
            " free=%d,"
            " percent=%d,"
            " datetime='%s'"
        )
        for disk_info in host_info.disk_infos:
            io_counters = disk_info.io_counters
            disk_usage = disk_info.usage
            try:
                stmt = disk_io_insert_stmt % (
                    disk_io_tbl_name,
                    disk_info.device_name,
                    io_counters.read_count,
                    io_counters.write_count,
                    io_counters.read_bytes,
                    io_counters.write_bytes,
                    io_counters.read_time,
                    io_counters.write_time,
                    report_time
                )
                cursor.execute(stmt)

                stmt = disk_usage_insert_stmt % (
                    disk_usage_tbl_name,
                    disk_info.device_name,
                    disk_usage.total,
                    disk_usage.used,
                    disk_usage.free,
                    disk_usage.percent,
                    report_time
                )
                cursor.execute(stmt)
            except mysql.connector.Error as err:
                print err.msg
                self.__cnx.rollback()
                return False

        self.__cnx.commit()
        return True

    def get_host_stats(self, host_id, fields):
        LAST_NUM_MIN = 30
        cursor = self.__cnx.cursor()
        stmt_template = ('SELECT * FROM %s' +
                         (' WHERE DATE_SUB(NOW(),INTERVAL %d MINUTE) <= datetime' % LAST_NUM_MIN) +
                         ' ORDER BY datetime ASC')
        stats = {}
        for field in fields:
            if field in _VALID_FIELDS:
                select_stmt = stmt_template % self.get_host_table_name(host_id, field)
                if field == 'cpu':
                    cursor.execute(select_stmt)
                    cpu_infos = []
                    for stat in cursor:
                        cpu_info = monsit_pb2.CPUInfo(name=stat[1],
                                                      user_count=stat[2],
                                                      nice_count=stat[3],
                                                      sys_count=stat[4],
                                                      idle_count=stat[5],
                                                      iowait_count=stat[6],
                                                      total_count=stat[7])
                        cpu_infos.append((stat[8], cpu_info))

                    stats[field] = cpu_infos
                elif field == 'net':
                    cursor.execute(select_stmt)
                    net_infos = []
                    try:
                        for stat in cursor:
                            net_info = monsit_pb2.NetInfo(name=stat[1],
                                                          ip=stat[2],
                                                          recv_byte=stat[4],
                                                          send_byte=stat[5])
                            net_infos.append((stat[3], net_info))
                    except:
                        import traceback
                        traceback.print_exc()
                        raise

                    stats[field] = net_infos
                elif field == 'vmem':
                    cursor.execute(select_stmt)
                    vmem_infos = []
                    try:
                        for stat in cursor:
                            vmem_info = monsit_pb2.VirtualMemInfo(total=stat[1],
                                                                  available=stat[2],
                                                                  used=stat[3],
                                                                  percent=stat[4])
                            vmem_infos.append((stat[5], vmem_info))
                    except:
                        import traceback
                        traceback.print_exc()
                        raise

                    stats[field] = vmem_infos
                elif field == 'swap':
                    cursor.execute(select_stmt)
                    swap_infos = []
                    try:
                        for stat in cursor:
                            swap_info = monsit_pb2.SwapMemInfo(total=stat[1],
                                                               free=stat[2],
                                                               used=stat[3],
                                                               percent=stat[4])
                            swap_infos.append((stat[5], swap_info))
                    except:
                        import traceback
                        traceback.print_exc()
                        raise

                    stats[field] = swap_infos
                elif field == 'disk_io':
                    cursor.execute(select_stmt)
                    disk_io_infos = []
                    try:
                        for stat in cursor:
                            disk_io_info = \
                                monsit_pb2.DiskInfo.IOCounter(read_count=stat[2],
                                                              write_count=stat[3],
                                                              read_bytes=stat[4],
                                                              write_bytes=stat[5],
                                                              read_time=stat[6],
                                                              write_time=stat[7])
                            disk_io_infos.append((stat[8], disk_io_info, stat[1]))
                    except:
                        import traceback
                        traceback.print_exc()
                        raise

                    stats[field] = disk_io_infos
                elif field == 'disk_usage':
                    cursor.execute(select_stmt)
                    disk_usage_infos = []
                    try:
                        for stat in cursor:
                            disk_usage_info = \
                                monsit_pb2.DiskInfo.UsageInfo(total=stat[2],
                                                              used=stat[3],
                                                              free=stat[4],
                                                              percent=stat[5])
                            disk_usage_infos.append((stat[6], disk_usage_info, stat[1]))
                    except:
                        import traceback
                        traceback.print_exc()
                        raise

                    stats[field] = disk_usage_infos

        #print stats
        return stats

    def get_updated_stats(self, host_id, field_type, last_date):
        return
