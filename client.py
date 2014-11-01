import socket
import time
import optparse
import sys

import gevent
from monsit import cpu, net, memory, disk
from monsit.proto import monsit_pb2
from recall.client import RpcClient
from recall.controller import RpcController


def get_register_info():
    reg_info = monsit_pb2.RegisterRequest()
    reg_info.host_name = socket.getfqdn()
    return reg_info


def get_cpu_usage(cpu_stat, last_cpu_stat):
    used_diff = ((cpu_stat['user'] + cpu_stat['sys'] + cpu_stat['nice']) -
                 (last_cpu_stat['user'] + last_cpu_stat['sys'] + last_cpu_stat['nice']))
    total_diff = cpu_stat['total'] - last_cpu_stat['total']
    return used_diff * 100 / total_diff


_COLLECT_INTERVAL = 30

_last_stat = {}

_CPU_ID = 1
_NETWORK_RECV_ID = 2
_NETWORK_SEND_ID = 3
_VIRTUAL_MEM_ID = 4
_SWAP_MEM_ID = 5
_DISK_WRITE_KB_ID = 6
_DISK_READ_KB_ID = 7

_SUPPORT_DATA_TYPE = {'int': int,
                      'double': float,
                      'string': str}

_register_stat_funcs = {}


def register_stat_func(stat_id, func):
    _register_stat_funcs[stat_id] = func


def get_cpu_stat(last_cpu_stats):
    cpu_stats = cpu.get_cpu_stat()

    stat_series = []
    stat_result = {'data_type': 'int',
                   'series': stat_series,
                   'memorized_data': cpu_stats}

    if last_cpu_stats is not None:
        for name, stat in cpu_stats.iteritems():
            last_stat = last_cpu_stats[name]
            cpu_usage = get_cpu_usage(stat, last_stat)
            stat_series.append((name, cpu_usage))
    else:  # the first time should set all to 0
        for name, stat in cpu_stats.iteritems():
            stat_series.append((name, 0))

    return stat_result


def collect_machine_info():
    global _last_stat

    machine_info = monsit_pb2.ReportRequest()

    for stat_id, func in _register_stat_funcs.iteritems():
        result = func(_last_stat.get(stat_id))
        type_func = _SUPPORT_DATA_TYPE[result['data_type']]
        stat_series = result['series']

        stat = machine_info.stat.add()
        stat.id = stat_id

        for name, y_value in stat_series:
            if not isinstance(y_value, type_func):
                try:
                    y_value = type_func(y_value)
                except ValueError:
                    continue

            y_val = stat.y_axis_value.add()
            y_val.name = name
            if type_func is int:
                y_val.num_value = y_value
            elif type_func is float:
                y_val.double_value = y_value
            elif type_func is str:
                y_val.str_value = y_value
            else:
                y_val.reserve_value = str(y_value)




    # get cpu stats
    cpu_stats = cpu.get_cpu_stat()
    cpu_stat = machine_info.stat.add()
    cpu_stat.id = _CPU_ID
    if 'cpu' in _last_stat:
        last_cpu_stats = _last_stat['cpu']
        for name, stat in cpu_stats.iteritems():
            last_stat = last_cpu_stats[name]
            cpu_usage = get_cpu_usage(stat, last_stat)

            y_value = cpu_stat.y_axis_value.add()
            y_value.name = name
            y_value.num_value = cpu_usage
    else:  # the first time should set all to 0
        for name, stat in cpu_stats.iteritems():
            y_value = cpu_stat.y_axis_value.add()
            y_value.name = name
            y_value.num_value = 0

    _last_stat['cpu'] = cpu_stats

    # get network stats
    net_stats = net.get_netdevs()
    net_recv_stat = machine_info.stat.add()
    net_recv_stat.id = _NETWORK_RECV_ID
    if 'net_recv' in _last_stat:
        last_net_recv_stats = _last_stat['net_recv']
        for dev_name, dev_info in net_stats.iteritems():
            last_recv_bytes = last_net_recv_stats[dev_name].recv_byte
            cur_recv_bytes = dev_info.recv_byte
            recv_rate = ((cur_recv_bytes - last_recv_bytes) /
                         _COLLECT_INTERVAL / 1024)  # MB

            y_value = net_recv_stat.y_axis_value.add()
            y_value.name = dev_name
            y_value.num_value = recv_rate
    else:  # the first time should set all to 0
        for dev_name, dev_info in net_stats.iteritems():
            y_value = net_recv_stat.y_axis_value.add()
            y_value.name = dev_name
            y_value.num_value = 0

    _last_stat['net_recv'] = net_stats

    net_send_stat = machine_info.stat.add()
    net_send_stat.id = _NETWORK_SEND_ID
    if 'net_send' in _last_stat:
        last_net_send_stats = _last_stat['net_send']
        for dev_name, dev_info in net_stats.iteritems():
            last_send_bytes = last_net_send_stats[dev_name].send_byte
            cur_send_bytes = dev_info.send_byte
            send_rate = ((cur_send_bytes - last_send_bytes) /
                         _COLLECT_INTERVAL / 1024)  # MB

            y_value = net_send_stat.y_axis_value.add()
            y_value.name = dev_name
            y_value.num_value = send_rate
    else:  # the first time should set all to 0
        for dev_name, dev_info in net_stats.iteritems():
            y_value = net_send_stat.y_axis_value.add()
            y_value.name = dev_name
            y_value.num_value = 0

    _last_stat['net_send'] = net_stats

    # get memory stats
    vmem_info, swap_info = memory.get_mem_stat()
    vmem_stat = machine_info.stat.add()
    vmem_stat.id = _VIRTUAL_MEM_ID
    y_value = vmem_stat.y_axis_value.add()
    y_value.name = 'vmem'
    y_value.num_value = int(vmem_info.percent)

    swap_stat = machine_info.stat.add()
    swap_stat.id = _SWAP_MEM_ID
    y_value = swap_stat.y_axis_value.add()
    y_value.name = 'swap'
    y_value.num_value = int(swap_info.percent)

    # get disk io stat
    disk_io_counters = disk.get_io_counters()
    disk_basic_infos = disk.get_partitions()

    disk_io_write_stat = machine_info.stat.add()
    disk_io_write_stat.id = _DISK_WRITE_KB_ID
    if 'disk_io_write' in _last_stat:
        last_disk_io_write_stat = _last_stat['disk_io_write']
        for info in disk_basic_infos:
            dev_name = info.device
            dev_name = dev_name.split('/')[-1]
            if dev_name not in disk_io_counters:
                continue

            last_disk_stat = last_disk_io_write_stat[dev_name]
            last_write = last_disk_stat.write_bytes

            cur_disk_stat = disk_io_counters[dev_name]
            cur_write = cur_disk_stat.write_bytes

            write_rate = (cur_write - last_write) / _COLLECT_INTERVAL / 1024  # KB
            if write_rate < 0:
                write_rate = 0

            y_value = disk_io_write_stat.y_axis_value.add()
            y_value.name = dev_name
            y_value.num_value = write_rate
    else:  # the first time should set all to 0
        for info in disk_basic_infos:
            dev_name = info.device
            dev_name = dev_name.split('/')[-1]
            if dev_name not in disk_io_counters:
                continue

            y_value = disk_io_write_stat.y_axis_value.add()
            y_value.name = dev_name
            y_value.num_value = 0

    _last_stat['disk_io_write'] = disk_io_counters

    disk_io_read_stat = machine_info.stat.add()
    disk_io_read_stat.id = _DISK_READ_KB_ID
    if 'disk_io_read' in _last_stat:
        last_disk_io_read_stat = _last_stat['disk_io_read']
        for info in disk_basic_infos:
            dev_name = info.device
            dev_name = dev_name.split('/')[-1]
            if dev_name not in disk_io_counters:
                continue

            last_disk_stat = last_disk_io_read_stat[dev_name]
            last_read = last_disk_stat.read_bytes

            cur_disk_stat = disk_io_counters[dev_name]
            cur_read = cur_disk_stat.read_bytes

            read_rate = (cur_read - last_read) / _COLLECT_INTERVAL / 1024  # KB
            if read_rate < 0:
                read_rate = 0

            y_value = disk_io_read_stat.y_axis_value.add()
            y_value.name = dev_name
            y_value.num_value = read_rate
    else:  # the first time should set all to 0
        for info in disk_basic_infos:
            dev_name = info.device
            dev_name = dev_name.split('/')[-1]
            if dev_name not in disk_io_counters:
                continue

            y_value = disk_io_read_stat.y_axis_value.add()
            y_value.name = dev_name
            y_value.num_value = 0

    _last_stat['disk_io_read'] = disk_io_counters

    machine_info.datetime = int(time.time())

    return machine_info


def collect_thread(master_addr, interval):
    rpc_client = RpcClient()
    tcp_channel = rpc_client.get_tcp_channel(master_addr)
    stub = monsit_pb2.MonsitService_Stub(tcp_channel)

    # first register to the master
    req = get_register_info()
    controller = RpcController(method_timeout=10)
    rsp = stub.Register(controller, req)
    if rsp is None:
        print 'Failed to register to master'
        sys.exit(1)
    elif rsp.return_code != 0:
        print 'Failed to register to master: ', rsp.msg
        sys.exit(1)

    host_id = rsp.host_id

    while True:
        req = collect_machine_info()
        req.host_id = host_id
        controller = RpcController()
        rsp = stub.Report(controller, req)
        print rsp
        gevent.sleep(interval)


def main():
    optparser = optparse.OptionParser(usage = "%prog [options]")
    optparser.add_option('--master-ip', dest="master_ip",
                         help="IP of the master", default="127.0.0.1")
    optparser.add_option('--master-port', dest="master_port",
                         help="Port of the master", default='30002')

    opts, args = optparser.parse_args()
    master_addr = opts.master_ip + ':' + opts.master_port

    job = gevent.spawn(collect_thread, master_addr, _COLLECT_INTERVAL)

    try:
        job.join()
    except KeyboardInterrupt:
        print 'monsit agent got SIGINT, exit.'


if __name__ == '__main__':
    main()
