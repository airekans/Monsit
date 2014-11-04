import socket
import time
import optparse
import sys
import importlib
import inspect
import traceback

import gevent
from monsit import cpu, net, memory, disk
from monsit.proto import monsit_pb2
from monsit import api

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


_new_net_stats = [None]


def get_net_recv_stat(last_net_recv_stats):
    if _new_net_stats[0] is None:
        net_stats = net.get_netdevs()
        _new_net_stats[0] = net_stats
    else:
        net_stats = _new_net_stats[0]
        _new_net_stats[0] = None

    stat_series = []
    stat_result = {'data_type': 'int',
                   'series': stat_series,
                   'memorized_data': net_stats}

    if last_net_recv_stats is not None:
        for dev_name, dev_info in net_stats.iteritems():
            last_recv_bytes = last_net_recv_stats[dev_name].recv_byte
            cur_recv_bytes = dev_info.recv_byte
            recv_rate = ((cur_recv_bytes - last_recv_bytes) /
                         _COLLECT_INTERVAL / 1024)  # MB
            stat_series.append((dev_name, recv_rate))
    else:  # the first time should set all to 0
        for dev_name, dev_info in net_stats.iteritems():
            stat_series.append((dev_name, 0))

    return stat_result


def get_net_send_stat(last_net_send_stats):
    if _new_net_stats[0] is None:
        net_stats = net.get_netdevs()
        _new_net_stats[0] = net_stats
    else:
        net_stats = _new_net_stats[0]
        _new_net_stats[0] = None

    stat_series = []
    stat_result = {'data_type': 'int',
                   'series': stat_series,
                   'memorized_data': net_stats}

    if last_net_send_stats is not None:
        for dev_name, dev_info in net_stats.iteritems():
            last_send_bytes = last_net_send_stats[dev_name].send_byte
            cur_send_bytes = dev_info.send_byte
            send_rate = ((cur_send_bytes - last_send_bytes) /
                         _COLLECT_INTERVAL / 1024)  # MB
            stat_series.append((dev_name, send_rate))
    else:  # the first time should set all to 0
        for dev_name, dev_info in net_stats.iteritems():
            stat_series.append((dev_name, 0))

    return stat_result


_new_memory_stat = [None]


def get_virtual_memory_stat(_):
    if _new_memory_stat[0] is None:
        vmem_info, swap_info = memory.get_mem_stat()
        _new_memory_stat[0] = (vmem_info, swap_info)
    else:
        vmem_info, _ = _new_memory_stat[0]
        _new_memory_stat[0] = None

    return {'data_type': 'int',
            'series': [('vmem', int(vmem_info.percent))]}


def get_swap_memory_stat(_):
    if _new_memory_stat[0] is None:
        vmem_info, swap_info = memory.get_mem_stat()
        _new_memory_stat[0] = (vmem_info, swap_info)
    else:
        _, swap_info = _new_memory_stat[0]
        _new_memory_stat[0] = None

    return {'data_type': 'int',
            'series': [('swap', int(swap_info.percent))]}


_new_disk_stat = [None]


def get_disk_write_stat(last_disk_io_write_stat):
    if _new_disk_stat[0] is None:
        disk_io_counters = disk.get_io_counters()
        disk_basic_infos = disk.get_partitions()
        _new_disk_stat[0] = (disk_io_counters, disk_basic_infos)
    else:
        disk_io_counters, disk_basic_infos = _new_disk_stat[0]
        _new_disk_stat[0] = None

    stat_series = []
    stat_result = {'data_type': 'int',
                   'series': stat_series,
                   'memorized_data': disk_io_counters}

    if last_disk_io_write_stat is not None:
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

            stat_series.append((dev_name, write_rate))
    else:  # the first time should set all to 0
        for info in disk_basic_infos:
            dev_name = info.device
            dev_name = dev_name.split('/')[-1]
            if dev_name not in disk_io_counters:
                continue

            stat_series.append((dev_name, 0))

    return stat_result


def get_disk_read_stat(last_disk_io_read_stat):
    if _new_disk_stat[0] is None:
        disk_io_counters = disk.get_io_counters()
        disk_basic_infos = disk.get_partitions()
        _new_disk_stat[0] = (disk_io_counters, disk_basic_infos)
    else:
        disk_io_counters, disk_basic_infos = _new_disk_stat[0]
        _new_disk_stat[0] = None

    stat_series = []
    stat_result = {'data_type': 'int',
                   'series': stat_series,
                   'memorized_data': disk_io_counters}

    if last_disk_io_read_stat is not None:
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

            stat_series.append((dev_name, read_rate))
    else:  # the first time should set all to 0
        for info in disk_basic_infos:
            dev_name = info.device
            dev_name = dev_name.split('/')[-1]
            if dev_name not in disk_io_counters:
                continue

            stat_series.append((dev_name, 0))

    return stat_result


def collect_machine_info():
    global _last_stat

    register_stat_funcs = api.get_registered_stat_funcs()
    machine_info = monsit_pb2.ReportRequest()

    for stat_id, func in register_stat_funcs.iteritems():
        result = func(_last_stat.get(stat_id))
        type_func = _SUPPORT_DATA_TYPE[result['data_type']]
        stat_series = result['series']
        if 'memorized_data' in result:
            _last_stat[stat_id] = result['memorized_data']

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

    machine_info.datetime = int(time.time())

    return machine_info


def register_builtin_stat_funcs():
    api.register_stat_func(_CPU_ID, get_cpu_stat)
    api.register_stat_func(_NETWORK_RECV_ID, get_net_recv_stat)
    api.register_stat_func(_NETWORK_SEND_ID, get_net_send_stat)
    api.register_stat_func(_VIRTUAL_MEM_ID, get_virtual_memory_stat)
    api.register_stat_func(_SWAP_MEM_ID, get_swap_memory_stat)
    api.register_stat_func(_DISK_WRITE_KB_ID, get_disk_write_stat)
    api.register_stat_func(_DISK_READ_KB_ID, get_disk_read_stat)


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
    optparser.add_option('-u', '--user-module', dest="user_module",
                         help="Name of the user module")
    optparser.add_option('-e', '--user-stat-func', dest="user_stat_func",
                         help="Name of the user stat function")

    opts, args = optparser.parse_args()
    master_addr = opts.master_ip + ':' + opts.master_port
    get_user_stat_func = None
    if opts.user_module is not None:
        get_user_stat_func_name = 'get_stat_funcs'
        if opts.user_stat_func is not None:
            get_user_stat_func_name = opts.user_stat_func

        try:
            user_module = importlib.import_module(opts.user_module)
            get_user_stat_func = user_module.__dict__.get(get_user_stat_func_name)
            if not inspect.isfunction(get_user_stat_func):
                get_user_stat_func = None
        except ImportError, e:
            print 'import user module error:', e

    register_builtin_stat_funcs()
    if get_user_stat_func is not None:
        try:
            for stat_id, stat_func in get_user_stat_func():
                api.register_stat_func(stat_id, stat_func)
        except:
            traceback.print_exc()
            sys.exit(1)

    job = gevent.spawn(collect_thread, master_addr, _COLLECT_INTERVAL)

    try:
        job.join()
    except KeyboardInterrupt:
        print 'monsit agent got SIGINT, exit.'


if __name__ == '__main__':
    main()
