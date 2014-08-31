import socket
import time
import optparse
import sys

import gevent
from monsit import cpu, rpc, net, memory, disk

from monsit.proto import monsit_pb2


def get_register_info():
    reg_info = monsit_pb2.RegisterRequest()
    reg_info.host_name = socket.getfqdn()
    return reg_info


def get_cpu_usage(cpu_stat, last_cpu_stat):
    used_diff = ((cpu_stat['user'] + cpu_stat['sys'] + cpu_stat['nice']) -
                 (last_cpu_stat['user'] + last_cpu_stat['sys'] + last_cpu_stat['nice']))
    total_diff = cpu_stat['total'] - last_cpu_stat['total']
    return used_diff * 100 / total_diff


_last_stat = {}
_CPU_ID = 1


def collect_machine_info(is_first_time):
    global _last_stat

    machine_info = monsit_pb2.ReportRequest()

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

    machine_info.datetime = int(time.time())

    return machine_info


def collect_thread(master_addr, interval):
    rpc_client = rpc.RpcClient()
    tcp_channel = rpc_client.get_tcp_channel(master_addr)
    stub = monsit_pb2.MonsitService_Stub(tcp_channel)

    # first register to the master
    req = get_register_info()
    controller = rpc.RpcController(method_timeout=10)
    rsp = stub.Register(controller, req)
    if rsp is None:
        print 'Failed to register to master'
        sys.exit(1)
    elif rsp.return_code != 0:
        print 'Failed to register to master: ', rsp.msg
        sys.exit(1)

    host_id = rsp.host_id

    is_first_time = True
    while True:
        req = collect_machine_info(is_first_time)
        req.host_id = host_id
        is_first_time = False
        controller = rpc.RpcController()
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

    job = gevent.spawn(collect_thread, master_addr, 30)

    try:
        job.join()
    except KeyboardInterrupt:
        print 'monsit agent got SIGINT, exit.'


if __name__ == '__main__':
    main()
