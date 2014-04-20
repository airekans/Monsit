import socket
import time
import optparse
import sys

import gevent
from monsit import cpu, rpc, net

from monsit.proto import simple_pb2


def get_register_info():
    reg_info = simple_pb2.RegisterRequest()
    reg_info.host_name = socket.gethostname()
    return reg_info


def collect_machine_info():
    machine_info = simple_pb2.SimpleRequest()
    machine_info.host_name = socket.gethostname()

    cpu_stats = cpu.get_cpu_stat()
    for name, stat in cpu_stats.iteritems():
        cpu_info = machine_info.cpu_infos.add()
        cpu_info.name = name
        cpu_info.user_count = stat['user']
        cpu_info.nice_count = stat['nice']
        cpu_info.sys_count = stat['sys']
        cpu_info.idle_count = stat['idle']
        cpu_info.iowait_count = stat['iowait']
        cpu_info.total_count = stat['total']

    net_infos = net.get_netdevs()
    for dev_name, dev_info in net_infos.iteritems():
        net_info = machine_info.net_infos.add()
        net_info.name = dev_name
        net_info.ip = dev_info.ip
        net_info.recv_byte = dev_info.recv_byte
        net_info.send_byte = dev_info.send_byte

    machine_info.datetime = int(time.time())

    return machine_info


def collect_thread(master_addr, interval):
    opts, args = optparser.parse_args()
    master_addr = (opts.master_ip, opts.master_port)

    tcp_channel = rpc.TcpChannel(master_addr)
    stub = simple_pb2.MonsitService_Stub(tcp_channel)

    # first register to the master
    req = get_register_info()
    controller = rpc.RpcController()
    rsp = stub.Register(controller, req)
    if rsp.return_code != 0:
        print 'Failed to register to master: ', rsp.msg
        sys.exit(1)

    while True:
        req = collect_machine_info()
        controller = rpc.RpcController()
        rsp = stub.Report(controller, req)
        print rsp
        gevent.sleep(interval)


if __name__ == '__main__':
    optparser = optparse.OptionParser(usage = "%prog [options]")
    optparser.add_option('--master-ip', dest="master_ip",
                         help="IP of the master", default="127.0.0.1")
    optparser.add_option('--master-port', dest="master_port",
                         help="Port of the master", type="int",
                         default=30002)

    opts, args = optparser.parse_args()
    master_addr = (opts.master_ip, opts.master_port)

    job = gevent.spawn(lambda: collect_thread(master_addr, 30))

    try:
        job.join()
    except KeyboardInterrupt:
        print 'monsit agent got SIGINT, exit.'
