import gevent
from proto import simple_pb2
import struct
import protocodec
import socket
import net
import time
import cpu


MONSIT_SERVER_ADDR = ('127.0.0.1', 30002)


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


def handle_response(rsp):
    print 'retcode', rsp.return_code, 'msg', rsp.msg

    if isinstance(rsp, simple_pb2.RegisterResponse):
        return rsp.return_code == 0
    else:
        return True




def print_binary_string(bin_str):
    for c in bin_str:
        print ord(c),
    print ''


if __name__ == '__main__':
    sock = gevent.socket.socket()
    sock.connect(MONSIT_SERVER_ADDR)

    # first register to the master
    is_registered = False

    while True:
        if not is_registered:
            req = get_register_info()
        else:
            req = collect_machine_info()

        req_buf = protocodec.serialize_message(req)

        sock.send(req_buf)
        rsp_buf = sock.recv(1024)
        while len(rsp_buf) < 6:
            rsp_buf += sock.recv(1024)

        if rsp_buf[:2] != 'PB':
            continue

        (rsp_len,) = struct.unpack('!I', rsp_buf[2:6])
        while len(rsp_buf) < 6 + rsp_len:
            rsp_buf += sock.recv(1024)

        rsp = protocodec.parse_message(rsp_buf[6:6 + rsp_len])
        if not is_registered:
            if handle_response(rsp):
                is_registered = True
            else:
                break

        gevent.sleep(5)




