import gevent
from proto import simple_pb2
import struct
import protocodec
import socket
import fcntl


MONSIT_SERVER_ADDR = ('127.0.0.1', 30002)


def get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', ifname[:15])
    )[20:24])


def collect_machine_info():
    machine_info = simple_pb2.SimpleRequest()
    machine_info.host_name = socket.gethostname()
    machine_info.host_ip = get_ip_address('wlan0')
    return machine_info


def handle_response(rsp):
    print 'retcode', rsp.return_code, 'msg', rsp.msg


def print_binary_string(bin_str):
    for c in bin_str:
        print ord(c),
    print ''


if __name__ == '__main__':
    sock = gevent.socket.socket()
    sock.connect(MONSIT_SERVER_ADDR)

    while True:
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
        handle_response(rsp)

        gevent.sleep(5)




