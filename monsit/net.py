from collections import namedtuple
import fcntl
import socket
import struct


_data = namedtuple('data', ['ip', 'recv_byte', 'send_byte'])


def get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        return socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', ifname[:15])
        )[20:24])
    except IOError:
        return ''


def get_netdevs():
    ''' RX and TX bytes for each of the network devices
    '''

    with open('/proc/net/dev') as f:
        net_dump = f.readlines()

    device_data = {}
    for line in net_dump[2:]:
        line = line.split(':')
        dev_name = line[0].strip()
        if dev_name != 'lo':
            ip_addr = get_ip_address(dev_name)
            if ip_addr:
                device_data[dev_name] = \
                    _data(ip_addr,
                          int(line[1].split()[0]),
                          int(line[1].split()[8]))

    return device_data

