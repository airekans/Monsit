import gevent.server
import struct
from proto import simple_pb2
import protocodec
import db


def handle_simple_req(req):
    print req

    rsp = simple_pb2.SimpleResponse()
    rsp.return_code = 0
    rsp.msg = 'success'
    return rsp


def print_binary_string(bin_str):
    for c in bin_str:
        print ord(c),
    print ''


def handle(socket, addr):
    print addr

    content = ""
    while True:
        try:
            recv_buf = socket.recv(1024)
            if len(recv_buf) == 0:
                break
        except Exception, e:
            print e
            break

        content += recv_buf
        mem_content = memoryview(content)
        cur_index = 0
        while cur_index < len(content):
            if len(mem_content[cur_index:]) < 6:
                break
            elif mem_content[cur_index:cur_index + 2] != 'PB':
                cur_index += 2  # skip the first 2 bytes
                break

            (buf_size,) = struct.unpack('!I',
                                        mem_content[cur_index + 2: cur_index + 6].tobytes())
            if len(mem_content[cur_index + 6:]) < buf_size:
                break

            pb_buf = mem_content[cur_index + 6: cur_index + 6 + buf_size].tobytes()
            cur_index += buf_size + 6
            req = protocodec.parse_message(pb_buf)
            if req is None:
                print 'pb decode error, skip this message'
                break

            rsp = handle_simple_req(req)
            serialized_rsp = protocodec.serialize_message(rsp)
            socket.send(serialized_rsp)

        if cur_index > 0:
            content = content[cur_index:]

    print addr, 'has disconnected'

if __name__ == '__main__':
    db.init()
    server = gevent.server.StreamServer(('127.0.0.1', 30002), handle)
    server.serve_forever()
