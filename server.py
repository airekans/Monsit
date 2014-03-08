import gevent.server
import struct
from proto import simple_pb2
import protocodec


def handle_simple_req(req):
    print 'req.name', req.name, 'req.id', req.id

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
            print_binary_string(pb_buf)
            req = simple_pb2.SimpleRequest()
            try:
                req.ParseFromString(pb_buf)
                is_parsed_req = True
            except simple_pb2.message.DecodeError:
                print 'pb decode error, skip this message'
                is_parsed_req = False
            finally:
                cur_index += buf_size + 6

            if is_parsed_req:
                rsp = handle_simple_req(req)
                serialized_rsp = protocodec.serialize_message(rsp)
                socket.send(serialized_rsp)

        if cur_index > 0:
            content = content[cur_index:]

    print addr, 'has closed'

if __name__ == '__main__':
    server = gevent.server.StreamServer(('127.0.0.1', 30002), handle)
    server.serve_forever()