import gevent.server
import struct
from proto import simple_pb2


def serialize_message(msg):
    return ''


def handle_simple_req(req):
    # TODO: add implementation
    rsp = simple_pb2.SimpleResponse()
    return rsp


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
                break

            buf_size = struct.unpack('!I', mem_content[cur_index + 2: cur_index + 6])
            if len(mem_content[cur_index + 6:]) < buf_size:
                break

            pb_buf = mem_content[cur_index + 6: cur_index + buf_size].tobytes()
            req = simple_pb2.SimpleRequest()
            try:
                req.ParseFromString(pb_buf)
            except simple_pb2.message.DecodeError:
                pass
            finally:
                cur_index += buf_size + 6

            rsp = handle_simple_req(req)
            serialized_rsp = serialize_message(rsp)
            socket.send(serialized_rsp)

        if cur_index > 0:
            content = content[cur_index:]

    print addr, 'has closed'

if __name__ == '__main__':
    server = gevent.server.StreamServer(('127.0.0.1', 30002), handle)
    server.serve_forever()
