import gevent.server
import struct
from proto import simple_pb2
import protocodec
from gevent import monkey
monkey.patch_all()

import db


class ProtocolServer(object):

    def __init__(self):
        self.__req_handlers = {}

    def register_handler(self, req_cls, handler):
        # TODO: double check the performance of hashing long stirng
        self.__req_handlers[req_cls.DESCRIPTOR.full_name] = handler

    def handle_req(self, req):
        req_full_name = req.DESCRIPTOR.full_name
        try:
            handler = self.__req_handlers[req_full_name]
        except KeyError as e:
            print 'Cannot find handler for %s: %s' % (req_full_name, str(e))
            return ProtocolServer._get_error_rsp(1, 'cannot find handler for ' + req_full_name)

        try:
            rsp = handler(req)
        except:
            return ProtocolServer._get_error_rsp(2, 'Exception when handling ' + req_full_name)

        return rsp

    @staticmethod
    def _get_error_rsp(return_code, err_msg):
        rsp = simple_pb2.SimpleResponse()
        rsp.return_code = return_code
        rsp.msg = err_msg
        return rsp


def handle_simple_req(req):
    print req

    if len(req.net_infos) > 0:
        db.create_host_tables(req.net_infos[0].ip)
        if not db.insert_host_info(req):
            print 'failed to store req in db'

    rsp = simple_pb2.SimpleResponse()
    rsp.return_code = 0
    rsp.msg = 'success'
    return rsp


def print_binary_string(bin_str):
    for c in bin_str:
        print ord(c),
    print ''


_pb_server = ProtocolServer()


def init_pb_server():
    global _pb_server

    _pb_server.register_handler(simple_pb2.SimpleRequest, handle_simple_req)


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

            rsp = _pb_server.handle_req(req)
            serialized_rsp = protocodec.serialize_message(rsp)
            socket.send(serialized_rsp)

        if cur_index > 0:
            content = content[cur_index:]

    print addr, 'has disconnected'

if __name__ == '__main__':
    db.init()
    init_pb_server()
    server = gevent.server.StreamServer(('127.0.0.1', 30002), handle)
    server.serve_forever()
