import gevent.server
import gevent.socket
import protocodec
import struct
import google.protobuf.service
from proto import simple_pb2


class RpcController(google.protobuf.service.RpcController):
    def __init__(self):
        self.error = None

    def Reset(self):
        self.error = None

    def Failed(self):
        return self.error is not None

    def ErrorText(self):
        return self.error

    def SetFailed(self, reason):
        self.error = reason


class TcpChannel(google.protobuf.service.RpcChannel):
    def __init__(self, addr):
        google.protobuf.service.RpcChannel.__init__(self)
        self._addr = addr
        self._socket = gevent.socket.socket()
        self.connect()

    def connect(self):
        self._socket.connect(self._addr)

    def CallMethod(self, method_descriptor, rpc_controller,
                   request, response_class, done):
        service_descriptor = method_descriptor.containing_service
        meta_info = simple_pb2.MetaInfo()
        meta_info.service_name = service_descriptor.full_name
        meta_info.method_name = method_descriptor.name
        serialized_req = protocodec.serialize_rpc_message(meta_info, request)
        self._socket.send(serialized_req)

        pb_buf = self._socket.recv(2 + struct.calcsize("!I"))
        if pb_buf[:2] != 'PB':
            print 'buffer not begin with PB'
            return None

        buf_size = struct.unpack("!I", pb_buf[2:])[0]
        pb_buf = self._socket.recv(buf_size)
        result = protocodec.parse_message(pb_buf)
        if result is None:
            print 'pb decode error, skip this message'
            return None

        meta_info, rsp = result
        if meta_info.service_name != service_descriptor.full_name or \
           meta_info.method_name != method_descriptor.name:
            print 'rsp meta not match'
            return None
        elif not isinstance(rsp, response_class):
            print 'rsp class not match'
            return None

        if done is not None:
            done(rsp)
            return None
        else:
            return rsp


class RpcServer(object):
    def __init__(self, addr):
        self._addr = addr
        self._services = {}
        self._stream_server = gevent.server.StreamServer(addr, self._handle_connection)

    def _handle_connection(self, socket, addr):
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
                result = protocodec.parse_message(pb_buf)
                if result is None:
                    print 'pb decode error, skip this message'
                    break
                meta_info, req = result

                # try to find the service
                try:
                    service = self._services[meta_info.service_name]
                except KeyError:
                    print 'cannot find the service', meta_info.service_name
                    break

                method = service.GetDescriptor().FindMethodByName(meta_info.method_name)
                if method is None:
                    print 'cannot find the method', meta_info.method_name
                    break

                controller = RpcController()
                rsp = service.CallMethod(method, controller, req, None)
                serialized_rsp = protocodec.serialize_rpc_message(meta_info, rsp)
                socket.send(serialized_rsp)

            if cur_index > 0:
                content = content[cur_index:]

        print addr, 'has disconnected'

    def register_service(self, service):
        self._services[service.GetDescriptor().full_name] = service

    def run(self):
        self._stream_server.serve_forever()



