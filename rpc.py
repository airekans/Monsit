import gevent.server
import gevent.socket
import struct
import google.protobuf.service
from google.protobuf import message
from proto import rpc_meta_pb2


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


def _serialize_message(meta_info, msg):
    meta_info.msg_name = msg.DESCRIPTOR.full_name

    meta_buf = meta_info.SerializeToString()
    msg_buf = msg.SerializeToString()
    meta_buf_len = len(meta_buf)
    msg_buf_len = len(msg_buf)

    pb_buf_len = struct.pack('!III', meta_buf_len + msg_buf_len + 8,
                             meta_buf_len, msg_buf_len)
    msg_buf = 'PB' + pb_buf_len + meta_buf + msg_buf
    return msg_buf


def parse_meta(buf):
    if len(buf) < 8:
        return None

    (meta_len, pb_msg_len) = struct.unpack('!II', buf[:8])
    if len(buf) < 8 + meta_len + pb_msg_len:
        return None

    meta_msg_buf = buf[8:8 + meta_len]
    meta_info = rpc_meta_pb2.MetaInfo()
    try:
        meta_info.ParseFromString(meta_msg_buf)
        return meta_len, pb_msg_len, meta_info
    except message.DecodeError as err:
        print 'parsing msg meta info failed: ' + str(err)
        return None


def _parse_message(buf, msg_cls):
    msg = msg_cls()
    try:
        msg.ParseFromString(buf)
        return msg
    except message.DecodeError as err:
        print 'parsing msg failed: ' + str(err)
        return None


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
        meta_info = rpc_meta_pb2.MetaInfo()
        meta_info.service_name = service_descriptor.full_name
        meta_info.method_name = method_descriptor.name
        serialized_req = _serialize_message(meta_info, request)
        self._socket.send(serialized_req)

        pb_buf = self._socket.recv(2 + struct.calcsize("!I"))
        if pb_buf[:2] != 'PB':
            print 'buffer not begin with PB'
            return None

        buf_size = struct.unpack("!I", pb_buf[2:])[0]
        pb_buf = self._socket.recv(buf_size)
        result = parse_meta(pb_buf)
        if result is None:
            print 'pb decode error, skip this message'
            return None

        meta_len, pb_msg_len, meta_info = result
        if meta_info.service_name != service_descriptor.full_name or \
           meta_info.method_name != method_descriptor.name or \
           meta_info.msg_name != response_class.DESCRIPTOR.full_name:
            print 'rsp meta not match'
            return None

        rsp = _parse_message(pb_buf[8 + meta_len:8 + meta_len + pb_msg_len],
                        response_class)
        if rsp is None:
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
                result = self.parse_message(pb_buf)
                if result is None:
                    print 'pb decode error, skip this message'
                    break

                meta_info, service, method, req = result
                controller = RpcController()
                rsp = service.CallMethod(method, controller, req, None)
                serialized_rsp = _serialize_message(meta_info, rsp)
                socket.send(serialized_rsp)

            if cur_index > 0:
                content = content[cur_index:]

        print addr, 'has disconnected'

    def parse_message(self, buf):
        result = parse_meta(buf)
        if result is None:
            return None
        meta_len, pb_msg_len, meta_info = result

        # try to find the service
        try:
            service = self._services[meta_info.service_name]
        except KeyError:
            print 'cannot find the service', meta_info.service_name
            return None

        method = service.GetDescriptor().FindMethodByName(meta_info.method_name)
        if method is None:
            print 'cannot find the method', meta_info.method_name
            return None

        msg = _parse_message(buf[8 + meta_len:8 + meta_len + pb_msg_len],
                        service.GetRequestClass(method))
        if msg is None:
            return None
        else:
            return meta_info, service, method, msg

    def register_service(self, service):
        self._services[service.GetDescriptor().full_name] = service

    def run(self):
        self._stream_server.serve_forever()



