import struct
import google.protobuf.service
from google.protobuf import message

import gevent.server
import gevent.socket
import gevent.queue
import gevent.event
import gevent

from monsit.proto import rpc_meta_pb2


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
    def __init__(self, addr, socket_cls=gevent.socket.socket):
        google.protobuf.service.RpcChannel.__init__(self)
        self._flow_id = 0
        self._addr = addr
        self._socket = socket_cls()
        self.connect()

        self._send_task_queue = gevent.queue.Queue()
        self._recv_infos = {}
        self._workers = [gevent.spawn(self.send_loop), gevent.spawn(self.recv_loop)]

    def __del__(self):
        self.close()

    def _get_flow_id(self):
        flow_id = self._flow_id
        self._flow_id += 1
        return flow_id

    def connect(self):
        self._socket.connect(self._addr)

    def close(self):
        self._socket.close()
        gevent.killall(self._workers)

    def _recv(self, expected_size):
        recv_buf = ''
        while len(recv_buf) < expected_size:
            try:
                buf = self._socket.recv(expected_size - len(recv_buf))
                if len(buf) == 0:
                    break

                recv_buf += buf
            except Exception, e:
                print e
                break

        return recv_buf

    def recv_loop(self):
        expected_size = 2 + struct.calcsize("!I")
        while True:
            if not self.recv_rsp():
                break

        # check if there is any request has not been processed.
        for v in self._recv_infos.itervalues():
            expected_meta, rpc_controller, response_class, done = v
            rpc_controller.SetFailed('channel has been closed prematurely')
            done(None)

    def send_loop(self):
        while True:
            send_task = self._send_task_queue.get()
            send_task()

    def send_req(self, method_descriptor, rpc_controller, request, response_class, done):
        service_descriptor = method_descriptor.containing_service
        flow_id = self._get_flow_id()
        meta_info = rpc_meta_pb2.MetaInfo()
        meta_info.flow_id = flow_id
        meta_info.service_name = service_descriptor.full_name
        meta_info.method_name = method_descriptor.name
        serialized_req = _serialize_message(meta_info, request)
        self._socket.send(serialized_req)
        self._recv_infos[flow_id] = meta_info, rpc_controller, response_class, done

    # return True when the connection is not closed
    # return False when it's closed
    def recv_rsp(self):
        expected_size = 2 + struct.calcsize("!I")
        pb_buf = self._recv(expected_size)
        if len(pb_buf) == 0:
            print 'socket has been closed'
            return False
        if len(pb_buf) < expected_size:
            print 'rsp buffer broken'
            return True
        if pb_buf[:2] != 'PB':
            print 'rsp buffer not begin with PB'
            return True

        buf_size = struct.unpack("!I", pb_buf[2:])[0]
        pb_buf = self._recv(buf_size)
        if len(pb_buf) == 0:
            print 'socket has been closed'
            return False

        result = parse_meta(pb_buf)
        if result is None:
            print 'pb decode error, skip this message'
            return True

        meta_len, pb_msg_len, meta_info = result

        if meta_info.flow_id in self._recv_infos:
            expected_meta, rpc_controller, response_class, done = \
                self._recv_infos[meta_info.flow_id]

            if meta_info.flow_id != expected_meta.flow_id:
                rpc_controller.SetFailed('rsp flow id not match')
                print 'rsp flow id not match:', expected_meta.flow_id, meta_info.flow_id
                done(None)
                return True
            elif meta_info.service_name != expected_meta.service_name or \
                            meta_info.method_name != expected_meta.method_name or \
                            meta_info.msg_name != response_class.DESCRIPTOR.full_name:
                rpc_controller.SetFailed('rsp meta not match')
                print 'rsp meta not match'
                done(None)
                return True

            rsp = _parse_message(pb_buf[8 + meta_len:8 + meta_len + pb_msg_len],
                                 response_class)
            del self._recv_infos[meta_info.flow_id]
            done(rsp)
            return True
        else:
            print 'flow id not found:', meta_info.flow_id
            return True

    # when done is None, it means the method call is synchronous
    # when it's not None, it means the call is asynchronous
    def CallMethod(self, method_descriptor, rpc_controller,
                   request, response_class, done):
        if done is None:
            res = gevent.event.AsyncResult()
            done = lambda rsp: res.set(rsp)
            self._send_task_queue.put_nowait(lambda: self.send_req(method_descriptor, rpc_controller,
                                                                   request, response_class, done))
            return res.get()
        else:
            self._send_task_queue.put_nowait(lambda: self.send_req(method_descriptor, rpc_controller,
                                                                   request, response_class, done))
            return None


class RpcServer(object):
    def __init__(self, addr):
        self._addr = addr
        self._services = {}
        self._stream_server = gevent.server.StreamServer(addr, self._handle_connection)

    def _handle_connection(self, socket, addr):
        rsp_queue = gevent.queue.Queue()
        is_connection_closed = [False]

        def call_service(req_info):
            meta_info, service, method, req = req_info
            controller = RpcController()
            rsp = service.CallMethod(method, controller, req, None)
            rsp_queue.put_nowait((meta_info, rsp))

        def recv_req():
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

                    gevent.spawn(lambda: call_service(result))

                if cur_index > 0:
                    content = content[cur_index:]

            print addr, 'has disconnected'
            is_connection_closed[0] = True

        def send_rsp():
            while not is_connection_closed[0]:
                meta_info, rsp = rsp_queue.get()
                serialized_rsp = _serialize_message(meta_info, rsp)
                socket.send(serialized_rsp)

        workers = [gevent.spawn(recv_req), gevent.spawn(send_rsp)]
        gevent.joinall(workers)

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



