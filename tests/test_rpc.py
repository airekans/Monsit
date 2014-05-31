from monsit import rpc
import unittest
from test_proto import test_pb2
from monsit.proto import rpc_meta_pb2
import gevent


class FakeTcpSocket(object):
    def __init__(self, is_client=True):
        self.__recv_content = ""
        self.__send_content = ""
        self.__is_connected = False
        self.__is_client = is_client

    def set_is_client(self, is_client):
        self.__is_client = is_client

    def connect(self, addr):
        self.__is_connected = True

    def close(self):
        self.__is_connected = False

    def setsockopt(self, *args):
        pass

    def is_connected(self):
        return self.__is_connected

    def recv(self, size):
        if self.__is_client:
            while len(self.__send_content) == 0: # not recv anything
                gevent.sleep(0)

        if len(self.__recv_content) == 0:
            if not self.__is_client:
                while self.__is_connected:
                    gevent.sleep(1)
            return ""

        buf = self.__recv_content[:size]
        self.__recv_content = self.__recv_content[size:]
        return buf

    def send(self, buf):
        self.__send_content += buf
        return len(buf)

    def set_recv_content(self, recv_content):
        self.__recv_content = recv_content

    def get_send_content(self):
        return self.__send_content


def fake_spawn(func, *args, **kwargs):
    func(*args, **kwargs)


class FakeTcpConnection(rpc.TcpConnection):
    def __init__(self, addr, recv_content):
        rpc.TcpConnection.__init__(self, addr, FakeTcpSocket, spawn=fake_spawn)

    def get_socket(self):
        return self._socket


class FakeTcpChannel(rpc.TcpChannel):

    def __init__(self, addr, spawn, recv_content=''):
        rpc.TcpChannel.__init__(self, addr,
                                lambda ad, _spawn: FakeTcpConnection(ad, recv_content))
        self.socket = self._connections[0].get_socket()
        if recv_content:
            self.socket.set_recv_content(recv_content)

    def get_connections(self):
        return self._connections

    def get_socket(self):
        return self.socket

    def get_flow_id(self):
        return self._flow_id


class TcpChannelTest(unittest.TestCase):

    def setUp(self):
        self.channel = FakeTcpChannel('127.0.0.1:11111', "")
        self.assertTrue(self.channel.get_socket().is_connected())

        self.service_stub = test_pb2.TestService_Stub(self.channel)
        self.method = None
        for method in self.service_stub.GetDescriptor().methods:
            self.method = method
            break

        self.service_descriptor = self.method.containing_service
        request_class = self.service_stub.GetRequestClass(self.method)
        self.request_class = request_class
        self.request = request_class(name='test', num=123)
        self.response_class = self.service_stub.GetResponseClass(self.method)

    def tearDown(self):
        sock = self.channel.get_socket()
        self.channel.close()
        self.assertFalse(sock.is_connected())

    def get_serialize_message(self, flow_id, msg):
        meta_info = rpc_meta_pb2.MetaInfo(flow_id=flow_id, service_name=self.service_descriptor.full_name,
                                          method_name=self.method.name)
        return rpc._serialize_message(meta_info, msg)

    def test_CallMethod(self):
        channel = self.channel
        self.assertEqual(0, channel.get_flow_id())

        serialized_request = self.get_serialize_message(0, self.request)
        rsp = self.response_class(return_code=0, msg='SUCCESS')
        serialized_response = self.get_serialize_message(0, rsp)
        channel.get_socket().set_recv_content(serialized_response)

        controller = rpc.RpcController()
        actual_rsp = channel.CallMethod(self.method, controller,
                                        self.request, self.response_class, None)

        self.assertEqual(serialized_request, channel.get_socket().get_send_content())
        self.assertEqual(1, channel.get_flow_id())
        self.assertEqual(rsp, actual_rsp, str(actual_rsp))

    def test_CallMethodTimeout(self):
        channel = self.channel
        self.assertEqual(0, channel.get_flow_id())
        channel.get_socket().set_is_client(False)

        serialized_request = self.get_serialize_message(0, self.request)

        controller = rpc.RpcController(method_timeout=1)
        actual_rsp = channel.CallMethod(self.method, controller,
                                        self.request, self.response_class, None)

        self.assertEqual(serialized_request, channel.get_socket().get_send_content())
        self.assertEqual(1, channel.get_flow_id())
        self.assertIsNone(actual_rsp)
        self.assertTrue(controller.Failed())
        self.assertEqual(rpc.RpcController.SERVICE_TIMEOUT, controller.err_code)

    def test_CallMethodWithEmptyBuffer(self):
        channel = self.channel
        self.assertEqual(0, channel.get_flow_id())

        serialized_request = self.get_serialize_message(0, self.request)
        channel.get_socket().set_recv_content('')

        controller = rpc.RpcController()
        actual_rsp = channel.CallMethod(self.method, controller,
                                        self.request, self.response_class, None)

        self.assertEqual(serialized_request, channel.get_socket().get_send_content())
        self.assertEqual(1, channel.get_flow_id())
        self.assertIsNone(actual_rsp)
        self.assertTrue(controller.Failed())
        self.assertEqual(rpc.RpcController.SERVER_CLOSE_CONN_ERROR, controller.err_code)

    def test_CallMethodWithBufferNotStartsWithPb(self):
        channel = self.channel
        self.assertEqual(0, channel.get_flow_id())

        serialized_request = self.get_serialize_message(0, self.request)
        channel.get_socket().set_recv_content('AB1231')

        controller = rpc.RpcController()
        actual_rsp = channel.CallMethod(self.method, controller,
                                        self.request, self.response_class, None)

        self.assertEqual(serialized_request, channel.get_socket().get_send_content())
        self.assertEqual(1, channel.get_flow_id())
        self.assertIsNone(actual_rsp)
        self.assertTrue(controller.Failed())
        self.assertEqual(rpc.RpcController.SERVER_CLOSE_CONN_ERROR, controller.err_code)

    def test_CallMethodWithWrongFlowId(self):
        channel = self.channel
        self.assertEqual(0, channel.get_flow_id())

        serialized_request = self.get_serialize_message(0, self.request)
        rsp = self.response_class(return_code=0, msg='SUCCESS')
        serialized_response = self.get_serialize_message(2, rsp)
        channel.get_socket().set_recv_content(serialized_response)

        controller = rpc.RpcController()
        actual_rsp = channel.CallMethod(self.method, controller,
                                        self.request, self.response_class, None)

        self.assertEqual(serialized_request, channel.get_socket().get_send_content())
        self.assertEqual(1, channel.get_flow_id())
        self.assertIsNone(actual_rsp)
        self.assertTrue(controller.Failed())
        self.assertEqual(rpc.RpcController.SERVER_CLOSE_CONN_ERROR, controller.err_code)

    def test_CallMethodWithWrongMetaInfo(self):
        channel = self.channel
        self.assertEqual(0, channel.get_flow_id())

        serialized_request = self.get_serialize_message(0, self.request)
        rsp = self.response_class(return_code=0, msg='SUCCESS')
        meta_info = rpc_meta_pb2.MetaInfo(flow_id=0,
                                          service_name='WrongServiceName',
                                          method_name='WrongMethodName')
        serialized_response = rpc._serialize_message(meta_info, rsp)
        channel.get_socket().set_recv_content(serialized_response)

        controller = rpc.RpcController()
        actual_rsp = channel.CallMethod(self.method, controller,
                                        self.request, self.response_class, None)

        self.assertEqual(serialized_request, channel.get_socket().get_send_content())
        self.assertEqual(1, channel.get_flow_id())
        self.assertIsNone(actual_rsp)
        self.assertTrue(controller.Failed())
        self.assertEqual(rpc.RpcController.WRONG_RSP_META_ERROR, controller.err_code)

    def test_CallMethodWithErrorInRspMeta(self):
        channel = self.channel
        self.assertEqual(0, channel.get_flow_id())

        serialized_request = self.get_serialize_message(0, self.request)
        rsp = rpc_meta_pb2.ErrorResponse(err_code=rpc_meta_pb2.SERVER_SERVICE_ERROR,
                                         err_msg='test error')
        meta_info = rpc_meta_pb2.MetaInfo(flow_id=0,
                                          service_name=self.service_descriptor.full_name,
                                          method_name=self.method.name,
                                          has_error=True)
        serialized_response = rpc._serialize_message(meta_info, rsp)
        channel.get_socket().set_recv_content(serialized_response)

        controller = rpc.RpcController()
        actual_rsp = channel.CallMethod(self.method, controller,
                                        self.request, self.response_class, None)

        self.assertEqual(serialized_request, channel.get_socket().get_send_content())
        self.assertEqual(1, channel.get_flow_id())
        self.assertIsNone(actual_rsp)
        self.assertTrue(controller.Failed())
        self.assertEqual(rpc_meta_pb2.SERVER_SERVICE_ERROR, controller.err_code)

    def test_CallMethodAsync(self):
        channel = self.channel
        self.assertEqual(0, channel.get_flow_id())

        serialized_request = self.get_serialize_message(0, self.request)
        rsp = self.response_class(return_code=0, msg='SUCCESS')
        serialized_response = self.get_serialize_message(0, rsp)
        channel.get_socket().set_recv_content(serialized_response)

        controller = rpc.RpcController()
        actual_rsp = []
        done = lambda ctrl, rsp: actual_rsp.append(rsp)
        result = channel.CallMethod(self.method, controller,
                                    self.request, self.response_class, done)
        self.assertIsNone(result)
        self.assertEqual(0, len(actual_rsp))
        gevent.sleep(1)

        self.assertEqual(serialized_request, channel.get_socket().get_send_content())
        self.assertEqual(1, channel.get_flow_id())
        self.assertEqual(1, len(actual_rsp))
        self.assertEqual(rsp, actual_rsp[0], str(actual_rsp))
        self.assertFalse(controller.Failed())

    def test_resolve_addr_with_single_addr(self):
        expected_addrs = [('127.0.0.1', 30012)]
        addrs = self.channel.resolve_addr('127.0.0.1:30012')
        self.assertEqual(expected_addrs, addrs)

        addrs = self.channel.resolve_addr('ip/127.0.0.1:30012')
        self.assertEqual(expected_addrs, addrs)

    def test_resolve_addr_with_multiple_addrs(self):
        expected_addrs = [('127.0.0.1', 30012), ('192.168.1.1', 30021)]
        addrs = self.channel.resolve_addr(['127.0.0.1:30012',
                                           '192.168.1.1:30021'])
        self.assertEqual(expected_addrs, addrs)

        addrs = self.channel.resolve_addr(['ip/127.0.0.1:30012',
                                           'ip/192.168.1.1:30021'])
        self.assertEqual(expected_addrs, addrs)

    def test_resolve_addr_with_wrong_addr(self):
        addrs = self.channel.resolve_addr('127.0.0.1')
        self.assertIsNone(addrs)


class RpcClientTest(unittest.TestCase):

    def setUp(self):
        rpc.RpcClient.tcp_channel_class = FakeTcpChannel
        self.client = rpc.RpcClient()

    def test_get_tcp_channel_with_one_ip(self):
        test_addr = '127.0.0.1:30002'
        channel1 = self.client.get_tcp_channel(test_addr)
        channel2 = self.client.get_tcp_channel(test_addr)
        self.assertIs(channel1, channel2)

    def test_get_tcp_channel_with_multiple_ips(self):
        test_addrs = ('127.0.0.1:30002', '192.168.1.12:30003')
        channel1 = self.client.get_tcp_channel(test_addrs)
        channel2 = self.client.get_tcp_channel(test_addrs)
        self.assertIs(channel1, channel2)


class FakeRpcServer(rpc.RpcServer):

    def __init__(self, service_timeout=10):
        self._addr = ('127.0.0.1', 12345)
        self._services = {}
        self._service_timeout = service_timeout
        self._stat = rpc.RpcServerStat()
        # not calling parent's __init__ to bypass the StreamServer init

    def set_service_timeout(self, service_timeout):
        self._service_timeout = service_timeout

    def get_service(self, name):
        return self._services[name]

    def run(self):
        pass

    def handle_connection(self, socket, addr):
        self._handle_connection(socket, addr)


class FakeTestService(test_pb2.TestService):
    def __init__(self, is_async, rsp, sleep_time=1):
        self.is_async = is_async
        self.rsp = rsp
        self.sleep_time = sleep_time

    def TestMethod(self, rpc_controller, request, done):
        if self.is_async:
            gevent.sleep(self.sleep_time)

        return self.rsp


class RpcServerTest(unittest.TestCase):

    def setUp(self):
        self.server = FakeRpcServer()

        self.service = test_pb2.TestService()
        self.method = None
        for method in self.service.GetDescriptor().methods:
            self.method = method
            break

        self.service_descriptor = self.service.GetDescriptor()

    def get_serialize_message(self, flow_id, service_desc, method_name, msg):
        has_error = isinstance(msg, rpc_meta_pb2.ErrorResponse)
        meta_info = rpc_meta_pb2.MetaInfo(flow_id=flow_id, service_name=service_desc.full_name,
                                          method_name=method_name, has_error=has_error)
        return rpc._serialize_message(meta_info, msg)

    def test_register_service(self):
        service_name = self.service_descriptor.full_name
        self.server.register_service(self.service)

        self.assertIs(self.service, self.server.get_service(service_name))

    def test_parse_message_with_empty_buf(self):
        self.assertIsNone(self.server.parse_message(''))

    def test_parse_message_with_non_reg_service(self):
        req = test_pb2.TestRequest(name='abc', num=1)
        serialized_req = self.get_serialize_message(1, self.service_descriptor,
                                                    self.method.name, req)
        self.assertIsNone(self.server.parse_message(serialized_req[6:]))

    def test_parse_message_with_wrong_method(self):
        self.server.register_service(self.service)

        req = test_pb2.TestRequest(name='abc', num=1)
        serialized_req = self.get_serialize_message(1, self.service_descriptor,
                                                    'WrongMethodName', req)
        self.assertIsNone(self.server.parse_message(serialized_req[6:]))

    def test_parse_message_with_wrong_msg(self):
        self.server.register_service(self.service)

        req = test_pb2.TestRequest(name='abc', num=1)
        serialized_req = self.get_serialize_message(1, self.service_descriptor,
                                                    self.method.name, req)
        wrong_req = serialized_req[:-3] + 'abc'
        self.assertIsNone(self.server.parse_message(wrong_req[6:]))

    def test_parse_message(self):
        self.server.register_service(self.service)

        req = test_pb2.TestRequest(name='abc', num=1)
        serialized_req = self.get_serialize_message(1, self.service_descriptor,
                                                    self.method.name, req)
        result = self.server.parse_message(serialized_req[6:])
        self.assertIsNotNone(result)
        self.assertTrue(len(result) == 4)
        meta_info, service, method, actual_req = result
        self.assertIs(self.service, service)
        self.assertIs(self.method, method)
        self.assertEqual(req, actual_req)

    def test_handle_connection(self):
        rsp = test_pb2.TestResponse(return_code=0, msg='SUCCESS')
        service = FakeTestService(False, rsp)
        self.server.register_service(service)

        req = test_pb2.TestRequest(name='abc', num=1)
        serialized_req = self.get_serialize_message(1, self.service_descriptor,
                                                    self.method.name, req)
        serialized_rsp = self.get_serialize_message(1, self.service_descriptor,
                                                    self.method.name, rsp)
        socket = FakeTcpSocket(is_client=False)
        socket.connect(('127.0.0.1', 34567))
        socket.set_recv_content(serialized_req)

        t = gevent.spawn(self.server.handle_connection, socket, ('127.0.0.1', 34567))
        gevent.sleep(1)
        actual_serialized_rsp = socket.get_send_content()
        self.assertEqual(serialized_rsp, actual_serialized_rsp)

        socket.close()
        t.join()

    def test_handle_connection_async(self):
        rsp = test_pb2.TestResponse(return_code=0, msg='SUCCESS')
        service = FakeTestService(True, rsp)
        self.server.register_service(service)

        req = test_pb2.TestRequest(name='abc', num=1)
        serialized_req = self.get_serialize_message(1, self.service_descriptor,
                                                    self.method.name, req)
        serialized_rsp = self.get_serialize_message(1, self.service_descriptor,
                                                    self.method.name, rsp)
        socket = FakeTcpSocket(is_client=False)
        socket.connect(('127.0.0.1', 34567))
        socket.set_recv_content(serialized_req + serialized_req) # 2 requests

        t = gevent.spawn(self.server.handle_connection, socket, ('127.0.0.1', 34567))
        gevent.sleep(1)
        self.assertEqual("", socket.get_send_content())
        gevent.sleep(1)
        socket.close()
        t.join()

        actual_serialized_rsp = socket.get_send_content()
        self.assertEqual(serialized_rsp + serialized_rsp, actual_serialized_rsp)

    def test_handle_connection_timeout(self):
        self.server.set_service_timeout(1)
        rsp = test_pb2.TestResponse(return_code=0, msg='SUCCESS')
        service = FakeTestService(True, rsp, sleep_time=3)
        self.server.register_service(service)

        req = test_pb2.TestRequest(name='abc', num=1)
        serialized_req = self.get_serialize_message(1, self.service_descriptor,
                                                    self.method.name, req)
        err_rsp = rpc_meta_pb2.ErrorResponse(err_code=rpc_meta_pb2.SERVER_SERVICE_TIMEOUT,
                                             err_msg='service timeout')
        serialized_rsp = self.get_serialize_message(1, self.service_descriptor,
                                                    self.method.name, err_rsp)
        socket = FakeTcpSocket(is_client=False)
        socket.connect(('127.0.0.1', 34567))
        socket.set_recv_content(serialized_req)

        t = gevent.spawn(self.server.handle_connection, socket, ('127.0.0.1', 34567))
        gevent.sleep(2)
        actual_serialized_rsp = socket.get_send_content()
        self.assertEqual(serialized_rsp, actual_serialized_rsp)

        socket.close()
        t.join()

if __name__ == '__main__':
    unittest.main()