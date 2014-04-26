from monsit import rpc
import unittest
from test_proto import test_pb2
from monsit.proto import rpc_meta_pb2
import gevent


class FakeTcpSocket(object):
    def __init__(self):
        self.__recv_content = ""
        self.__send_content = ""
        self.__is_connected = False

    def connect(self, addr):
        self.__is_connected = True

    def close(self):
        self.__is_connected = False

    def is_connected(self):
        return self.__is_connected

    def recv(self, size):
        while len(self.__send_content) == 0: # not recv anything
            gevent.sleep(0)

        if len(self.__recv_content) == 0:
            return ""

        buf = self.__recv_content[:size]
        self.__recv_content = self.__recv_content[size:]
        return buf

    def send(self, buf):
        self.__send_content += buf

    def set_recv_content(self, recv_content):
        self.__recv_content = recv_content

    def get_send_content(self):
        return self.__send_content


class FakeTcpChannel(rpc.TcpChannel):

    def __init__(self, addr, recv_content):
        rpc.TcpChannel.__init__(self, addr, FakeTcpSocket)
        self._socket.set_recv_content(recv_content)

    def get_socket(self):
        return self._socket

    def get_flow_id(self):
        return self._flow_id


class TcpChannelTest(unittest.TestCase):

    def setUp(self):
        self.channel = FakeTcpChannel(('127.0.0.1', 11111), "")
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


if __name__ == '__main__':
    unittest.main()