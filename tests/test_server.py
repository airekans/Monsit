from monsit import rpc
from test_proto import test_pb2


class TestServiceImpl(test_pb2.TestService):
    def TestMethod(self, rpc_controller, request, done):
        rsp = test_pb2.TestResponse(return_code=0, msg='SUCCESS')
        return rsp


def main():
    server = rpc.RpcServer(('0.0.0.0', 54321))
    server.register_service(TestServiceImpl())
    try:
        server.run(print_stat_interval=60)
    except KeyboardInterrupt:
        print 'server got SIGINT, exit.'


if __name__ == '__main__':
    main()
