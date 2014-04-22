from gevent import monkey
monkey.patch_all()

from monsit import db, rpc
from monsit.proto import simple_pb2


class MonsitServiceImpl(simple_pb2.MonsitService):

    def __init__(self):
        with db.DBConnection() as cnx:
            registered_hosts = {}
            for host in cnx.get_all_hosts():
                registered_hosts[host[1]] = [host[0], False]

            self.__registered_hosts = registered_hosts

    def Register(self, rpc_controller, request, done):
        try:
            host_info = self.__registered_hosts[request.host_name]
            host_info[1] = True
        except KeyError:
            with db.DBConnection() as cnx:
                host_info = cnx.insert_new_host(request.host_name)
                self.__registered_hosts[request.host_name] = [host_info[0], True]

        rsp = simple_pb2.RegisterResponse(return_code=0, msg='SUCCESS')
        return rsp

    def Report(self, rpc_controller, request, done):
        if request.host_name not in self.__registered_hosts or \
           not self.__registered_hosts[request.host_name][1]:
            print 'Host not registered:', request.host_name
            rsp = simple_pb2.SimpleResponse(return_code=1, msg='Host not registered')
            return rsp

        print request

        if len(request.net_infos) > 0:
            with db.DBConnection() as cnx:
                host_id = self.__registered_hosts[request.host_name][0]
                cnx.create_host_tables(host_id)
                if not cnx.insert_host_info(request, host_id):
                    print 'failed to store req in db'

        rsp = simple_pb2.SimpleResponse(return_code=0, msg='SUCCESS')
        return rsp


if __name__ == '__main__':
    db.init()

    service = MonsitServiceImpl()
    rpc_server = rpc.RpcServer(('0.0.0.0', 30002))
    rpc_server.register_service(service)
    try:
        rpc_server.run()
    except KeyboardInterrupt:
        print 'monsit got SIGINT, exit.'
