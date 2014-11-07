from gevent import monkey
monkey.patch_all()

from monsit import db
from recall.server import RpcServer
from monsit.proto import monsit_pb2
import datetime
import json


class MonsitServiceImpl(monsit_pb2.MonsitService):

    def __init__(self):
        with db.DBConnection() as cnx:
            registered_hosts = {}
            registered_host_ids = {}
            for host_id, host_name in cnx.get_all_hosts():
                registered_hosts[host_name] = host_id
                registered_host_ids[host_id] = [host_name, False]

            self.__registered_host_names = registered_hosts
            self.__registered_host_ids = registered_host_ids

    def Register(self, rpc_controller, request, done):
        try:
            host_id = self.__registered_host_names[request.host_name]
            self.__registered_host_ids[host_id][1] = True
        except KeyError:
            with db.DBConnection() as cnx:
                host_id, host_name = cnx.insert_new_host(request.host_name)
                self.__registered_host_names[request.host_name] = host_id
                self.__registered_host_ids[host_id] = [request.host_name, True]

        print request

        rsp = monsit_pb2.RegisterResponse(return_code=0, msg='SUCCESS',
                                          host_id=host_id)
        return rsp

    def Report(self, rpc_controller, request, done):
        if request.host_id not in self.__registered_host_ids:
            print 'Host not registered:', request.host_id
            rsp = monsit_pb2.ReportResponse(return_code=1, msg='Host not registered')
            return rsp
        elif not self.__registered_host_ids[request.host_id][1]:
            host_name = self.__registered_host_ids[request.host_id][0]
            print 'Host not registered:', host_name
            rsp = monsit_pb2.ReportResponse(return_code=1, msg='Host not registered')
            return rsp

        print request

        # insert the connection time to the info
        connection_info = {'connected': True, 'datetime': request.datetime}
        basic_info = request.basic_infos.add()
        basic_info.id = 1
        basic_info.info = json.dumps(connection_info, separators=(',', ':'))

        with db.DBConnection() as cnx:
            report_time = datetime.datetime.fromtimestamp(request.datetime)
            report_time = report_time.strftime("%Y-%m-%d %H:%M:%S")
            cnx.insert_stat(request, report_time)
            cnx.update_info(request)
            cnx.commit()

        rsp = monsit_pb2.ReportResponse(return_code=0, msg='SUCCESS')
        return rsp


if __name__ == '__main__':
    db.init()

    service = MonsitServiceImpl()
    rpc_server = RpcServer(('0.0.0.0', 30002))
    rpc_server.register_service(service)
    try:
        rpc_server.run(print_stat_interval=60)
    except KeyboardInterrupt:
        print 'monsit got SIGINT, exit.'
