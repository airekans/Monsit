from gevent import monkey
monkey.patch_all()

import gevent

from monsit import db
from recall.server import RpcServer
from monsit.proto import monsit_pb2
from monsit.queue import PriorityQueue

import datetime
import time
import json
import smtplib
from email.mime.text import MIMEText
import optparse


_workers = []
_mail_server = "localhost"
_from_domain = "monsit-example.com"


def spawn(func, *args, **kwargs):
    _workers.append(gevent.spawn(func, *args, **kwargs))


def kill_all_workers():
    gevent.killall(_workers)


def send_alarm_email(host_id, alarm_setting):
    from_addr = "monsit-noreply@" + _from_domain
    to_addrs = alarm_setting['emails']
    to_addrs = to_addrs.split(";")
    mail_text = alarm_setting['message']
    msg = MIMEText(mail_text)
    msg['Subject'] = 'Host %d failed' % host_id
    msg['From'] = from_addr
    msg['To'] = to_addrs[0]
    msg['CC'] = ";".join(to_addrs[1:])

    server = smtplib.SMTP(_mail_server)
    # server.set_debuglevel(True)
    try:
        server.sendmail(from_addr, to_addrs, msg.as_string())
        print 'sent email from', from_addr, 'to', to_addrs
    except:
        print "Failed to send mail from %s to %s" % (from_addr, to_addrs)
    server.quit()


def check_stat_alarms(stats, alarms):
    for stat in stats.stat:
        stat_id = stat.id
        if stat_id not in alarms:
            continue

        alarm_setting = alarms[stat_id]
        threshold_type = alarm_setting['threshold_type']
        if threshold_type == 'int':
            cmp_func = lambda x: x.num_value >= alarm_setting['threshold']
        elif threshold_type == 'double':
            cmp_func = lambda x: x.double_value >= alarm_setting['threshold']
        elif threshold_type == 'string':
            cmp_func = lambda x: x.str_value != alarm_setting['threshold']
        elif threshold_type == 'json':
            cmp_func = lambda x: x.reserve_value != alarm_setting['threshold']
        else:
            cmp_func = alarm_setting['threshold']

        for y_value in stat.y_axis_value:
            if cmp_func(y_value):
                send_alarm_email(stats.host_id, alarm_setting)


def check_info_alarms(infos, alarms):
    for info in infos.basic_infos:
        if info.id not in alarms:
            continue

        alarm_setting = alarms[info.id]
        threshold_type = alarm_setting['threshold_type']
        if threshold_type == 'string':
            cmp_func = lambda x: x != alarm_setting['threshold']
        elif threshold_type == 'json':
            cmp_func = lambda x: json.loads(x) != alarm_setting['threshold']
        elif threshold_type == 'func':
            cmp_func = alarm_setting['threshold']
        else:
            assert False

        if cmp_func(json.loads(info.info)):
            send_alarm_email(infos.host_id, alarm_setting)


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
            self.__host_time_info = {}
            self.__timeout_queue = PriorityQueue()
            spawn(self._timeout_loop)

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

        self.__host_time_info[host_id] = (30, None)  # TODO: change this

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

#         print request

        # insert the connection time to the info
        connection_info = {'connected': True, 'datetime': request.datetime}
        basic_info = request.basic_infos.add()
        basic_info.id = 1
        basic_info.info = json.dumps(connection_info, separators=(',', ':'))

        # record the report time, and put it to the timeout queue
        report_interval, last_update_time = self.__host_time_info.get(request.host_id)
        timeout_period = (report_interval * 3 + 1) / 2
        new_deadline = request.datetime + timeout_period
        if last_update_time is None:
            self.__timeout_queue.push((new_deadline, request.host_id))
        else:
            last_deadline = last_update_time + timeout_period
            time_index = self.__timeout_queue.find((last_deadline, request.host_id))
            if time_index >= 0:
                self.__timeout_queue.increase_key(time_index, (new_deadline, request.host_id))
            else:
                self.__timeout_queue.push((new_deadline, request.host_id))
        self.__host_time_info[request.host_id] = (report_interval, request.datetime)

        with db.DBConnection() as cnx:
            report_time = datetime.datetime.fromtimestamp(request.datetime)
            report_time = report_time.strftime("%Y-%m-%d %H:%M:%S")
            cnx.insert_stat(request, report_time)
            cnx.update_info(request)
            cnx.commit()

            # check the whether the alarm is fired
            stat_alarms, info_alarms = cnx.get_alarm_settings(request.host_id)
            check_stat_alarms(request, stat_alarms)
            check_info_alarms(request, info_alarms)

        rsp = monsit_pb2.ReportResponse(return_code=0, msg='SUCCESS')
        return rsp

    def _timeout_loop(self):
        while True:
            gevent.sleep(1)
            if not self.__timeout_queue.is_empty():
                now = int(time.time())
                conn = None
                while not self.__timeout_queue.is_empty():
                    deadline, host_id = self.__timeout_queue.get_top()
                    if now < deadline:
                        break
                    elif conn is None:
                        conn = db.DBConnection()

                    print host_id, ' has been timeout.'

                    self.__timeout_queue.pop()
                    # set the host to not connected state
                    report_interval, last_update_time = self.__host_time_info[host_id]
                    self.__host_time_info[host_id] = (report_interval, None)
                    connection_info = {'connected': False, 'datetime': last_update_time}
                    req = monsit_pb2.ReportRequest()
                    req.host_id = host_id
                    basic_info = req.basic_infos.add()
                    basic_info.id = 1
                    basic_info.info = json.dumps(connection_info, separators=(',', ':'))
                    conn.update_info(req)

                    _, info_alarms = conn.get_alarm_settings(host_id)
                    check_info_alarms(req, info_alarms)

                if conn is not None:
                    conn.commit()
                    conn.close()


def main():
    global _mail_server, _from_domain

    optparser = optparse.OptionParser(usage = "%prog [options]")
    optparser.add_option('-p', '--port', dest="master_port",
                         help="Port of the master", type=int, default=30002)
    optparser.add_option('-m', '--mail-server', dest="mail_server",
                         help="The mail server used to send email")
    optparser.add_option('--from-domain', dest="from_domain",
                         help="The from domain used in the email")

    opts, _ = optparser.parse_args()
    master_port = opts.master_port
    if opts.mail_server is not None:
        _mail_server = opts.mail_server
        print 'mail_server', _mail_server
    if opts.from_domain is not None:
        _from_domain = opts.from_domain
        print 'from_domain', _from_domain
    
    db.init()

    service = MonsitServiceImpl()
    master_addr = ('0.0.0.0', master_port)
    rpc_server = RpcServer(master_addr)
    print 'master listen on', master_addr
    rpc_server.register_service(service)
    try:
        rpc_server.run(print_stat_interval=60)
    except KeyboardInterrupt:
        print 'monsit got SIGINT, exit.'
        kill_all_workers()


if __name__ == '__main__':
    main()


