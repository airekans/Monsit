import struct
import google.protobuf.service
from google.protobuf import message

import gevent
import gevent.server
import gevent.socket
import gevent.queue
import gevent.event
import gevent.pool
from gevent import Timeout
from socket import error as soc_error
import logging
import traceback
import time
import heapq
import random

from monsit.proto import rpc_meta_pb2


class Pool(object):
    def __init__(self, pool_size=None):
        self._task_queue = gevent.queue.JoinableQueue()
        self._pool = gevent.pool.Pool(pool_size)
        if pool_size is None:
            pool_size = 100

        for _ in xrange(pool_size):
            self._pool.spawn(self.worker_func)

    def worker_func(self):
        while True:
            task = self._task_queue.get()
            if task is None:
                self._task_queue.task_done()
                break
            task()
            self._task_queue.task_done()

    def spawn(self, func, *args, **kwargs):
        task = lambda: func(*args, **kwargs)
        self._task_queue.put_nowait(task)

    def join(self):
        for _ in xrange(len(self._pool)):
            self._task_queue.put_nowait(None)
        self._task_queue.join()
        self._pool.join()

    def kill(self):
        self._pool.kill()


class RpcController(google.protobuf.service.RpcController):
    SUCCESS = 0
    SERVICE_TIMEOUT = 100
    WRONG_FLOW_ID_ERROR = 101
    WRONG_RSP_META_ERROR = 102
    WRONG_MSG_NAME_ERROR = 103
    SERVER_CLOSE_CONN_ERROR = 104

    def __init__(self, method_timeout=0):
        self.err_code = RpcController.SUCCESS
        self.err_msg = None
        self.method_timeout = method_timeout

    def Reset(self):
        self.err_code = RpcController.SUCCESS
        self.err_msg = None

    def Failed(self):
        return self.err_code != RpcController.SUCCESS

    def ErrorText(self):
        return self.err_msg

    def SetFailed(self, reason):
        self.err_code, self.err_msg = reason


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
        logging.warning('parsing msg meta info failed: ' + str(err))
        return None


def _parse_message(buf, msg_cls):
    msg = msg_cls()
    try:
        msg.ParseFromString(buf)
        return msg
    except message.DecodeError as err:
        logging.warning('parsing msg failed: ' + str(err))
        return None


class LoadBalancerBase(object):
    def get_connection_for_req(self, flow_id, req, conns):
        raise NotImplementedError


class SingleConnLoadBalancer(LoadBalancerBase):
    def get_connection_for_req(self, flow_id, req, conns):
        assert len(conns) == 1
        return conns[0]


class IncLoadBalancer(LoadBalancerBase):
    def get_connection_for_req(self, flow_id, req, conns):
        return conns[flow_id % len(conns)]


class RandomLoadBalancer(LoadBalancerBase):
    def get_connection_for_req(self, flow_id, req, conns):
        return random.choice(conns)


class ReqNumLoadBalancer(LoadBalancerBase):
    def get_conn_req_num(self, conn):
        return conn.get_pending_send_task_num()

    def get_connection_for_req(self, flow_id, req, conns):
        return min(conns, key=self.get_conn_req_num)


class DelayLoadBalancer(LoadBalancerBase):
    def __init__(self, is_random=True):
        self._call_times = 0
        self._is_random = is_random

    def get_conn_avg_delay(self, conn):
        return conn.get_avg_delay_per_min()

    def get_connection_for_req(self, flow_id, req, conns):
        if self._is_random and self._call_times == 0:
            conn = random.choice(conns)
        else:
            conn = min(conns, key=self.get_conn_avg_delay)

        self._call_times = (self._call_times + 1) % 100
        return conn


class TcpConnectionStat(object):
    def __init__(self):
        self.total_req_num_per_min = 0
        self.total_rsp_num_per_min = 0
        self.total_delay_s_per_min = 0
        self.max_delay_s_per_min = -1
        self.min_delay_s_per_min = -1

    def add_req_stat(self, count=1):
        self.total_req_num_per_min += count

    def add_rsp_stat(self, count, delay_s):
        self.total_rsp_num_per_min += count
        self.total_delay_s_per_min = delay_s

        if delay_s > self.max_delay_s_per_min:
            self.max_delay_s_per_min = delay_s

        if self.min_delay_s_per_min < 0 or delay_s < self.min_delay_s_per_min:
            self.min_delay_s_per_min = delay_s

    @property
    def avg_delay_s_per_min(self):
        if self.total_rsp_num_per_min <= 0:
            return -1
        else:
            return self.total_delay_s_per_min / self.total_rsp_num_per_min

    def reset_stat_per_min(self):
        self.total_req_num_per_min = 0
        self.total_rsp_num_per_min = 0
        self.total_delay_s_per_min = 0
        self.min_delay_s_per_min = -1
        self.max_delay_s_per_min = -1


class TcpConnection(object):

    class Exception(Exception):
        def __init__(self, err_code, err_msg):
            self.err_code, self.err_msg = err_code, err_msg

    class _RequestData(object):
        def __init__(self, is_async):
            self.begin_time = 0
            self.is_async = is_async

    def __init__(self, addr, socket_cls=gevent.socket.socket, spawn=gevent.spawn):
        self._addr = addr
        self._spawn = spawn
        self._socket = socket_cls()
        self.connect()

        self._send_task_queue = gevent.queue.Queue()
        self._recv_infos = {}
        self._timeout_queue = []
        self._workers = [gevent.spawn(self.send_loop), gevent.spawn(self.recv_loop),
                         gevent.spawn(self.timeout_loop)]

        self._stat = TcpConnectionStat()

    def connect(self):
        self._socket.connect(self._addr)
        self._socket.setsockopt(gevent.socket.SOL_TCP, gevent.socket.TCP_NODELAY, 1)
        self._socket.setsockopt(gevent.socket.IPPROTO_TCP, gevent.socket.TCP_NODELAY, 1)

    def close(self):
        self._socket.close()
        gevent.killall(self._workers)

    def get_stat(self):
        return self._stat

    def get_avg_delay_per_min(self):
        return self.get_stat().avg_delay_s_per_min

    def get_pending_send_task_num(self):
        return self._send_task_queue.qsize()

    def add_send_task(self, flow_id, method_descriptor, rpc_controller,
                      request, response_class, done, req_data):
        self._send_task_queue.put_nowait(
            lambda: self.send_req(flow_id, method_descriptor, rpc_controller,
                                  request, response_class, done, req_data))

    def _finish_rpc(self, done, controller, rsp, is_async):
        if is_async:
            self._spawn(done, controller, rsp)
        else:
            done(controller, rsp)

    def timeout_loop(self):
        while True:
            gevent.sleep(1)
            if len(self._timeout_queue) > 0:
                now = time.time()
                while len(self._timeout_queue) > 0:
                    deadline, flow_id = self._timeout_queue[0]  # the smallest element
                    if deadline > now:
                        break
                    else:
                        heapq.heappop(self._timeout_queue)
                        meta_info, rpc_controller, response_class, done, req_data = \
                            self._recv_infos[flow_id]
                        timeout_time = time.time()
                        self._stat.add_rsp_stat(1, timeout_time - req_data.begin_time)
                        err_msg = 'service timeout'
                        rpc_controller.SetFailed((RpcController.SERVICE_TIMEOUT, err_msg))
                        logging.warning(err_msg)
                        del self._recv_infos[flow_id]
                        self._finish_rpc(done, rpc_controller, None, req_data.is_async)

    def recv_loop(self):
        while True:
            if not self.recv_rsp():
                break

        # check if there is any request has not been processed.
        for v in self._recv_infos.itervalues():
            expected_meta, rpc_controller, response_class, done, req_data = v
            rpc_controller.SetFailed((RpcController.SERVER_CLOSE_CONN_ERROR,
                                      'channel has been closed prematurely'))
            self._finish_rpc(done, rpc_controller, None, req_data.is_async)

        self._recv_infos.clear()

    def send_loop(self):
        while True:
            send_task = self._send_task_queue.get()
            send_task()

    def send_req(self, flow_id, method_descriptor, rpc_controller,
                 request, response_class, done, req_data):
        service_descriptor = method_descriptor.containing_service
        meta_info = rpc_meta_pb2.MetaInfo()
        meta_info.flow_id = flow_id
        meta_info.service_name = service_descriptor.full_name
        meta_info.method_name = method_descriptor.name
        serialized_req = _serialize_message(meta_info, request)
        sent_bytes = 0
        try:
            while sent_bytes < len(serialized_req):
                sent_bytes += self._socket.send(serialized_req[sent_bytes:])
        except soc_error as e:
            logging.warning('socket error: ' + e)
            return

        req_data.begin_time = time.time()
        self._recv_infos[flow_id] = (meta_info, rpc_controller, response_class, done, req_data)
        self._stat.add_req_stat()
        if rpc_controller.method_timeout > 0:
            heapq.heappush(self._timeout_queue,
                           (req_data.begin_time + rpc_controller.method_timeout, flow_id))

    def _recv(self, expected_size):
        recv_buf = ''
        while len(recv_buf) < expected_size:
            try:
                buf = self._socket.recv(expected_size - len(recv_buf))
                if len(buf) == 0:
                    break

                recv_buf += buf
            except Exception, e:
                logging.warning('recv failed: ' + e)
                break

        return recv_buf

    _expected_size = 2 + struct.calcsize("!I")
    _error_msg_name = rpc_meta_pb2.ErrorResponse.DESCRIPTOR.full_name

    # return True when the connection is not closed
    # return False when it's closed
    def recv_rsp(self):
        expected_size = TcpConnection._expected_size
        pb_buf = self._recv(expected_size)
        if len(pb_buf) == 0:
            logging.info('socket has been closed')
            return False
        if len(pb_buf) < expected_size:
            logging.warning('rsp buffer broken')
            return True
        if pb_buf[:2] != 'PB':
            logging.warning('rsp buffer not begin with PB')
            return True

        buf_size = struct.unpack("!I", pb_buf[2:])[0]
        pb_buf = self._recv(buf_size)
        if len(pb_buf) == 0:
            logging.info('socket has been closed')
            return False

        result = parse_meta(pb_buf)
        if result is None:
            logging.warning('pb decode error, skip this message')
            return True

        meta_len, pb_msg_len, meta_info = result

        if meta_info.flow_id in self._recv_infos:
            expected_meta, rpc_controller, response_class, done, req_data = \
                self._recv_infos[meta_info.flow_id]

            try:
                if meta_info.flow_id != expected_meta.flow_id:
                    err_msg = 'rsp flow id not match: %d %d' % (expected_meta.flow_id,
                                                                meta_info.flow_id)
                    raise TcpConnection.Exception(RpcController.WRONG_FLOW_ID_ERROR, err_msg)
                elif meta_info.service_name != expected_meta.service_name or \
                                meta_info.method_name != expected_meta.method_name:
                    err_msg = 'rsp meta not match'
                    raise TcpConnection.Exception(RpcController.WRONG_RSP_META_ERROR, err_msg)
                elif meta_info.HasField('has_error'):
                    if meta_info.msg_name != TcpConnection._error_msg_name:
                        err_msg = 'rsp meta has error, but with wrong msg name'
                        raise TcpConnection.Exception(RpcController.WRONG_MSG_NAME_ERROR, err_msg)
                    else:
                        response_class = rpc_meta_pb2.ErrorResponse
                elif meta_info.msg_name != response_class.DESCRIPTOR.full_name:
                    err_msg = 'wrong response class'
                    raise TcpConnection.Exception(RpcController.WRONG_MSG_NAME_ERROR, err_msg)

                rsp_time = time.time()
                self._stat.add_rsp_stat(1, rsp_time - req_data.begin_time)
                rsp = _parse_message(pb_buf[8 + meta_len:8 + meta_len + pb_msg_len],
                                     response_class)

                if meta_info.HasField('has_error'):
                    raise TcpConnection.Exception(rsp.err_code, rsp.err_msg)

                del self._recv_infos[meta_info.flow_id]
                self._finish_rpc(done, rpc_controller, rsp, req_data.is_async)
                return True
            except TcpConnection.Exception as e:
                rpc_controller.SetFailed((e.err_code, e.err_msg))
                logging.warning(e.err_msg)
                del self._recv_infos[meta_info.flow_id]
                self._finish_rpc(done, rpc_controller, None, req_data.is_async)
                return True
        else:
            logging.warning('flow id not found:', meta_info.flow_id)
            return True


class TcpChannel(google.protobuf.service.RpcChannel):
    def __init__(self, addr, conn_class=TcpConnection, load_balancer=None,
                 spawn=gevent.spawn):
        google.protobuf.service.RpcChannel.__init__(self)
        self._flow_id = 0
        self._addr = addr
        self._connections = []
        for ip_port in self.resolve_addr(addr):
            self._connections.append(conn_class(ip_port, spawn))

        if len(self._connections) == 1:
            self._balancer = SingleConnLoadBalancer()
        elif load_balancer is not None:
            self._balancer = load_balancer
        else:
            self._balancer = IncLoadBalancer()
        logging.info('use %s as load balancer' % self._balancer.__class__.__name__)

    def __del__(self):
        self.close()

    def _get_flow_id(self):
        flow_id = self._flow_id
        self._flow_id += 1
        return flow_id

    def close(self):
        for conn in self._connections:
            conn.close()

    def resolve_addr(self, addr):
        if isinstance(addr, (list, set, tuple)):
            ip_ports = []
            for ad in addr:
                ads = self.resolve_addr(ad)
                if ads is None:
                    return None
                ip_ports += ads
            return ip_ports
        elif isinstance(addr, str):
            sep_index = addr.find('/')
            if sep_index == -1:  # cannot find '/', so treat it as ip port
                try:
                    ip, port = addr.split(':')
                except ValueError:
                    return None

                port = int(port)
                return [(ip, port)]
            else:
                addr_type = addr[:sep_index]
                addr_str = addr[sep_index + 1:]
                if addr_type == 'ip':
                    ip, port = addr_str.split(':')
                    port = int(port)
                    return [(ip, port)]
                elif addr_type == 'zk':
                    raise NotImplementedError
                else:
                    raise NotImplementedError

    # when done is None, it means the method call is synchronous
    # when it's not None, it means the call is asynchronous
    def CallMethod(self, method_descriptor, rpc_controller,
                   request, response_class, done):
        flow_id = self._get_flow_id()
        conn = self._balancer.get_connection_for_req(flow_id, request, self._connections)

        if done is None:
            res = gevent.event.AsyncResult()
            done = lambda _, rsp: res.set(rsp)
            conn.add_send_task(flow_id, method_descriptor, rpc_controller,
                               request, response_class, done,
                               TcpConnection._RequestData(False))
            return res.get()
        else:
            conn.add_send_task(flow_id, method_descriptor, rpc_controller,
                               request, response_class, done,
                               TcpConnection._RequestData(True))
            return None


class RpcClient(object):

    tcp_channel_class = TcpChannel

    def __init__(self, pool_size=1000):
        self._channels = {}
        self._pool = Pool(pool_size)

    def __del__(self):
        for channel in self._channels.itervalues():
            channel.close()

        self._pool.join()

    def get_tcp_channel(self, addr):
        if isinstance(addr, list):
            addr = tuple(addr)
        if addr not in self._channels:
            channel = RpcClient.tcp_channel_class(addr, self._pool.spawn)
            self._channels[addr] = channel
        else:
            channel = self._channels[addr]

        return channel


class RpcServerStat(object):

    class MethodStat(object):
        def __init__(self):
            self.call_num_per_min = 0
            self.total_call_num = 0

        def reset_stat_per_min(self):
            self.call_num_per_min = 0

    class ServiceStat(object):
        def __init__(self):
            self.call_num_per_min = 0
            self.total_call_num = 0
            self.method_stats = {}

        def add_method_stat(self, method, count):
            try:
                method_stat = self.method_stats[method]
            except KeyError:
                method_stat = RpcServerStat.MethodStat()
                self.method_stats[method] = method_stat

            method_stat.call_num_per_min += count
            method_stat.total_call_num += count

        def reset_stat_per_min(self):
            self.call_num_per_min = 0
            for method_stat in self.method_stats.itervalues():
                method_stat.reset_stat_per_min()

    def __init__(self):
        self.service_stats = {}

    def add_method_stat(self, service, method, count):
        try:
            service_stat = self.service_stats[service]
        except KeyError:
            service_stat = RpcServerStat.ServiceStat()
            self.service_stats[service] = service_stat

        service_stat.call_num_per_min += count
        service_stat.total_call_num += count
        service_stat.add_method_stat(method, count)

    def print_stat(self):
        print 'name\t| total_count\t| count/60s'

        for service_name, service_stat in self.service_stats.iteritems():
            print '%s\t| %d\t| %d' % (service_name, service_stat.total_call_num,
                                      service_stat.call_num_per_min)
            for method_name, method_stat in service_stat.method_stats.iteritems():
                print '   %s\t| %d\t| %d' % (method_name, method_stat.total_call_num,
                                             method_stat.call_num_per_min)

        print ''

    def reset_stat_per_min(self):
        for service_stat in self.service_stats.itervalues():
            service_stat.reset_stat_per_min()


class RpcServer(object):
    def __init__(self, addr, service_timeout=10, spawn=1000):
        if isinstance(addr, str):
            self._addr = addr.split(':')  # addr string like '127.0.0.1:30006'
            self._addr[1] = int(self._addr[1])
        else:
            self._addr = addr

        if isinstance(spawn, (int, long)):
            self._pool = Pool(spawn)
            self._spawn = self._pool.spawn
        else:
            self._pool = None
            self._spawn = spawn

        self._services = {}
        self._service_timeout = service_timeout
        self._stat = RpcServerStat()
        self._stream_server = gevent.server.StreamServer(self._addr,
                                                         self._handle_connection)

    def _handle_connection(self, socket, addr):
        socket.setsockopt(gevent.socket.SOL_TCP, gevent.socket.TCP_NODELAY, 1)
        socket.setsockopt(gevent.socket.IPPROTO_TCP, gevent.socket.TCP_NODELAY, 1)

        rsp_queue = gevent.queue.Queue()
        is_connection_closed = [False]

        def call_service(req_info):
            meta_info, service, method, req = req_info
            self._stat.add_method_stat(meta_info.service_name,
                                       meta_info.method_name, 1)

            controller = RpcController()
            try:
                with Timeout(self._service_timeout):
                    rsp = service.CallMethod(method, controller, req, None)
            except Timeout:
                meta_info.has_error = True
                rsp = rpc_meta_pb2.ErrorResponse(err_code=rpc_meta_pb2.SERVER_SERVICE_TIMEOUT,
                                                 err_msg='service timeout')
            except:
                meta_info.has_error = True
                err_msg = 'Error calling service: ' + traceback.format_exc()
                rsp = rpc_meta_pb2.ErrorResponse(err_code=rpc_meta_pb2.SERVER_SERVICE_ERROR,
                                                 err_msg=err_msg)

            rsp_queue.put_nowait((meta_info, rsp))

        def recv_req():
            content = ""
            while True:
                try:
                    recv_buf = socket.recv(1024)
                    if len(recv_buf) == 0:
                        break
                except Exception, e:
                    logging.warning('recv_req error: ' + str(e))
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
                        logging.warning('pb decode error, skip this message')
                        break

                    self._spawn(call_service, result)

                if cur_index > 0:
                    content = content[cur_index:]

            logging.info(str(addr) + 'has disconnected')
            is_connection_closed[0] = True

        def send_rsp():
            while not is_connection_closed[0]:
                try:
                    meta_info, rsp = rsp_queue.get(timeout=1)
                except gevent.queue.Empty:
                    continue

                serialized_rsp = _serialize_message(meta_info, rsp)
                sent_bytes = 0
                try:
                    while sent_bytes < len(serialized_rsp):
                        sent_bytes += socket.send(serialized_rsp[sent_bytes:])
                except soc_error as e:
                    logging.warning('socket error: ' + str(e))
                    break

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
            logging.warning('cannot find the service: ' + meta_info.service_name)
            return None

        method = service.GetDescriptor().FindMethodByName(meta_info.method_name)
        if method is None:
            logging.warning('cannot find the method: ' + meta_info.method_name)
            return None

        msg = _parse_message(buf[8 + meta_len:8 + meta_len + pb_msg_len],
                             service.GetRequestClass(method))
        if msg is None:
            return None
        else:
            return meta_info, service, method, msg

    def register_service(self, service):
        self._services[service.GetDescriptor().full_name] = service

    def print_stat(self, interval):
        while True:
            gevent.sleep(interval)
            self._stat.print_stat()
            self._stat.reset_stat_per_min()

    def run(self, print_stat_interval=None):
        if print_stat_interval is not None and print_stat_interval > 0:
            stat_worker = gevent.spawn(self.print_stat, print_stat_interval)

        try:
            self._stream_server.serve_forever()
        finally:
            stat_worker.kill()
            if self._pool is not None:
                self._pool.join()

