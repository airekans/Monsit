from flask import Flask, render_template, request, abort, jsonify
from monsit import db
import datetime

app = Flask(__name__)


class HostInfo(object):
    def __init__(self, host_id, name):
        self.id = host_id
        self.name = name

@app.route("/")
def index():
    with db.DBConnection() as cnx:
        host_infos = [HostInfo(host[0], host[1]) for host in cnx.get_all_hosts()]

    return render_template('index.html', hosts=host_infos)

@app.route('/hostinfo', methods=['GET'])
def hostinfo():
    try:
        host_id = int(request.args['id'])
        host_name = request.args['name']
    except (KeyError, ValueError):
        abort(404)

    return render_template('hostinfo.html', host_id=host_id, host_name=host_name)


def get_cpu_usage(cpu_info, last_cpu_info):
    if last_cpu_info is None:
        return ((cpu_info.user_count + cpu_info.nice_count + cpu_info.sys_count) * 100 /
                cpu_info.total_count)

    used_diff = ((cpu_info.user_count + cpu_info.nice_count + cpu_info.sys_count) -
                 (last_cpu_info.user_count + last_cpu_info.nice_count + last_cpu_info.sys_count))
    total_diff = cpu_info.total_count - last_cpu_info.total_count
    return used_diff * 100 / total_diff


def get_net_flow_stat(net_info, last_net_info):
    cur_date = net_info[0]
    cur_net_info = net_info[1]
    if last_net_info is None:
        return {'recv': 0,
                'send': 0}

    last_date = last_net_info[0]
    last_net_inf = last_net_info[1]
    diff_sec = int((cur_date - last_date).total_seconds())

    recv_amount = (cur_net_info.recv_byte - last_net_inf.recv_byte) / diff_sec / 1024  # KB
    send_amount = (cur_net_info.send_byte - last_net_inf.send_byte) / diff_sec / 1024  # KB
    if recv_amount < 0:
        recv_amount = 0
    if send_amount < 0:
        send_amount = 0

    return {'recv': recv_amount, 'send': send_amount}


def get_disk_io_stat(disk_io_info, last_disk_io_info):
    cur_date = disk_io_info[0]
    cur_io_info = disk_io_info[1]
    if last_disk_io_info is None:
        return cur_io_info.read_bytes / 1024

    last_date = last_disk_io_info[0]
    last_io_info = last_disk_io_info[1]
    diff_sec = int((cur_date - last_date).total_seconds())

    read_rate = (cur_io_info.read_bytes - last_io_info.read_bytes) / diff_sec / 1024
    if read_rate < 0:
        read_rate = 0
    return read_rate


_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


@app.route('/_get_hostinfo', methods=['GET'])
def ajax_hostinfo():
    stat_ids = request.args.getlist('stat_ids[]', type=int)
    host_id = request.args.get('host_id', 0, type=int)

    with db.DBConnection() as cnx:
        host_stats = cnx.get_host_stats(host_id, stat_ids)

    return jsonify(return_code=0, stats=host_stats)


@app.route('/_get_latest_info', methods=['GET'])
def ajax_latest_info():
    field_type = request.args.get('type')
    host_id = request.args.get('id', 0, type=int)
    last_time = request.args.get('latest_time')
    try:
        last_date = datetime.datetime.strptime(last_time, _DATE_FORMAT)
    except ValueError, e:
        print 'date parsing error:', e
        return jsonify(return_code=1)

    with db.DBConnection() as cnx:
        try:
            db_host_stats = cnx.get_updated_stats(host_id, field_type, last_date)
        except:
            print 'db error'
            raise

    return jsonify(return_code=0)


if __name__ == "__main__":
    db.init()
    app.run(host='0.0.0.0', debug=True)
