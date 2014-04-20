from flask import Flask, render_template, request, abort, jsonify
import db

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
    diff_sec = (cur_date - last_date).total_seconds()

    return {'recv': (cur_net_info.recv_byte - last_net_inf.recv_byte) / diff_sec,
            'send': (cur_net_info.send_byte - last_net_inf.send_byte) / diff_sec}


@app.route('/_get_hostinfo', methods=['GET'])
def ajax_hostinfo():
    field_types = request.args.getlist('type[]')
    host_id = request.args.get('id', 0, type=int)
    host_stats = {}
    with db.DBConnection() as cnx:
        try:
            db_host_stats = cnx.get_host_stats(host_id, field_types)
        except:
            print 'db error'
            raise

        for field, db_stats in db_host_stats.iteritems():
            if field == 'cpu':
                host_stats['cpu'] = {}
                cpu_stat = host_stats['cpu']
                last_cpu_stat = {}
                for date, cpu_info in db_stats:
                    stat_time = date.strftime('%Y-%m-%d %H:%M:%S')
                    cpu_name = cpu_info.name
                    if cpu_name not in cpu_stat:
                        cpu_stat[cpu_name] = {}
                    cpu_stat[cpu_name][stat_time] = \
                        get_cpu_usage(cpu_info, last_cpu_stat.get(cpu_name, None))
                    last_cpu_stat[cpu_name] = cpu_info
            elif field == 'net':
                host_stats['net'] = {}
                net_stat = host_stats['net']
                last_net_stat = {}
                for date, net_info in db_stats:
                    stat_time = date.strftime('%Y-%m-%d %H:%M:%S')
                    net_dev_name = net_info.name
                    if net_dev_name not in net_stat:
                        net_stat[net_dev_name] = {}
                    net_stat[net_dev_name][stat_time] = \
                        get_net_flow_stat((date, net_info),
                                          last_net_stat.get(net_dev_name, None))
                    last_net_stat[net_dev_name] = (date, net_info)

    return jsonify(stats=host_stats)


if __name__ == "__main__":
    db.init()
    app.run(host='0.0.0.0', debug=True)