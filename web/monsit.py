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


def get_cpu_usage(cpu_info):
    return ((cpu_info.user_count + cpu_info.nice_count + cpu_info.sys_count) * 100 /
            cpu_info.total_count)


@app.route('/_get_hostinfo', methods=['GET'])
def ajax_hostinfo():
    field_type = request.args.get('type', 'cpu')
    print 'hello'
    host_id = request.args.get('id', 0, type=int)
    host_stats = {}
    with db.DBConnection() as cnx:
        db_host_stats = cnx.get_host_stats(host_id, [field_type])
        for field, db_stats in db_host_stats.iteritems():
            if field == 'cpu':
                host_stats['cpu'] = {}
                cpu_stat = host_stats['cpu']
                for date, cpu_info in db_stats:
                    stat_time = date.strftime('%Y-%m-%d %H:%M:%S')
                    cpu_name = cpu_info.name
                    if cpu_name not in cpu_stat:
                        cpu_stat[cpu_name] = {}
                    cpu_stat[cpu_name][stat_time] = get_cpu_usage(cpu_info)

    return jsonify(stats=host_stats)


if __name__ == "__main__":
    db.init()
    app.run(host='0.0.0.0', debug=True)