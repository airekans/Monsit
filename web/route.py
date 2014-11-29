from flask import Flask, render_template, request, abort, jsonify
from monsit import db
import datetime
import json

app = Flask(__name__)


class HostInfo(object):
    def __init__(self, host_id, name, is_connected, last_update_time):
        self.id = host_id
        self.name = name
        self.is_connected = is_connected
        self.last_update_time = last_update_time

@app.route("/")
def index():
    with db.DBConnection() as cnx:
        host_infos = []
        for host in cnx.get_all_hosts():
            host_id = host[0]
            host_name = host[1]
            infos = cnx.get_host_infos(host_id, [1])
            info_json = json.loads(infos[1])
            host_infos.append(
                HostInfo(host_id, host_name, info_json['connected'],
                         datetime.datetime.fromtimestamp(info_json['datetime'])))

    return render_template('index.html', hosts=host_infos)

@app.route('/hostinfo', methods=['GET'])
def hostinfo():
    try:
        host_id = int(request.args['id'])
        host_name = request.args['name']
    except (KeyError, ValueError):
        abort(404)

    return render_template('hostinfo.html', host_id=host_id, host_name=host_name)


_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


@app.route('/_get_host_stat', methods=['GET'])
def ajax_hoststat():
    stat_ids = request.args.getlist('stat_ids[]', type=int)
    host_id = request.args.get('host_id', 0, type=int)

    with db.DBConnection() as cnx:
        host_stats = cnx.get_host_stats(host_id, stat_ids)

    return jsonify(return_code=0, stats=host_stats)


@app.route('/_get_latest_stat', methods=['GET'])
def ajax_latest_stat():
    stat_ids = request.args.getlist('stat_ids[]', type=int)
    host_id = request.args.get('id', 0, type=int)
    last_times = request.args.getlist('latest_time[]')
    #print 'stat_id', stat_id, 'host_id', host_id, 'last_time', last_time

    with db.DBConnection() as cnx:
        try:
            latest_stats = cnx.get_updated_stats(host_id, stat_ids, last_times)
        except:
            print 'db error'
            return jsonify(return_code=1)

    if latest_stats is None:
        return jsonify(return_code=1)

    return jsonify(return_code=0, stats=latest_stats)


@app.route('/_get_host_info', methods=['GET'])
def ajax_host_info():
    info_ids = request.args.getlist('info_ids[]', type=int)
    host_id = request.args.get('id', 0, type=int)
    print 'host_id', host_id

    with db.DBConnection() as cnx:
        try:
            host_infos = cnx.get_host_infos(host_id, info_ids)
            print host_infos
            return jsonify(return_code=0, infos=host_infos)
        except:
            print 'db error'
            import traceback
            traceback.print_exc()
            return jsonify(return_code=1)


@app.route("/add_stat.html")
def add_stat():
    with db.DBConnection() as cnx:
        host_infos = []
        for host in cnx.get_all_hosts():
            host_id = host[0]
            host_name = host[1]
            host_infos.append(HostInfo(host_id, host_name, True, None))

    return render_template('add_stat.html', hosts=host_infos,
                           value_types=db.ValueType.value_type_str)


@app.route("/do_add_stat", methods=['POST'])
def do_add_stat():
    host_id = int(request.form['host_id'])
    stat_name = request.form['stat_name']
    chart_name = request.form['chart_name']
    y_value_type = int(request.form['value_type'])
    y_unit = request.form['unit']

    with db.DBConnection() as cnx:
        stat_id = cnx.insert_new_stat(host_id, stat_name, chart_name,
                                      y_value_type, y_unit)
        cnx.commit()

    return render_template('admin_msg.html',
                           msg=('New stat id is %d' % stat_id))


@app.route("/add_info.html")
def add_info():
    with db.DBConnection() as cnx:
        host_infos = []
        for host in cnx.get_all_hosts():
            host_id = host[0]
            host_name = host[1]
            host_infos.append(HostInfo(host_id, host_name, True, None))

    return render_template('add_info.html', hosts=host_infos)


@app.route("/do_add_info", methods=['POST'])
def do_add_info():
    host_id = int(request.form['host_id'])
    info_name = request.form['info_name']
    chart_name = request.form['chart_name']

    with db.DBConnection() as cnx:
        info_id = cnx.insert_new_info(host_id, info_name, chart_name)
        cnx.commit()

    return render_template('admin_msg.html',
                           msg=('New info id is %d' % info_id))


@app.route("/add_alarm.html")
def add_alarm():
    with db.DBConnection() as cnx:
        host_infos = []
        for host in cnx.get_all_hosts():
            host_id = host[0]
            host_name = host[1]
            host_infos.append(HostInfo(host_id, host_name, True, None))

    return render_template('add_alarm.html', hosts=host_infos)


@app.route("/do_add_alarm", methods=['POST'])
def do_add_alarm():
    host_id = int(request.form['host_id'])
    alarm_name = request.form['alarm_name']
    alarm_type = request.form['alarm_type']
    stat_or_info_id = int(request.form['stat_info_id'])
    threshold_type = request.form['threshold_type']
    threshold = request.form['threshold']
    if threshold_type == 'int':
        threshold = int(threshold)
    elif threshold_type == 'double':
        threshold = float(threshold)

    message = request.form['message']
    emails = request.form['emails']

    with db.DBConnection() as cnx:
        cnx.insert_alarm_setting(host_id, alarm_name, alarm_type,
                                 stat_or_info_id, threshold_type,
                                 threshold, message, emails)
        cnx.commit()

    return render_template('admin_msg.html',
                           msg='Alarm has been added successfully')


if __name__ == "__main__":
    db.init()
    app.run(host='0.0.0.0', debug=True)
