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
    stat_id = request.args.get('stat_id', 0, type=int)
    host_id = request.args.get('id', 0, type=int)
    last_time = request.args.get('latest_time')
    #print 'stat_id', stat_id, 'host_id', host_id, 'last_time', last_time

    with db.DBConnection() as cnx:
        try:
            latest_stats = cnx.get_updated_stats(host_id, stat_id, last_time)
        except:
            print 'db error'
            return jsonify(return_code=1)

    if latest_stats is None:
        return jsonify(return_code=1)

    return jsonify(return_code=0, stats=latest_stats)


if __name__ == "__main__":
    db.init()
    app.run(host='0.0.0.0', debug=True)
