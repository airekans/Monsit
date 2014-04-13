from flask import Flask, render_template, request
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
    host_id = request.args.get('id', 'None')
    return render_template('hostinfo.html', host_id=host_id)


if __name__ == "__main__":
    db.init()
    app.run(host='0.0.0.0', debug=True)