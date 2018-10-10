from flask import Flask
from flask import request
from flask import jsonify

app = Flask(__name__)

@app.route("/get_my_ip", methods=["GET"])
def get_my_ip():
    return jsonify({'ip': request.remote_addr}), 200

if __name__ == '__main__':
    app.run()
