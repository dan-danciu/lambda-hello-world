
from flask import Flask, jsonify, request
from flask_cors import CORS
from s3db import s3DB
import boto3
import json

app = Flask(__name__)
CORS(app)

@app.route('/', methods = ['GET'])
def hello_world():
    response={}
    response['username'] = request.headers.get('GoogleUserName')
    response['email'] = request.headers.get('GoogleEmail')
    response['image'] = request.headers.get('GoogleImage')
    response = jsonify(response)
    return response, 200

@app.route('/all_users', methods = ['GET'])
def all_users():
    response={}

    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('tbos-data')

    for obj in  my_bucket.objects.all():
         content = obj.get()['Body'].read().decode('utf-8')
         user_json = json.loads(content)
         response = user_json['obj']
    response = jsonify(response)
    return response, 200

@app.route('/add_user', methods = ['POST'])
def add_user():
    user = request.get_json()
    mydb = s3DB.s3DB('users')
    mydb.bucket = "tbos-data"
    mydb.index = "name"
    #user = {"name": "Dan Danciu", "age": 23}
    mydb.save(user)
    return jsonify({"message": "User added."}), 200

@app.route('/delete_user', methods = ['POST'])
def delete_user():
    user = request.get_json()
    mydb = s3DB.s3DB('users')
    mydb.bucket = "tbos-data"
    if 'name' in user:
        if mydb.delete(user["name"]):
            return jsonify(f"User {user['name']} deleted" ), 200
    else:
        return jsonify("User name not specified"), 404

if __name__ == '__main__':
    app.run()
