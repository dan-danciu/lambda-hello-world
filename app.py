
from flask import Flask, jsonify, request
from datetime import datetime
from flask_cors import CORS
from s3db import s3DB
from bosUser.BosUser import BosUser, BosDays, BosYear, AllUsers
import boto3
import json

app = Flask(__name__)
CORS(app)

@app.route('/auth', methods = ['GET'])
def auth():
    user_data={}
    user_data['username'] = request.headers.get('GoogleUserName')
    user_data['email'] = request.headers.get('GoogleEmail')
    user_data['image'] = request.headers.get('GoogleImage')
    user_data['principalId'] = request.headers.get('principalId')
    year = datetime.now().year
    user = BosUser(user_data, str(year))
    all_users = AllUsers()
    all_users.addUser(user)
    response = jsonify(user.toJSON())
    return response, 200

@app.route('/', methods = ['GET'])
def root_view():
    user_data={}
    user_data['username'] = request.headers.get('GoogleUserName')
    user_data['email'] = request.headers.get('GoogleEmail')
    user_data['image'] = request.headers.get('GoogleImage')
    user_data['principalId'] = request.headers.get('principalId')
    year = str(datetime.now().year)
    year_data = BosYear(year)
    response = jsonify(year_data.toJSON())
    return response, 200

@app.route('/<user_id>', methods = ['GET'])
def profile(user_id):
    user_data={}
    user_data['username'] = request.headers.get('GoogleUserName')
    user_data['email'] = request.headers.get('GoogleEmail')
    user_data['image'] = request.headers.get('GoogleImage')
    user_data['principalId'] = request.headers.get('principalId')
    year = datetime.now().year
    user = BosUser(user_data, str(year))
    response = jsonify(user.toJSON())
    return response, 200

@app.route('/all_users', methods = ['GET'])
def all_users():
    response={}
    user_list = AllUsers()
    response = jsonify(user_list.toJSON())
    return response, 200

@app.route('/add_user', methods = ['POST'])
def add_user():
    user = request.get_json()
    mydb = s3DB.s3DB('users')
    mydb.bucket = "tbos-data"
    mydb.index = "principalID"
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
