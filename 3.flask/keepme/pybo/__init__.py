from flask import Flask, request, redirect
from threading import Thread
import boto3
import json
import mysql.connector
import uuid

from sqs_flask_mysql.flask_mysql import save_sqs_messages_to_mysql
from sqs_flask_mysql.sqs_flask import send_sqs_messages

app = Flask(__name__)

@app.route('/')
def index():
    return 'Hello, World!'

if __name__ == "pybo":
    th1 = Thread(target=save_sqs_messages_to_mysql)
    th2 = Thread(target=send_sqs_messages)
    th1.start()
    th2.start()

    app.run(host='127.0.0.1', port=5000)

