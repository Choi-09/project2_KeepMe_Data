from flask import Flask, request, redirect
from threading import Thread
import boto3
import json
import mysql.connector
import uuid
import asyncio

from sqs_flask_mysql.sqs_flask import send_sqs_messages
from sqs_flask_mysql.flask_mysql import save_sqs_messages_to_mysql
app = Flask(__name__)

@app.route('/')
def index():
    return 'Hello, World!'

if __name__ == "pybo":
    loop = asyncio.get_event_loop()
    loop.create_task(send_sqs_messages())
    loop.create_task(save_sqs_messages_to_mysql())
    loop.run_forever()

    app.run(host='127.0.0.1', port=5000)

