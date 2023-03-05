from flask import Flask
import threading
import boto3
import json
import mysql.connector
import uuid
import random
import time

# MySQL 데이터베이스 설정
mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = 'tiger'
mysql_database = 'mydb01'
mysql_port = '3306'

# SQS 설정
sqs = boto3.client('sqs')
queue_url = 'https://sqs.ap-northeast-2.amazonaws.com/535901697629/test.fifo'

# MySQL 커넥션 생성
conn = mysql.connector.connect(user=mysql_user, password=mysql_password,
                               host=mysql_host, database=mysql_database, port=mysql_port)
cursor = conn.cursor(prepared=True)


def send_sqs_messages():
    while True:
        # GPS Table의 랜덤 더미 데이터 생성
        gps_data = {
            'id': str(random.randint(1, 100)),
            'user_id': str(random.randint(1, 10)),
            'latitude': random.uniform(-90, 90),
            'longitude': random.uniform(-180, 180),
            'time_sent': str(random.randint(1, 10))
        }
        message_body = 'Hello'
        message_deduplication_id = str(uuid.uuid4())

        # GPS 데이터 SQS에 전송
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(gps_data),
            MessageGroupId='my-group' + gps_data['id'],
            MessageDeduplicationId=message_deduplication_id,
        )

        print('Sent GPS data: ', gps_data)

        # # 10초 대기
        time.sleep(3)


def save_sqs_messages_to_mysql():
    while True:
        # SQS에서 메시지를 받아옴
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,  # maximum number of messages to receive
            # maximum time to wait for a message (in seconds)
            WaitTimeSeconds=0,
        )

        # 메시지 처리
        if 'Messages' in response:
            for message in response['Messages']:
                # 메시지 데이터 추출
                message_data = json.loads(message['Body'])

                # model 처리

                # MySQL DB에 데이터 저장
                sql = "INSERT INTO gps (id, user_id, latitude, longitude, time_sent) VALUES (%s, %s, %s, %s, %s)"
                val = (message_data['id'], message_data['user_id'], message_data['latitude'],
                       message_data['longitude'], message_data['time_sent'])
                cursor.execute(sql, val)
                conn.commit()

                # 메시지 삭제
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message['ReceiptHandle'],
                )


def create_app():
    app = Flask(__name__)

    from .views import main_views
    app.register_blueprint(main_views.bp)
    app.run(host='127.0.0.1', port=5000)
# 메시지 저장 스레드 시작
    t1 = threading.Thread(target=send_sqs_messages)
    t2 = threading.Thread(target=save_sqs_messages_to_mysql)

    t1.daemon = True
    t2.daemon = True
    t1.start()
    t2.start()
    return app
