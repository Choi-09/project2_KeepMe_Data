from flask import Flask
import threading
import boto3
import json
import mysql.connector
import uuid

app = Flask(__name__)

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

# SQS에서 메시지를 가져와 MySQL DB에 저장하는 함수


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


# Flask 애플리케이션 루트


@app.route('/')
def index():
    return 'Hello World!'


if __name__ == '__main__':
    # 메시지 저장 스레드 시작
    threading.Thread(target=save_sqs_messages_to_mysql).start()

    # Flask 애플리케이션 실행
    app.run()
