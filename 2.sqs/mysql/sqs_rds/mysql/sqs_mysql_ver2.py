import boto3
import mysql.connector
import json
import time

# MySQL 데이터베이스 설정
mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = 'tiger'
mysql_database = 'mydb01'
mysql_port = '3306'

conn = mysql.connector.connect(user=mysql_user, password=mysql_password,
                               host=mysql_host, database=mysql_database, port=mysql_port)
cursor = conn.cursor(prepared=True)
# create an SQS client
sqs = boto3.client('sqs')
# receive messages from the queue
response = sqs.receive_message(
    QueueUrl='https://sqs.ap-northeast-2.amazonaws.com/535901697629/test.fifo',
    MaxNumberOfMessages=10,  # maximum number of messages to receive
    WaitTimeSeconds=20,  # maximum time to wait for a message (in seconds)
)

# process the messages
if 'Messages' in response:
    for message in response['Messages']:
        # 메시지 데이터 추출
        # 메시지 데이터 문자열로 받음
        message_data = eval(message['Body'])
        # MySQL 데이터베이스에 데이터 삽입
        print(message_data)
        sql = "INSERT INTO gps (id, user_id, latitude, longitude, time_sent) VALUES (%s, %s, %s, %s, %s)"
        val = (message_data['id'], message_data['user_id'], message_data['latitude'],
               message_data['longitude'], message_data['time_sent'])

        cursor.execute(sql, val)
        conn.commit()
        # 시간 초
        time.sleep(3)
        # delete the message from the queue
        sqs.delete_message(
            QueueUrl='https://sqs.ap-northeast-2.amazonaws.com/535901697629/test.fifo',
            ReceiptHandle=message['ReceiptHandle'],
        )


else:
    print('No messages in the queue')
