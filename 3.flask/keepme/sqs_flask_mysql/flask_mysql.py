import threading
import boto3
import json
import mysql.connector

from faker import Faker
import random
import datetime
import time

## AWS-RDS 정보 가리기 
from dotenv import load_dotenv
import os

# MySQL 데이터베이스 설정
load_dotenv()

# SQS 설정
sqs = boto3.client('sqs')
queue_url = os.getenv("queue_url")

# MySQL 커넥션 생성
conn = mysql.connector.connect(user=os.getenv("mysql_user"), password=os.getenv("mysql_password"),
                               host=os.getenv("mysql_host"), database=os.getenv("mysql_database"), port=os.getenv("mysql_port"))
cursor = conn.cursor(prepared=True)

def save_sqs_messages_to_mysql():
    while True:
        # SQS에서 메시지를 받아옴
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,  # maximum number of messages to receive
            # maximum time to wait for a message (in seconds)
            WaitTimeSeconds=0,
        )

        # process the messages
        if 'Messages' in response:
            for message in response['Messages']:
                # 메시지 데이터 추출
                # 메시지 데이터 json형식으로 받음
                message_data = json.loads(message['Body'])

                # MySQL 데이터베이스에 데이터 삽입
                if len(message_data) >=2: 
                    for i in range(len(message_data)):
                        print('Save data: ', message_data[i])
                        sql1 = "INSERT INTO activity (userId, accX, accY, accZ, gyroX, gyroY, gyroZ) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                        val1 = (message_data[0]['userId'], message_data[0]['accX'],message_data[0]['accY'],message_data[0]['accZ'],
                            message_data[0]['gyroX'], message_data[0]['gyroY'], message_data[0]['gyroZ'])

                        sql2 = 'INSERT INTO device (userId, deviceName, battery) VALUES (%s, %s, %s)'
                        val2 = (message_data[1]['userId'], message_data[1]['deviceName'], message_data[1]['battery'])

                        sql3 = "INSERT INTO gps (userId, lat, lon, recordTime) VALUES (%s, %s, %s, %s)"
                        val3 = (message_data[2]['userId'], message_data[2]['lat'],
                            message_data[2]['lon'], message_data[2]['recordTime'])

                        sql4 = "INSERT INTO state (userId, state) VALUES (%s, %s)"
                        val4 = (message_data[3]['userId'], message_data[3]['state'])

                        sql5 = 'INSERT INTO user (id, pw, name, age, contact, position, role, employDate, workplace) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
                        val5 = (message_data[4]['id'], message_data[4]['pw'],message_data[4]['name'], message_data[4]['age'],message_data[4]['contact'],
                            message_data[4]['position'], message_data[4]['role'], message_data[4]['employDate'], message_data[4]['workplace'])

                        sql6 = 'INSERT INTO vitalsign (userId, insertTime, heartRate, temp, o2, steps) VALUES (%s, %s, %s, %s, %s, %s)'
                        val6 = (message_data[5]['userId'], message_data[5]['insertTime'],
                            message_data[5]['heartRate'], message_data[5]['temp'], message_data[5]['o2'], message_data[5]['steps'])
                        
                        cursor.execute(sql1, val1)
                        cursor.execute(sql2, val2)
                        cursor.execute(sql3, val3)
                        cursor.execute(sql4, val4)
                        cursor.execute(sql5, val5)
                        cursor.execute(sql6, val6)
                    
                        conn.commit()

                        # 메시지 삭제
                        sqs.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=message['ReceiptHandle'],
                            )