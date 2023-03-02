from flask import Flask
import threading
import boto3
import json
import mysql.connector
import uuid
import random
import time
import datetime
from faker import Faker
import asyncio
import requests

# MySQL 데이터베이스 설정
mysql_host = 'database-1.cani4y1t2hfg.ap-northeast-2.rds.amazonaws.com'
mysql_user = 'slows14tem'
mysql_password = '!wjdgy0117'
mysql_database = 'aiproject'
mysql_port = '3306'

# SQS 설정
sqs = boto3.client('sqs')
queue_url = 'https://sqs.ap-northeast-2.amazonaws.com/535901697629/test.fifo'
fake = Faker()
# MySQL 커넥션 생성
conn = mysql.connector.connect(user=mysql_user, password=mysql_password,
                               host=mysql_host, database=mysql_database, port=mysql_port)
cursor = conn.cursor(prepared=True)


async def send_sqs_messages():
    while True:
        # 더미데이터 초기값 설정

        # Define the range of values for each vital sign
        heart_rate_range = (60, 100)
        oxygen_saturation_range = (95, 100)
        body_temperature_range = (36.0, 37.5)
        step_stride_range = (60, 80)

        # Define the standard deviation for the normal distribution of each vital sign
        heart_rate_stddev = 5
        oxygen_saturation_stddev = 1
        body_temperature_stddev = 0.1
        step_stride_stddev = 2

        # Define the range of values for GPS coordinates, gyrosensor readings, and accelerometer readings
        gps_latitude_range = [35.168008, 35.169691]  # Busan, Republic of Korea
        # Busan, Republic of Korea
        gps_longitude_range = [129.139300, 129.141249]

        # Define the range of values for worker ID and age
        worker_id_range = [i for i in range(1001, 1011)]

        for worker_id in worker_id_range:
            heart_rate = random.randint(*heart_rate_range)
            oxygen_saturation = random.randint(*oxygen_saturation_range)
            body_temperature = round(
                random.uniform(*body_temperature_range), 1)
            step_stride = random.randint(*step_stride_range)
            gps_latitude = round(random.uniform(*gps_latitude_range), 6)
            gps_longitude = round(random.uniform(*gps_longitude_range), 6)

            worker_id = worker_id
            heart_rate += random.normalvariate(0, heart_rate_stddev)
            heart_rate = max(
                min(round(heart_rate), heart_rate_range[1]), heart_rate_range[0])
            oxygen_saturation += random.normalvariate(
                0, oxygen_saturation_stddev)
            oxygen_saturation = max(min(round(
                oxygen_saturation), oxygen_saturation_range[1]), oxygen_saturation_range[0])
            body_temperature += random.normalvariate(
                0, body_temperature_stddev)
            body_temperature = max(min(round(
                body_temperature, 1), body_temperature_range[1]), body_temperature_range[0])
            step_stride += random.normalvariate(0, step_stride_stddev)
            step_stride = max(
                min(round(step_stride), step_stride_range[1]), step_stride_range[0])
            gps_latitude += random.uniform(-0.0001, 0.0001)
            gps_latitude = max(
                min(round(gps_latitude, 6), gps_latitude_range[1]), gps_latitude_range[0])
            gps_longitude += random.uniform(-0.0001, 0.0001)
            gps_longitude = max(
                min(round(gps_longitude, 6), gps_longitude_range[1]), gps_longitude_range[0])

            # 랜덤 더미 데이터 생성

            gps_data = {
                'userId': worker_id,
                'lat': gps_latitude,
                'lon': gps_longitude,
                'recordTime': str(datetime.datetime.now())
            }

            vitalSign_data = {
                "userId": worker_id,
                "insertTime": str(datetime.datetime.now()),
                "heartRate": heart_rate,
                "temp": body_temperature,
                "o2": oxygen_saturation,
                "steps": step_stride
            }

            # Key 중복 허용
            message_body = 'Hello'
            message_deduplication_id = str(uuid.uuid4())

            # 데이터 SQS에 전송
            response = sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(
                    [gps_data,  vitalSign_data], default=str),
                MessageGroupId='my-group' + message_deduplication_id,
                MessageDeduplicationId=message_deduplication_id,
            )

            print()
            print('1. Sent GPS data: ', gps_data)
            print('2. Sent vitalSign data: ', vitalSign_data)
            print()

            # 1초 대기

            await asyncio.sleep(10)


async def save_sqs_messages_to_mysql():
    while True:
        # SQS에서 메시지를 받아옴
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,  # maximum number of messages to receive
            # maximum time to wait for a message (in seconds)
            WaitTimeSeconds=0,
        )

        # 메시지 처리
        if 'Messages' in response:
            for message in response['Messages']:
                # 메시지 데이터 추출
                message_data = json.loads(message['Body'])

                # model 처리

                # MySQL 데이터베이스에 데이터 삽입
                if len(message_data) >= 2:
                    for i in range(len(message_data)):
                        print('Save data: ', message_data[i])

                    sql1 = "INSERT INTO gps (userId, lat, lon, recordTime) VALUES (%s, %s, %s, %s)"
                    val1 = (message_data[0]['userId'], message_data[0]['lat'],
                            message_data[0]['lon'], message_data[0]['recordTime'])

                    sql2 = 'INSERT INTO vitalsign (userId, insertTime, heartRate, temp, o2, steps) VALUES (%s, %s, %s, %s, %s, %s)'
                    val2 = (message_data[1]['userId'], message_data[1]['insertTime'],
                            message_data[1]['heartRate'], message_data[1]['temp'], message_data[1]['o2'], message_data[1]['steps'])

                cursor.execute(sql1, val1)

                cursor.execute(sql2, val2)

                conn.commit()

                # 메시지 삭제
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )

                # Spring Boot 애플리케이션의 URL 호출
                data = message_data[0]['userId']

                # url = f'http://10.125.121.171:8081/healthpush/{data}'
                # response = requests.get(url)

                # url = f'http://10.125.121.183:3000/healthpush/{data}'

                url = f'http://10.125.121.183:8081/healthpush/{data}'
                response = requests.get(url)

            await asyncio.sleep(10)


def create_app():
    app = Flask(__name__)

    from .views import main_views
    app.register_blueprint(main_views.bp)
    app.run(host='127.0.0.1', port=5000)

    # 이벤트 루프 생성 및 실행
    loop = asyncio.get_event_loop()
    loop.create_task(send_sqs_messages())
    loop.create_task(save_sqs_messages_to_mysql())
    loop.run_forever()

    return app
