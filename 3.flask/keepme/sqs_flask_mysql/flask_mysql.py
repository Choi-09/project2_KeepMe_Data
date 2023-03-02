import boto3
import json
import mysql.connector
import asyncio
import tracemalloc

## AWS-RDS 정보 가리기 
from dotenv import load_dotenv
import os

tracemalloc.start()
# MySQL 데이터베이스 설정
load_dotenv()

# SQS 설정
sqs = boto3.client('sqs')
queue_url = os.getenv("queue_url")

# MySQL 커넥션 생성
conn = mysql.connector.connect(user=os.getenv("mysql_user"), password=os.getenv("mysql_password"),
                               host=os.getenv("mysql_host"), database=os.getenv("mysql_database"), port=os.getenv("mysql_port"))
cursor = conn.cursor(prepared=True)

async def save_sqs_messages_to_mysql():
    while True:
        # SQS에서 메시지를 받아옴
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,  # maximum number of messages to receive
            # maximum time to wait for a message (in seconds)
            WaitTimeSeconds=0,
        )

        # process the messages
        if 'Messages' in response:
            for message in response['Messages']:
                # 메시지 데이터 추출
                # 메시지 데이터 json형식으로 받음
                message_data = json.loads(message['Body'])

            # 모델 학습

                # MySQL 데이터베이스에 데이터 삽입
                for i in range(len(message_data)):
                    print('Save data: ', message_data[i])
                    sql = "INSERT INTO HealthLog (userId, recordTime, lat, lon, accX, accY, accZ, gyroX, gyroY, gyroZ, heartRate, temperature, o2, steps, status, accuracy) VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s, %s)"
                    val = (message_data[0]['userId'], message_data[0]['recordTime'], message_data[0]["lat"], message_data[0]['lon'], message_data[0]['accX'],message_data[0]['accY'],message_data[0]['accZ'],
                        message_data[0]['gyroX'], message_data[0]['gyroY'], message_data[0]['gyroZ'], message_data[0]["heartRate"], message_data[0]['temperature'], message_data[0]['o2'], message_data[0]['steps'], message_data[0]['status'], message_data[0]['accuracy'])
                        
                    cursor.execute(sql, val)
                    conn.commit()

                    # 메시지 삭제
                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                        )
                
                # Spring Boot 애플리케이션의 URL 호출
                # data = message_data[0]['userId']

                # # url = f'http://10.125.121.171:8081/healthpush/{data}'
                # # response = requests.get(url)

                # # url = f'http://10.125.121.183:3000/healthpush/{data}'

                # url = f'http://10.125.121.183:8081/healthpush/{data}'
                # response = requests.get(url)

            await asyncio.sleep(1)

        else:
            print("No Messages in Queue")
            break
            