import boto3
import json
import random
import time
import hashlib
import datetime as dt
#
sqs = boto3.client('sqs')

queue_url = 'https://sqs.ap-northeast-2.amazonaws.com/535901697629/test.fifo'

while True:
    # GPS Table의 랜덤 더미 데이터 생성
    gps_data = {
        'id': str(random.randint(1, 100)),
        'user_id': str(random.randint(1, 10)),
        'latitude': random.uniform(-90, 90),
        'longitude': random.uniform(-180, 180),
        'time_sent': str(random.randint(1, 10))
    }
    message_body = 'Hello' + str(random.randint(-100, 100))
    message_deduplication_id = hashlib.sha256(
        message_body.encode('utf-8')).hexdigest()
    # GPS 데이터 SQS에 전송
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(gps_data),
        MessageGroupId='my-group' + gps_data['id'],
        MessageDeduplicationId=message_deduplication_id,
    )

    print('Sent GPS data: ', gps_data)

    # 10초 대기
    time.sleep(1)
