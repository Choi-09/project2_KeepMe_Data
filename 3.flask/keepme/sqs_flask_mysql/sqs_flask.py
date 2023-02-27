import uuid
import boto3
import json

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


def send_sqs_messages():   
    fake= Faker()
    # 더미데이터 초기값 설정
    # static 데이터
    worker_id = ['1001', '1002', '1003', '1004', '1005', '1006', '1007', '1008', '1009', '1010']
    device = ['ph', 'w']
    state = ""
    # dynamic 데이터 범위 : vital sign
    heart_rate_range = (60, 100)
    oxygen_saturation_range = (95, 100)
    body_temperature_range = (36.0, 37.5)
    step_stride_range = (60, 80)

    # dynamic 데이터 범위 :  GPS좌표, gyrosensor 정보, 가속도계 정보
    gps_latitude_range = [35.168008 , 35.169691]  # 우2동 재개발지역
    gps_longitude_range = [129.139300, 129.141249]  # 우2동 재개발지역
    gyrosensor_x_range = (-10, 10)
    gyrosensor_y_range = (-10, 10)
    gyrosensor_z_range = (-10, 10)
    accelerometer_x_range = (-2, 2)
    accelerometer_y_range = (-2, 2)
    accelerometer_z_range = (-2, 2)

    # dynamic 데이터 범위: 나이, 배터리 
    battery_range = (0, 100)

    # Define the standard deviation for the normal distribution of each vital sign
    heart_rate_stddev = 5
    oxygen_saturation_stddev = 1
    body_temperature_stddev = 0.1
    step_stride_stddev = 2

    battery_stddev = 2
    battery = 100 

    while True: 
        for i in range(len(worker_id)):      
            heart_rate = random.randint(*heart_rate_range)
            oxygen_saturation = random.randint(*oxygen_saturation_range)
            body_temperature = round(random.uniform(*body_temperature_range), 1)
            step_stride = random.randint(*step_stride_range)
            gps_latitude = round(random.uniform(*gps_latitude_range), 6)
            gps_longitude = round(random.uniform(*gps_longitude_range), 6)
            gyrosensor_x = random.uniform(*gyrosensor_x_range)
            gyrosensor_y = random.uniform(*gyrosensor_y_range)
            gyrosensor_z = random.uniform(*gyrosensor_z_range)
            accelerometer_x = random.uniform(*accelerometer_x_range)
            accelerometer_y = random.uniform(*accelerometer_y_range)
            accelerometer_z = random.uniform(*accelerometer_z_range)

            heart_rate += random.normalvariate(0, heart_rate_stddev)
            oxygen_saturation += random.normalvariate(0, oxygen_saturation_stddev)
            body_temperature += random.normalvariate(0, body_temperature_stddev)
            step_stride += random.normalvariate(0, step_stride_stddev)
            gps_latitude += random.uniform(-0.0001, 0.0001)
            gps_longitude += random.uniform(-0.0001, 0.0001)
            gyrosensor_x += random.uniform(-0.05, 0.05)
            gyrosensor_y += random.uniform(-0.05, 0.05)
            gyrosensor_z += random.uniform(-0.05, 0.05)
            accelerometer_x += random.uniform(-0.02, 0.02)
            accelerometer_y += random.uniform(-0.02, 0.02)
            accelerometer_z += random.uniform(-0.02, 0.02)       
            # dynamic 데이터
            heart_rate = max(min(round(heart_rate), heart_rate_range[1]), heart_rate_range[0])
            oxygen_saturation = max(min(round(oxygen_saturation), oxygen_saturation_range[1]), oxygen_saturation_range[0])
            body_temperature = max(min(round(body_temperature, 1), body_temperature_range[1]), body_temperature_range[0])
            step_stride = max(min(round(step_stride), step_stride_range[1]), step_stride_range[0])
            gps_latitude = max(min(round(gps_latitude, 6), gps_latitude_range[1]), gps_latitude_range[0])
            gps_longitude = max(min(round(gps_longitude, 6), gps_longitude_range[1]), gps_longitude_range[0])
            gyrosensor_x = max(min(round(gyrosensor_x, 17), gyrosensor_x_range[1]), gyrosensor_x_range[0])
            gyrosensor_y = max(min(round(gyrosensor_y, 17), gyrosensor_y_range[1]), gyrosensor_y_range[0])
            gyrosensor_z = max(min(round(gyrosensor_z, 17), gyrosensor_z_range[1]), gyrosensor_z_range[0])
            accelerometer_x = max(min(round(accelerometer_x, 17), accelerometer_x_range[1]), accelerometer_x_range[0])
            accelerometer_y = max(min(round(accelerometer_y, 17), accelerometer_y_range[1]), accelerometer_y_range[0])
            accelerometer_z = max(min(round(accelerometer_z, 17), accelerometer_z_range[1]), accelerometer_z_range[0])

            # 디바이스 배터리
            battery = max(min(round(battery), battery_range[1]), battery_range[0])
            battery -= random.randint(0, battery_stddev)
            deviceName = device[random.randint(0,len(device)-1)]
            
            # 랜덤 더미 데이터 생성
            activity_data = {
                'userId' : worker_id[i],
                "accX" : accelerometer_x,
                "accY" : accelerometer_y,
                "accZ" : accelerometer_z,
                "gyroX" : gyrosensor_x,
                "gyroY" : gyrosensor_y,
                "gyroZ" : gyrosensor_z,
            }

            device_data = {
                'userId' : activity_data['userId'],
                "deviceName" : deviceName,
                'battery' : battery,
            }

            gps_data = {
                'userId': activity_data['userId'],
                'lat': gps_latitude,
                'lon': gps_longitude,
                'recordTime': str(datetime.datetime.now()),
            }

            state_data = {
                'userId': activity_data['userId'],
                'state' : state
            }
            
            vitalSign_data = {
                "userId" : activity_data['userId'],
                "insertTime" : str(datetime.datetime.now()),
                "heartRate" : heart_rate, 
                "temp" : body_temperature,
                "o2" : oxygen_saturation, 
                "steps" : step_stride
            }
            
            # Key 중복 허용 
            message_body = 'Hello'
            message_deduplication_id = str(uuid.uuid4())
            
            # 데이터 SQS에 전송
            response = sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps([activity_data,device_data,gps_data, state_data, vitalSign_data], default=str),
                MessageGroupId='my-group' + str(activity_data['userId']),
                MessageDeduplicationId=message_deduplication_id,
            )
            print('1. Sent Activity data: ', activity_data)
            print('2. Sent Device data: ', device_data)
            print('3. Sent GPS data: ', gps_data)
            print('4. Sent State data: ', state_data)
            print('6. Sent vitalSign data: ', vitalSign_data)

        # 1초 대기
        time.sleep(3)
        # return response

send_sqs_messages()
