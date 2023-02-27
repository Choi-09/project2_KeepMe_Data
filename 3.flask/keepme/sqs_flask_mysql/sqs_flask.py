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

fake= Faker()
def send_sqs_messages():   
    while True:
        # 더미데이터 초기값 설정
        # Define the range of values for each vital sign
        heart_rate_range = (60, 100)
        oxygen_saturation_range = (95, 100)
        body_temperature_range = (36.0, 37.5)
        step_stride_range = (60, 80)

        # Define a list of 10 names for our construction workers
        names = ['최정인', '김민수', '손재우', '윤진식', '박경관', '금민경', '최한수', '이정민', '이창현', '김찬준']

        # Define the standard deviation for the normal distribution of each vital sign
        heart_rate_stddev = 5
        oxygen_saturation_stddev = 1
        body_temperature_stddev = 0.1
        step_stride_stddev = 2

        # Define the range of values for GPS coordinates, gyrosensor readings, and accelerometer readings
        gps_latitude_range = [35.168008 , 35.169691]  # Busan, Republic of Korea
        gps_longitude_range = [129.139300, 129.141249]  # Busan, Republic of Korea
        gyrosensor_x_range = (-10, 10)
        gyrosensor_y_range = (-10, 10)
        gyrosensor_z_range = (-10, 10)
        accelerometer_x_range = (-2, 2)
        accelerometer_y_range = (-2, 2)
        accelerometer_z_range = (-2, 2)

        # Define the range of values for worker ID and age
        worker_id_range = (1000, 1010)
        age_range = (25, 55)
        device = ['ph', 'w']
        battery_range = (0, 100)
        battery_stddev = 2
        battery = 100 
        states = ['안전', '주의', '위험']
        job_position = ['부장', '과장', '대리', '주임', '사원']
        job_role = ['철골', '조적', '방수', '미장', '목공', '콘크리트']
        job_workPlace = ['A', 'B', 'C']

        for name in names:
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
            worker_id = random.randint(*worker_id_range)
            age = random.randint(*age_range)
            password = ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890', k=8))
            battery -= random.randint(0, battery_stddev)
            hired_date = fake.date_between(start_date='-20y', end_date='today')
            contact = fake.phone_number()
            
            heart_rate += random.normalvariate(0, heart_rate_stddev)
            heart_rate = max(min(round(heart_rate), heart_rate_range[1]), heart_rate_range[0])
            oxygen_saturation += random.normalvariate(0, oxygen_saturation_stddev)
            oxygen_saturation = max(min(round(oxygen_saturation), oxygen_saturation_range[1]), oxygen_saturation_range[0])
            body_temperature += random.normalvariate(0, body_temperature_stddev)
            body_temperature = max(min(round(body_temperature, 1), body_temperature_range[1]), body_temperature_range[0])
            step_stride += random.normalvariate(0, step_stride_stddev)
            step_stride = max(min(round(step_stride), step_stride_range[1]), step_stride_range[0])
            gps_latitude += random.uniform(-0.0001, 0.0001)
            gps_latitude = max(min(round(gps_latitude, 6), gps_latitude_range[1]), gps_latitude_range[0])
            gps_longitude += random.uniform(-0.0001, 0.0001)
            gps_longitude = max(min(round(gps_longitude, 6), gps_longitude_range[1]), gps_longitude_range[0])
            gyrosensor_x += random.uniform(-0.05, 0.05)
            gyrosensor_x = max(min(round(gyrosensor_x, 17), gyrosensor_x_range[1]), gyrosensor_x_range[0])
            gyrosensor_y += random.uniform(-0.05, 0.05)
            gyrosensor_y = max(min(round(gyrosensor_y, 17), gyrosensor_y_range[1]), gyrosensor_y_range[0])
            gyrosensor_z += random.uniform(-0.05, 0.05)
            gyrosensor_z = max(min(round(gyrosensor_z, 17), gyrosensor_z_range[1]), gyrosensor_z_range[0])
            accelerometer_x += random.uniform(-0.02, 0.02)
            accelerometer_x = max(min(round(accelerometer_x, 17), accelerometer_x_range[1]), accelerometer_x_range[0])
            accelerometer_y += random.uniform(-0.02, 0.02)
            accelerometer_y = max(min(round(accelerometer_y, 17), accelerometer_y_range[1]), accelerometer_y_range[0])
            accelerometer_z += random.uniform(-0.02, 0.02)
            accelerometer_z = max(min(round(accelerometer_z, 17), accelerometer_z_range[1]), accelerometer_z_range[0])
            battery = max(min(round(battery), battery_range[1]), battery_range[0])
            position = job_position[random.randint(0,len(job_position)-1)]
            role = job_role[random.randint(0,len(job_role)-1)]
            workPlace = job_workPlace[random.randint(0,len(job_workPlace)-1)]
            deviceName = device[random.randint(0,1)]
            state = states[random.randint(0,2)] ## 모델 학습 후에는 결과를 반영시켜야 함.

            # 랜덤 더미 데이터 생성
            activity_data = {
                'userId' : worker_id,
                "accX" : accelerometer_x,
                "accY" : accelerometer_y,
                "accZ" : accelerometer_z,
                "gyroX" : gyrosensor_x,
                "gyroY" : gyrosensor_y,
                "gyroZ" : gyrosensor_z
            }

            device_data = {
                'userId' : activity_data['userId'],
                "deviceName" : deviceName,
                'battery' : battery
            }

            gps_data = {
                'userId': activity_data['userId'],
                'lat': gps_latitude,
                'lon': gps_longitude,
                'recordTime': str(datetime.datetime.now())
            }

            state_data = {
                'userId': activity_data['userId'],
                'state' : state
            }
            
            user_data = {
                "id" :  activity_data['userId'],
                "pw"  : password,
                "name" : name,
                "age" : age,
                "contact" : contact,
                "position" : position,
                "role"  : role,
                "employDate"  : hired_date,
                "workplace"  : workPlace
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
                MessageBody=json.dumps([activity_data, device_data, gps_data, state_data, user_data, vitalSign_data], default=str),
                MessageGroupId='my-group' + str(activity_data['userId']),
                MessageDeduplicationId=message_deduplication_id,
            )
            print('1. Sent Activity data: ', activity_data)
            print('2. Sent Device data: ', device_data)
            print('3. Sent GPS data: ', gps_data)
            print('4. Sent State data: ', state_data)
            print('5. Sent User data: ', user_data)
            print('6. Sent vitalSign data: ', vitalSign_data)

            # 1초 대기
            time.sleep(3)

