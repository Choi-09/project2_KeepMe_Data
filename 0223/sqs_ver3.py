import boto3
import json
import random
import time
import hashlib
import datetime
import mysql.connector
import uuid

from faker import Faker

# MySQL 데이터베이스 설정
mysql_host = 'keepme.c2ve8hvd51l9.ap-northeast-2.rds.amazonaws.com'
mysql_user = 'admin'
mysql_password = 'tiger123'
mysql_database = 'keepme'
mysql_port = '3306'

conn = mysql.connector.connect(user=mysql_user, password=mysql_password,
                               host=mysql_host, database=mysql_database, port=mysql_port)
cursor = conn.cursor(prepared=True)
sqs = boto3.client('sqs')
queue_url = 'https://sqs.ap-northeast-2.amazonaws.com/512431715371/dummy.fifo'


while True:
    # 더미데이터 초기값 설정

    # 1. activity table 컬럼 초기값
    # 작업자 id 범위 설정
    worker_id_range = (1000, 1010)
    accelerometer_x_range = (-2, 2)
    accelerometer_y_range = (-2, 2)
    accelerometer_z_range = (-2, 2)
    gyrosensor_x_range = (-10, 10)
    gyrosensor_y_range = (-10, 10)
    gyrosensor_z_range = (-10, 10)

    gyrosensor_x = random.uniform(*gyrosensor_x_range)
    gyrosensor_y = random.uniform(*gyrosensor_y_range)
    gyrosensor_z = random.uniform(*gyrosensor_z_range)
    accelerometer_x = random.uniform(*accelerometer_x_range)
    accelerometer_y = random.uniform(*accelerometer_y_range)
    accelerometer_z = random.uniform(*accelerometer_z_range)

    gyrosensor_x += random.uniform(-0.05, 0.05)
    gyrosensor_y += random.uniform(-0.05, 0.05)
    gyrosensor_z += random.uniform(-0.05, 0.05)
    accelerometer_x += random.uniform(-0.02, 0.02)
    accelerometer_y += random.uniform(-0.02, 0.02)
    accelerometer_z += random.uniform(-0.02, 0.02)

    # 2. device table 컬럼 초기값
    device = ['ph', 'w']
    battery_range = (0, 100)
    battery_stddev = 2
    battery = 100 
    battery -= random.normalvariate(0, battery_stddev)

    # 3. gps table 컬럼 초기값
    gps_latitude_range = [35.168008 , 35.169691] 
    gps_latitude = round(random.uniform(*gps_latitude_range), 6)  
    gps_latitude += random.uniform(-0.0001, 0.0001)
    
    gps_longitude_range = [129.139300, 129.141249]
    gps_longitude = round(random.uniform(*gps_longitude_range), 6)
    gps_longitude += random.uniform(-0.0001, 0.0001)
    
    # 4. state table 컬럼 초기값
    state = ['안전', '주의', '위험']

    # 5. user table 컬럼 초기값
    fake = Faker()
    names = ['최정인', '김민수', '손재우', '윤진식', '박경관', '금민경', '최한수', '이정민', '이창현', '김찬준']
    age_range = (25, 55)
    job_position = ['부장', '과장', '대리', '주임', '사원']
    job_role = ['철골', '조적', '방수', '미장', '목공', '콘크리트']
    job_workPlace = ['A', 'B', 'C']
    

    for name in names:
        password = ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890', k=8))
        age = random.randint(*age_range)
        contact = fake.phone_number()
        position = job_position[random.randint(0,len(job_position)-1)]
        role = job_role[random.randint(0,len(job_role)-1)]
        workPlace = job_workPlace[random.randint(0,len(job_workPlace)-1)]
        hired_date = fake.date_between(start_date='-20y', end_date='today')

    # 6. vitalSign table 컬럼 초기값 
    # Define the standard deviation for the normal distribution of each vital sign
    heart_rate_stddev = 5
    oxygen_saturation_stddev = 1
    body_temperature_stddev = 0.1
    step_stride_stddev = 2
    
    heart_rate_range = (60, 100)
    heart_rate = random.randint(*heart_rate_range)
    heart_rate += random.normalvariate(0, heart_rate_stddev)

    oxygen_saturation_range = (95, 100)
    oxygen_saturation = random.randint(*oxygen_saturation_range)
    oxygen_saturation += random.normalvariate(0, oxygen_saturation_stddev)
    
    body_temperature_range = (36.0, 37.5)
    body_temperature = round(random.uniform(*body_temperature_range), 1)
    body_temperature += random.normalvariate(0, body_temperature_stddev)

    step_stride_range = (60, 80)
    step_stride = random.randint(*step_stride_range)
    step_stride += random.normalvariate(0, step_stride_stddev)

    # 7. work_schedules table 초기값


    # 랜덤 더미 데이터 생성
    activity_data = {
        'userId' : random.randint(*worker_id_range),
        "accX" : max(min(round(accelerometer_x, 17), accelerometer_x_range[1]), accelerometer_x_range[0]),
        "accY" : max(min(round(accelerometer_y, 17), accelerometer_y_range[1]), accelerometer_y_range[0]),
        "accZ" : max(min(round(accelerometer_z, 17), accelerometer_z_range[1]), accelerometer_z_range[0]),
        "gyroX" : max(min(round(gyrosensor_x, 17), gyrosensor_x_range[1]), gyrosensor_x_range[0]),
        "gyroY" : max(min(round(gyrosensor_y, 17), gyrosensor_y_range[1]), gyrosensor_y_range[0]),
        "gyroZ" : max(min(round(gyrosensor_z, 17), gyrosensor_z_range[1]), gyrosensor_z_range[0])
    }

    device_data = {
        'userId' : activity_data['userId'],
        "deviceName" : device[random.randint(0,1)],
        'battery' : max(min(round(battery), battery_range[1]), battery_range[0])
    }
    gps_data = {
        'userId': activity_data['userId'],
        'lat': max(min(round(gps_latitude, 6), gps_latitude_range[1]), gps_latitude_range[0]),
        'lon': max(min(round(gps_longitude, 6), gps_longitude_range[1]), gps_longitude_range[0]),
        'recordTime': str(datetime.datetime.now())
    }

    state_data = {
        'userId': activity_data['userId'],
        'state' : state[random.randint(0,2)] ## 모델 학습 후에는 결과를 반영시켜야 함.
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
        "heartRate" : max(min(round(heart_rate), heart_rate_range[1]), heart_rate_range[0]), 
        "temp" : max(min(round(body_temperature, 1), body_temperature_range[1]), body_temperature_range[0]),
        "o2" : max(min(round(oxygen_saturation), oxygen_saturation_range[1]), oxygen_saturation_range[0]), 
        "steps" : max(min(round(step_stride), step_stride_range[1]), step_stride_range[0])
    }

    message_body = 'Hello'
    message_deduplication_id = str(uuid.uuid4())
    
    # GPS 데이터 SQS에 전송
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps([activity_data, gps_data, vitalSign_data]),
        MessageGroupId='my-group' + str(activity_data['userId']),
        MessageDeduplicationId=message_deduplication_id,
    )
    print('1. Sent Activity data: ', activity_data)
    print('2. Sent Device data: ', device_data)
    print('3. Sent GPS data: ', gps_data)
    print('4. Sent State data: ', state_data)
    print('5. Sent User data: ', user_data)
    print('6. Sent vitalSign data: ', vitalSign_data)

    # 10초 대기
    time.sleep(1)

    # receive messages from the queue
    response = sqs.receive_message(
        QueueUrl='https://sqs.ap-northeast-2.amazonaws.com/512431715371/dummy.fifo',
        MaxNumberOfMessages=10,  # maximum number of messages to receive
        WaitTimeSeconds=20,  # maximum time to wait for a message (in seconds)
    )

    # process the messages
    if 'Messages' in response:
        for message in response['Messages']:
            # 메시지 데이터 추출
            # 메시지 데이터 json형식으로 받음
            message_data = eval(message['Body'])

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
                    val4 = (message_data[3]['userId'], message_data[3]['lat'])

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
                    # cursor.execute(sql3, val3)
                    # cursor.execute(sql4, val4)
                    # cursor.execute(sql5, val5)
                    # cursor.execute(sql6, val6)
                
                    conn.commit()

                # delete the message from the queue
                sqs.delete_message(
                    QueueUrl='https://sqs.ap-northeast-2.amazonaws.com/512431715371/dummy.fifo',
                    ReceiptHandle=message['ReceiptHandle'],
                )
