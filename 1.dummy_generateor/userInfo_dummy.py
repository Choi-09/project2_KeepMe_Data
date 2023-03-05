import random
import datetime
import time
import pandas as pd
from faker import Faker
import mysql.connector

import pymysql
from dotenv import load_dotenv
import os

load_dotenv()


worker_id = ['1001', '1002', '1003', '1004', '1005', '1006', '1007', '1008', '1009', '1010']

start_date = datetime.date(1968, 1, 1)
end_date = datetime.date(1998, 12, 31)

def main():
    # 입력할 db 선택
    conn = pymysql.connect(user=os.getenv("mysql_user"), password=os.getenv("mysql_password"),
    host=os.getenv("mysql_host"), database=os.getenv("mysql_database"), port=3306)

    try:
        with conn.cursor() as cursor:
            sql = 'INSERT INTO worker (workerId, name, birth, contact, email, position, role, employedDate, workplace) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
            lst = insert_worker_Info()

            cursor.executemany(sql, lst)
            conn.commit()

    finally:
        conn.close()

# 더미 데이터 생성
def create_worker_info_dummy():
    # static 데이터
    # Define a list of static data for our construction workers
    names = ['최정인', '김민수', '손재우', '윤진식', '박경관', '금민경', '최한수', '이정민', '이창현', '김찬준']
    
    job_position = ['부장', '과장', '대리', '주임', '사원']
    job_role = ['철골', '조적', '방수', '미장', '목공', '콘크리트']
    job_workPlace = ['A', 'B', 'C']

    # 모델 학습 후 결과로 업데이트
    status = ['안전', '주의', '위험']

    # Define an empty list to hold the generated data
    construction_workers = []

    fake = Faker("ko_KR")
    # Loop through each worker and generate their data for every second of the day
    for i, name in enumerate(names):
        id = worker_id[i]
        # password = ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890', k=8))
        birth = fake.date_between(start_date=start_date, end_date=end_date)
        contact = fake.phone_number()
        email = fake.email(domain='example.com')
        position = job_position[random.randint(0,len(job_position)-1)]
        role = job_role[random.randint(0,len(job_role)-1)]
        hiredDate = fake.date_between(start_date='-20y', end_date='today')
        workPlace = job_workPlace[random.randint(0,len(job_workPlace)-1)]
        row = {
            'workerId': id,
            'name' : name,
            'birth': birth,
            'contact': contact,
            'email' : email,
            'position' : position,
            'role' : role,
            'employedDate' : hiredDate,
            'workplace' : workPlace
        }
        construction_workers.append(row)     
    return construction_workers

# user 테이블 정보 데이터에 insert
def insert_worker_Info():
    user = create_worker_info_dummy()
    list_worker_data = []
    for i in range(len(user)):
		# 컬럼 리스트 생성
        id = user[i]['workerId']
        name = user[i]['name']
        birth = user[i]['birth']
        contact = user[i]['contact']
        email = user[i]['email']
        position =user[i]['position']
        role = user[i]['role']
        employedDate =user[i]['employedDate']
        workplace = user[i]['workplace']
        
		# 입력할 데이터 리스트 생성
        input_data = id, name, birth, contact, email, position, role, employedDate.strftime("%Y-%m-%d"), workplace
        list_worker_data.append(input_data)
    return list_worker_data

main()
