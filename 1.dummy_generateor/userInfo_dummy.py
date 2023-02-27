import random
import datetime
import time
import pandas as pd
from faker import Faker
import mysql.connector

import pymysql
from dotenv import load_dotenv
import os
import shortid

load_dotenv()

def main():
# 입력할 db 선택
    conn = pymysql.connect(user=os.getenv("mysql_user"), password=os.getenv("mysql_password"),
                               host=os.getenv("mysql_host"), database=os.getenv("mysql_database"), port=3306)

    try:
        with conn.cursor() as cursor:
            sql = 'INSERT INTO user (id, pw, name, age, contact, position, role, employedDate, workplace) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
            lst = insert_user_Info()
            cursor.executemany(sql, lst)
        conn.commit()
        
    finally:
        conn.close()

# 더미 데이터 생성
def create_user_info_dummy():
    # static 데이터
    # Define a list of static data for our construction workers
    worker_id = ['1001', '1002', '1003', '1004', '1005', '1006', '1007', '1008', '1009', '1010']
    names = ['최정인', '김민수', '손재우', '윤진식', '박경관', '금민경', '최한수', '이정민', '이창현', '김찬준']
    
    job_position = ['부장', '과장', '대리', '주임', '사원']
    job_role = ['철골', '조적', '방수', '미장', '목공', '콘크리트']
    job_workPlace = ['A', 'B', 'C']

    # 모델 학습 후 결과로 업데이트
    states = ['안전', '주의', '위험']
    
    # Define the range of values for worker ID and age
    worker_id_range = (1000, 1010)
    age_range = (25, 55)

    # Define an empty list to hold the generated data
    construction_workers = []

    fake = Faker("ko_KR")
    # Loop through each worker and generate their data for every second of the day
    for i, name in enumerate(names):
        id = worker_id[i]
        password = ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890', k=8))
        age = random.randint(*age_range)
        position = job_position[random.randint(0,len(job_position)-1)]
        role = job_role[random.randint(0,len(job_role)-1)]
        contact = fake.phone_number()
        workPlace = job_workPlace[random.randint(0,len(job_workPlace)-1)]
        hired_date = fake.date_between(start_date='-20y', end_date='today')
        row = {
            'WorkerID': id,
            'Password' : password,
            'Name': name,
            'Age': age,
            'Contact' : contact,
            'Position' : position,
            'Role' : role,
            'Workplace' : workPlace,
            'EmployedDate' : hired_date
        }
        construction_workers.append(row)
       
    return construction_workers


# user 테이블 정보 데이터에 insert
def insert_user_Info():
	user = create_user_info_dummy()
	list_user_data = []
	for i in range(len(user)):
		# 컬럼 리스트 생성
		id = user[i]['WorkerID']
		pw = user[i]['Password']
		name = user[i]['Name']
		age = user[i]['Age']
		contact = user[i]['Contact']
		position =user[i]['Position']
		role = user[i]['Role']
		employedDate =user[i]['EmployedDate']
		workplace = user[i]['Workplace']

		# 입력할 데이터 리스트 생성
		
		input_data = id, pw, name, age, contact, position, role, employedDate.strftime("%Y-%m-%d"), workplace
		list_user_data.append(input_data)
	
	return list_user_data

main()