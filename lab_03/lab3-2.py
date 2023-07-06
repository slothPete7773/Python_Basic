import os 
import re
import logging
import mariadb
import sys

HOST = "localhost"
PORT = "8181"
USER = "root"
PASSWD = "example"
try:
    connection = mariadb.connect(
        user=USER,
        password=PASSWD,
        host=HOST,
        port=3306,
        local_infile=True
        # database="mysql"
    )
    
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

cursor = connection.cursor()
print(cursor)

def list_database():
    statement = "show databases"
    try:    
        cursor.execute(statement)
        connection.commit()
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1) 
    return [ i[0] for i in cursor]

def create_database():
    # %s
    statement = """
CREATE DATABASE stg CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci
"""
    try:    
        cursor.execute(statement)
        connection.commit()
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1) 
    
    databases = list_database()
    print(databases)
    if ('stg' in databases):
        print('successfully created database')
    else:
        print('there was error during database creation')
        sys.exit(1)
    
        

def create_tables():
    statement = """
CREATE TABLE stg.LOG_EVENTS (
DATE_TIME varchar(123),
NAME VARCHAR(128),
CITY VARCHAR(128),
ZIPCODE VARCHAR(128),
BBAN VARCHAR(128),
LOCALE VARCHAR(128),
BANK_COUNTRY VARCHAR(128),
IBAN VARCHAR(128),
COUNTRY_CALLING_CODE VARCHAR(128),
MSISDN VARCHAR(128),
PHONE_NUMBER VARCHAR(128),
STATUS VARCHAR(128),
GENDER VARCHAR(128),
STG_SOURCE VARCHAR(123)
)
"""
    try:    
        cursor.execute(statement)
        connection.commit()
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1) 
    
    try: 
        statement_2 = """
describe stg.LOG_EVENTS
"""
        cursor.execute(statement_2)
        print('successfully created table')
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1) 

def populate_data(file_path):
    statement = """
load data local infile %s 
into table stg.LOG_EVENTS 
fields terminated by '|' 
(DATE_TIME,NAME,CITY,ZIPCODE,BBAN,LOCALE,BANK_COUNTRY,IBAN,COUNTRY_CALLING_CODE,MSISDN,PHONE_NUMBER,STATUS,GENDER,STG_SOURCE)
"""
    # print(f'from populate_data : filepath: {file_path}')
    try:    
        cursor.execute(statement, (file_path,))
        connection.commit()
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1) 


    print('successfully populated database')

def drop_database():
    statement = """
DROP DATABASE stg
"""
    try:    
        cursor.execute(statement)
        connection.commit()
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1) 
    print('successfully dropped database')

if __name__ == '__main__':
    # create_database()
    drop_database()
    create_database()
    create_tables()
    
    PATH = "./input"
    DIR_LIST = [i for i in os.listdir(PATH)]
    total_records = 0
    for file_name in [i for i in DIR_LIST if re.match("^DESCRIBE_LOG_EVENTS_[0-9]{8}_[0-1]{1}[0-9]{1}[0-9]{4}.txt$", i) and os.path.isfile(f"{PATH}/{i}")]:
        file_path = f"{PATH}/{file_name}" 
        is_header = True
        with open(f'{file_path}.stg', 'r') as file:
            print(f'{file_path}.stg')
            with open(f'{file_path}.tmp', 'w') as temp_file:
                lines = file.readlines()
                lines = lines[1:]
                print(len(lines))
                temp_file.writelines(lines)
                total_records += len(lines)
            populate_data(f'{file_path}.tmp')
            os.remove(f'{file_path}.tmp')
        os.remove(f'{file_path}.stg')
    # Q4:
    print(f'Total records : src file : {total_records}')
    cursor.execute('select count(1) from stg.LOG_EVENTS')
    print(f'Total records : tgt db : {list(cursor)[0][0]}')
    
# Q7
# select 
#  PHONE_NUMBER as phone_number
#  , STG_SOURCE as source_filename
# from stg.LOG_EVENTS
# where PHONE_NUMBER regexp '^\\([0-9]{3}\\)[0-9]{3}-[0-9]{4}$'
# -- regexp '^\([0-9]{3}\)[0-9]{3}-[0-9]{4}$'
# -- group by source_filename, phone_number
# order by phone_number desc, source_filename 
# limit 100