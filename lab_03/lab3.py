# %% [markdown]
# # Prepare libraries

# %%
import pandas as pd
import sys
import os
# from os import path
import re
from itertools import groupby
from operator import itemgetter
import logging
import logging.handlers
log_formatter = logging.Formatter('[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')
logging.basicConfig(stream=sys.stdout, format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)

LIMIT = 100
PATH = "./input"
DIR_LIST = [i for i in os.listdir(PATH)]
print(DIR_LIST)


# %% [markdown]
# # Loading files
# 
# This following section loading the event log files to the program.
# 
# The valid files must conform to the following format: `DESCRIBE_LOG_EVENTS_{YYYYMMDD}_{HHMMSS}` if any character is different, or the time is wrong that file is discarded.
# 
# Valid files are as follow: 
# 
# ```
# DESCRIBE_LOG_EVENTS_20230625_154002.txt
# DESCRIBE_LOG_EVENTS_20230625_160416.txt
# DESCRIBE_LOG_EVENTS_20230625_160558.txt
# ```
# 
# The program also removed trailling newline, and appended every records up to a single list. 
# 
# ---
# 
# โค้ดต่อไปนี้เป็นขั้นตอนการโหลดไฟล์ Event Log เข้ามาทำการประมวลผล
# 
# ซึ่งไฟล์ที่ถูกต้องจะต้องมีรูปแบบดังต่อไปนี้: `DESCRIBE_LOG_EVENTS_{YYYYMMDD}_{HHMMSS}` หากมีตัวอักษรตัวใดตัวหนึ่ง หรือเวลาไม่ถูกต้อง ไฟล์นั้นจะถูกคัดออก
# 
# ไฟล์ที่ถูกต้องคือ:
# 
# ```
# DESCRIBE_LOG_EVENTS_20230625_154002.txt
# DESCRIBE_LOG_EVENTS_20230625_160416.txt
# DESCRIBE_LOG_EVENTS_20230625_160558.txt
# ```
# 
# และตัวโปรแกรมยังทำการตัด newline ที่ท้ายบรรทัด และทำการนำทุก ๆ แถว ในทุก ๆ ไฟล์มารวมกันเป็น list อันเดียว เพื่อง่ายต่อการทำงาน

# %%
accumulated_log = []
has_added_header = False
for file_name in [i for i in DIR_LIST if re.match("^DESCRIBE_LOG_EVENTS_[0-9]{8}_[0-1]{1}[0-9]{1}[0-9]{4}.txt$", i)]:
    file_path = f"{PATH}/{file_name}"
    
    is_file = os.path.isfile(file_path)

    if is_file:
        with open(file_path, 'r') as file_log:
            log.info(f"Processed file : {file_path}")
            
            is_header = True
            line_count = 0
            for line in file_log:
                if (is_header):
                    if not (has_added_header):
                        accumulated_log.append(line.split('|'))
                        has_added_header = True
                    is_header = False
                else:
                    if not (re.match("^\d{4}.+$", line)):
                        continue
                    accumulated_log.append(line.rstrip().split("|"))
                    line_count += 1


# %%
log.info(f"Total processed record(s) : {len(accumulated_log)-1}")


# %%
list(enumerate(accumulated_log[0]))

# %% [markdown]
# # Counting Active Records
# 
# This section count the number of records with the column `STATUS` that has value `active`.
# 
# It simply use `filter` method and a filter function to determine whether the record has value `active` in column `STATUS`.
# 
# ---
# 
# ส่วนนี้ใช้สำหรับนับจำนวนแถวที่มีคอลัมน์ `STATUS` มีค่าเป็น `active`
# 
# โดยตัวโปรแกรมจะใช้เมธอด `filter` และฟังก์ชันเพื่อกรองและตรวจสอบว่าแถวนั้นมีค่า `active` ในคอลัมน์ `STATUS` หรือไม่

# %%
def filter_active(record):
    if (record[11] == 'active'):
        return True
    else:
        return False
        
active_logs = list(filter(filter_active, accumulated_log))
log.info(f"Total active record(s) : {len(active_logs)}")

# %% [markdown]
# # Counting Active Records in Each Gender
# 
# This section also count the number of records with value `active` in column `STATUS`, in each `GENDER`.
# 
# It use the `groupby` method to organize sorted records using the column `GENDER`.
# 
# ---
# 
# ส่วนนี้ยังคำนวณจำนวนฉถวที่มีค่า `active` ในคอลัมน์ `STATUS` แยกตามเพศด้วย
# 
# โดยในโปรแกรมจะใช้เมธอด `groupby` เพื่อจัดกลุ่มแถวทั้งหมด โดยจำแนกไปตามค่าในคอลัมน์ `GENDER`

# %%
group_by_gender_output = []
for _, value in groupby(sorted(active_logs, key=itemgetter(12)), key=itemgetter(12)):
    value = list(value)
    value_count = len(value)
    log.info(f"Total active record(s) in Gender [{value[0][12]}] : {value_count}")
    group_by_gender_output.append(value)

# %% [markdown]
# # Find Minimum and Maximum Zipcode
# 
# This section simply find the minimum and maximum of zipcode value. Using the built-in `sorted` method, to sort all records, then take the first record, and the last record, which represent the top and the bottom of sorted records.
# 
# ---
# 
# ส่วนนี้เพียงแค่ค้นหา Zipcode ที่มีค่าต่ำสุดและสูงสุด โดยใช้เมธอด `sorted` เพื่อเรียงลำดับแถวทั้งหมด จากนั้นเลือกแถวแรกและบันทึกสุดท้ายที่แสดงถึงส่วนบนและส่วนล่างของบันทึกที่เรียงลำดับ

# %%
sorted_by_zipcode = sorted(accumulated_log[1:], key=itemgetter(3))
log.info(f"Minimum number of zipcode : {sorted_by_zipcode[0][3]}")
log.info(f"Maximum number of zipcode : {sorted_by_zipcode[-1][3]}")

# %% [markdown]
# # Extracting Special Phone Number Format
# 
# This section extract the column `PHONE_NUMBER` with the specific format as followed: `(XXX)XXX-XXXX`. Then, output the extracted phone numbers into a text file.
# 
# ---
# 
# ส่วนนี้นำคอลัมน์ `PHONE_NUMBER` ที่มีรูปแบบเฉพาะดังต่อไปนี้: `(XXX)XXX-XXXX` แล้วนำออกไปเป็นไฟล์ข้อความชื่อ `special_phonenumber.txt`

# %%
def filter_phonenumber_format(record):
    if (re.match("^\(\d{3}\)\d{3}-\d{4}$", record[10])):
        return True
    else:
        return False

phonenumber_output = []
phonnumber_formats = list(filter(filter_phonenumber_format, accumulated_log))
log.info(f"Total record(s) of special phone number format : {len(phonnumber_formats)}")
with open("./special_phonenumber.txt", "w") as file:
    for i in phonnumber_formats:
        file.write(f'{i[10]}\n')


