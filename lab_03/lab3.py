
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
# print(DIR_LIST)

minimum_zipcode = 0
maximum_zipcode = 0
is_zipcode_first = True
male_active_records = 0
female_active_records = 0
active_records = 0
accumulated_log = []
phonenumber_output = []
has_added_header = False
for file_name in [i for i in DIR_LIST if re.match("^DESCRIBE_LOG_EVENTS_[0-9]{8}_[0-1]{1}[0-9]{1}[0-9]{4}.txt$", i)]:
    file_path = f"{PATH}/{file_name}"
    
    is_file = os.path.isfile(file_path)

    if is_file:
        with open(file_path, 'r') as file_log:
            log.info(f"Processed file : {file_path}")
            
            is_header = True
            line_count = 0
            with open(f'{file_path}.stg', 'w') as stg_file:

                for line in file_log:
                    if (is_header):
                        header = line.rstrip().split('|')
                        header.append('STG_SOURCE')
                        stg_file.write(f"{'|'.join(header)}\n")
                        if not (has_added_header):
                            accumulated_log.append(header)
                            has_added_header = True
                        is_header = False
                    else:
                        if not (re.match("^\d{4}.+$", line)):
                            continue
                        record = line.rstrip().split("|")
                        record.append(file_name)
                        accumulated_log.append(record)
                        stg_file.write(f"{'|'.join(record)}\n")

                        if (record[11] == 'active'):
                            active_records += 1
                        
                        if (record[12] == 'm' and record[11] == 'active'):
                            male_active_records += 1
                        elif (record[12] == 'f' and record[11] == 'active'):
                            female_active_records += 1
                        
                        if (is_zipcode_first):
                            maximum_zipcode = int(record[3])
                            minimum_zipcode = int(record[3])
                            is_zipcode_first = False

                        if (int(record[3]) > int(maximum_zipcode)):
                            maximum_zipcode = record[3]
                        
                        if (int(record[3]) < int(minimum_zipcode)):
                            minimum_zipcode = record[3]
                            
                        if (re.match("^\(\d{3}\)\d{3}-\d{4}$", record[10])):
                            phonenumber_output.append(f"{record[10]}\n")
                        
                        line_count += 1
with open("./output/special_phonenumber.txt", "w") as file:
    file.writelines(phonenumber_output)

with open("./output/accumulated_records.csv", "w") as file:
    for record in accumulated_log:
        line = ", ".join(record)
        # print(f"{line}/n")
        file.write(f"{line}\n")

log.info(f"Total processed record(s) : {len(accumulated_log)-1}")
log.info(f"Total active record(s) : {active_records}")
log.info(f"Total active record(s) in Gender Male : {male_active_records}")
log.info(f"Total active record(s) in Gender Female : {female_active_records}")
log.info(f"Minimum number of zipcode : {maximum_zipcode}")
log.info(f"Maximum number of zipcode : {minimum_zipcode}")
log.info(f"Total record(s) of special phone number format : {len(phonenumber_output)}")
