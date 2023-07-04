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

# %%
def filter_active(record):
    if (record[11] == 'active'):
        return True
    else:
        return False
        
i = 0
active_logs = list(filter(filter_active, accumulated_log))
log.info(f"Total active record(s) : {len(active_logs)}")

# %%
group_by_gender_output = []
for _, value in groupby(sorted(active_logs, key=itemgetter(12)), key=itemgetter(12)):
    value = list(value)
    value_count = len(value)
    log.info(f"Total active record(s) in Gender [{value[0][12]}] : {value_count}")
    group_by_gender_output.append(value)

# %%
sorted_by_zipcode = sorted(accumulated_log[1:], key=itemgetter(3))
log.info(f"Minimum number of zipcode : {sorted_by_zipcode[0][3]}")
log.info(f"Maximum number of zipcode : {sorted_by_zipcode[-1][3]}")

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


