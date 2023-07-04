# %%
import pandas as pd
import os
# from os import path
import re
from itertools import groupby
from operator import itemgetter


LIMIT = 100
PATH = "./input"
DIR_LIST = [i for i in os.listdir(PATH)]
print(DIR_LIST)


# %%
accumulated_log = []
has_added_header = False
_temp_filepaths = []
# for file_name in [i for i in DIR_LIST if re.match("^describe_log_events_\d{8}_\d{6}.txt$", i)]:
for file_name in [i for i in DIR_LIST if re.match("^DESCRIBE_LOG_EVENTS_[0-9]{8}_[0-1]{1}[0-9]{1}[0-9]{4}.txt$", i)]:
    file_path = f"{PATH}/{file_name}"
    
    is_file = os.path.isfile(file_path)

    if is_file:
        _temp_filepaths.append(file_path)
        with open(file_path, 'r') as file_log:
            print(f"File: {file_path}")
            is_header = True
            # i = 0
            line_count = 0
            # print(file_log.read())
            for line in file_log:
                if (is_header):
                    if not (has_added_header):
                        accumulated_log.append(line.split('|'))
                        has_added_header = True
                    is_header = False
                else:
                    # i = i + 1
                    # if (i >= 3):
                    #     break
                    # if (line_count == LIMIT):
                    #     break

                    if not (re.match("^\d{4}.+$", line)):
                        continue
                    # print(line.split('|'))
                    # print(f"line count: {line_count}")
                    accumulated_log.append(line.split("|"))
                    line_count += 1


    # break


# %%
print(len(accumulated_log))


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
# for record in accumulated_log:
#     if (filter_active(record)):
#         active_log.append(record)
#         i += 1

# print(type(active_logs))
print(f"active records : {len(active_logs)}")
# for i in active_logs:
#     print(i)  

# %%
group_by_gender_output = []
_temp_genders = []
for _, value in groupby(sorted(active_logs, key=itemgetter(12)), key=itemgetter(12)):
    value = list(value)
    value_count = len(value)
    print(f"gender: {value[0][12]} | count: {value_count}")
    _temp_genders.append(f"gender: {value[0][12]} | count: {value_count}")
    # break
    # print(value)
    group_by_gender_output.append(value)

# %%
sorted_by_zipcode = sorted(active_logs, key=itemgetter(3))
_temp_zipcode = f"minimum zipcode: {sorted_by_zipcode[0][3]}\nmaximum zipcode: {sorted_by_zipcode[-1][3]}"
print(f"minimum zipcode: {sorted_by_zipcode[0][3]}\nmaximum zipcode: {sorted_by_zipcode[-1][3]}")

# %%
def filter_phonenumber_format(record):
    if (re.match("^\(\d{3}\)\d{3}-\d{4}$", record[10])):
        return True
    else:
        return False

phonenumber_output = []
phonnumber_formats = list(filter(filter_phonenumber_format, active_logs))
print(len(phonnumber_formats))
# for i in phonnumber_formats:
#     print(i[10])
with open("./special_phonenumber.txt", "w") as file:
    for i in phonnumber_formats:
        file.write(f'{i[10]}\n')
    # file.writelines(phonnumber_formats)

# %%
print("Summary")

print(f"Processed files:")
for i in _temp_filepaths:
    print(i)
print(f"Total records: {len(accumulated_log)-1}")

print(f"status active records : {len(active_logs)}")

for i in _temp_genders:
    print(i)

print(_temp_zipcode)

answers_text = f"""

""" 

# %%



