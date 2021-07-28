from operator import is_

import pandas as pd
import re
import csv
import sqlite3
import json
from lxml import etree
import math

print('Input file name')

filename = input()
filename_short = filename.split('.')[0]
is_checked = False
is_database = False

if 'CHECKED' in filename:
    is_checked = True
    filename_short = filename.split('[')[0]
elif '.s3db' in filename:
    is_database = True

if '.xlsx' in filename:
    my_df = pd.read_excel(filename, sheet_name='Vehicles', dtype=str)
    my_df = my_df[['vehicle_id', 'engine_capacity', 'fuel_consumption', 'maximum_load']]
    shape = my_df.shape
    shape_str = f'{shape[0]} line was'
    if shape[0] > 1:
        shape_str = f'{shape[0]} lines were'

    my_df.to_csv(f'{filename_short}.csv')
    print(f'{shape_str} added to {filename_short}.csv')

    my_df.to_csv(f'{filename_short}.csv')

if not is_checked and not is_database:
    data_updated = []
    with open(f"{filename_short}.csv", newline='', encoding='utf-8') as csv_data:
        with open(f"{filename_short}[CHECKED].csv", "w", encoding='utf-8') as csv_data_writer:

            file_reader = csv.DictReader(csv_data, delimiter=",")
            file_writer = csv.writer(csv_data_writer, delimiter=",", lineterminator="\n")

            file_writer.writerow(['vehicle_id', 'engine_capacity', 'fuel_consumption', 'maximum_load'])
            count_corrected = 0
            for line in file_reader:
                line_updated = []
                for key in line:
                    if key != '':
                        if not line[key].isdigit():
                            count_corrected += 1
                        val = re.sub("\D", "", line[key])
                        if val == '':
                            val = 0
                        line_updated.append(val)

                file_writer.writerow(line_updated)

        if count_corrected == 1:
            print(f"1 cell was corrected in {filename_short}[CHECKED].csv")
        else:
            print(f"{count_corrected} cells were corrected in {filename_short}[CHECKED].csv")

with sqlite3.connect(f'{filename_short}.s3db') as conn:
    cursor = conn.cursor()

    if not is_database:
        cursor.execute('DROP TABLE IF EXISTS convoy')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS convoy (
                vehicle_id INTEGER PRIMARY KEY,
                engine_capacity INTEGER NOT NULL,
                fuel_consumption INTEGER NOT NULL,
                maximum_load INTEGER NOT NULL,
                score INTEGER NOT NULL
            )
        ''')

        conn.commit()

        with open(f"{filename_short}[CHECKED].csv", newline='') as csv_data:
            file_reader = csv.DictReader(csv_data, delimiter=",")
            count = 0

            for line in file_reader:

                pitstops = math.floor(450 / (int(line['engine_capacity']) / int(line['fuel_consumption']) * 100))
                if pitstops == 0:
                    score = 2
                elif pitstops == 1:
                    score = 1

                if int(line['fuel_consumption']) * 4.5 < 230:
                    score += 2
                else:
                    score += 1

                if int(line['maximum_load']) >= 20:
                    score += 2

                result = cursor.execute(f'''
                    INSERT INTO convoy ('vehicle_id', 'engine_capacity', 'fuel_consumption', 'maximum_load', 'score')
                    VALUES ({line['vehicle_id']}, 
                            {line['engine_capacity']}, 
                            {line['fuel_consumption']}, 
                            {line['maximum_load']}, 
                            {score}
                    ) 
                ''')
                count += 1
            conn.commit()

            if count == 1:
                print(f'{count} record was inserted into {filename_short}.s3db')
            else:
                print(f'{count} records were inserted into {filename_short}.s3db')

    with open(f'{filename_short}.json', 'w') as json_file:
        cursor.execute('SELECT * FROM convoy')
        json_data = {'convoy': []}
        count = 0
        for item in cursor.fetchall():
            if item[4] > 3:
                json_data['convoy'].append({
                    "vehicle_id": item[0],
                    "engine_capacity": item[1],
                    "fuel_consumption": item[2],
                    "maximum_load": item[3]
                })
                count += 1

        json.dump(json_data, json_file)
        if count == 1:
            print(f'{count} vehicle was saved into {filename_short}.json')
        else:
            print(f'{count} vehicles were saved into {filename_short}.json')

    cursor.execute('SELECT * FROM convoy')
    count = 0
    xml_str = ""
    for item in cursor.fetchall():
        if item[4] <= 3:
            xml_item = f'<vehicle_id>{item[0]}</vehicle_id>'
            xml_item += f'<engine_capacity>{item[1]}</engine_capacity>'
            xml_item += f'<fuel_consumption>{item[2]}</fuel_consumption>'
            xml_item += f'<maximum_load>{item[3]}</maximum_load>'
            xml_str += f'<vehicle>{xml_item}</vehicle>'
            count += 1

    xml_str = f'<convoy>{xml_str}</convoy>'
    root = etree.fromstring(xml_str)
    etree = etree.ElementTree(root)
    if count == 0:
        with open(f'{filename_short}.xml', 'w') as xml_file:
            xml_file.write(xml_str)
    else:
        etree.write(f'{filename_short}.xml')


    if count == 1:
        print(f'{count} vehicle was saved into {filename_short}.xml')
    else:
        print(f'{count} vehicles were saved into {filename_short}.xml')