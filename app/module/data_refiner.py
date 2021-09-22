import csv
import sys
import os
import json
import subprocess
from datetime import datetime as dt
import database_handler as DH

DB = DH.Database()


def refine_t8411():

    path_dir_result = '../data/210919_t8144_refined'

    res = DB.executeAll('SHOW TABLES LIKE "%%t8411%%"')
    tables = [row[0] for row in res]

    # 완료 table 제외
    files_done = os.listdir(path_dir_result)
    tables_done = [file.replace('_refined.csv', '') for file in files_done]

    tables = [table for table in tables
              if table not in tables_done]

    for table in tables:
        flag_file_not_written = 1

        res = DB.executeAll(f'SELECT idx_god_follow FROM {table} ORDER BY idx_god_follow DESC LIMIT 1')
        len_table = int(res[0][0])
        print(table, len_table)

        # column 가져오기

        res = DB.executeAll(f'DESC {table}')
        table_columns = [row[0] for row in res]
        table_columns_dict = {}
        for i, column in enumerate(table_columns):
            table_columns_dict[column] = i

        # values 가져오기

        cycle_size = 10**6
        cycle_count = int(len_table / cycle_size) + 1

        for i in range(cycle_count):
            index_start = i * cycle_size + 1
            index_end = (i + 1) * cycle_size
            table_rows = DB.executeAll(f'SELECT * FROM {table} WHERE idx_god_follow BETWEEN {index_start} AND {index_end}')
            print(table, len(table_rows))

            # open, high, low, close 같은지 check
            flag_error = 0
            for row in table_rows:
                idx_god_follow = row[table_columns_dict['idx_god_follow']]
                c_open = row[table_columns_dict['c_open']]
                c_high = row[table_columns_dict['c_high']]
                c_low = row[table_columns_dict['c_low']]
                c_close = row[table_columns_dict['c_close']]
                c_jongchk = row[table_columns_dict['c_jongchk']]
                c_rate = row[table_columns_dict['c_rate']]
                c_pricechk = row[table_columns_dict['c_pricechk']]

                if not (c_open == c_high) and (c_high == c_low) and (c_low == c_close):
                    print(idx_god_follow, c_open, c_high, c_low, c_close, sep='\t')
                    flag_error = 1
                if not (c_jongchk == 0) and (c_rate == 0) and (c_pricechk == 0):
                    print(idx_god_follow, c_jongchk, c_rate, c_pricechk, sep='\t')
                    flag_error = 1

            # 오류 없으면 정제해서 기록

            if flag_error:
                break

            else:
                table_columns = [
                    'idx_god_follow',
                    'c_shcode',
                    'c_date',
                    'c_time',
                    'c_close',
                    'c_jdiff_vol'
                ]
                table_rows = [
                    [
                        row[table_columns_dict['idx_god_follow']],
                        row[table_columns_dict['c_shcode']],
                        row[table_columns_dict['c_date']],
                        row[table_columns_dict['c_time']],
                        row[table_columns_dict['c_close']],
                        row[table_columns_dict['c_jdiff_vol']]
                    ]
                    for row in table_rows]

                if flag_file_not_written:
                    with open(f'{path_dir_result}/{table}_refined.csv', 'w', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow(table_columns)
                        flag_file_not_written = 0

                with open(f'{path_dir_result}/{table}_refined.csv', 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerows(table_rows)
                    print(f'refined table {table}')





if __name__ == '__main__':
    refine_t8411()