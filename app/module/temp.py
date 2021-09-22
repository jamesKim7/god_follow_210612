import csv

import database_handler
import pandas as pd
import time
import csv
import sys

DH = database_handler.Database()

tables_cum = {
            't8413': 'xing_day_candle_chart'
    }

a = 12

def make_bookmark_initial():
    t8413_old = 'xing_t8413_outblock1_20210403_212121'
    data = DH.executeAll(f'SELECT idx_god_follow, c_shcode from {t8413_old};')

    results = []

    shcode_before = ''
    shcode_now = ''
    index_start = 0
    index_before = 0
    shcode_now = 0

    for i, row in enumerate(data):
        index_now = row[0]
        shcode_now = row[1]
        if shcode_before:
            if shcode_before != shcode_now:
                result_row = [shcode_before, index_start, index_before]
                results.append(result_row)
                index_start = index_now
            elif i == len(data) - 1:
                result_row = [shcode_before, index_start, index_now]
                results.append(result_row)
            shcode_before = shcode_now
            index_before = index_now
        else:
            shcode_before = shcode_now
            index_before = index_now
            index_start = index_now

    values = [str(tuple(row)) for row in results]
    values = ','.join(values)
    query_insert = f'INSERT INTO `xing_day_candle_chart_bookmark` (c_shcode, c_index_start, c_index_end) VALUES {values};'

    DH.execute(query_insert)
    DH.commit()


def find_start_index():
    pass


def get_bookmarks(trcode):
    table_bookmark = tables_cum[trcode] + '_bookmark'
    res = DH.executeAll(f'select * from {table_bookmark}')
    header = [row[0] for row in DH.executeAll(f'desc {table_bookmark}')]
    data_df = pd.DataFrame(res)
    data_df.columns = header

    return data_df


def copy_table_data():
    trcode = 't8413'
    t8413_old = 'xing_t8413_outblock1_20210403_212121'
    bookmarks = get_bookmarks(trcode)

    shcodes = list(set(bookmarks['c_shcode'].values.tolist()))
    shcodes.sort()
    for shcode in shcodes:
        print(shcode)
        shcode_series = bookmarks.loc[bookmarks['c_shcode'] == shcode]
        index_start = shcode_series['c_index_start'].values[0]
        index_end = shcode_series['c_index_end'].values[0]

        header = [row[0] for row in DH.executeAll(f'DESC {t8413_old};')]
        query_columns = header.copy()
        query_columns.remove('idx_god_follow')
        query_columns = str(tuple(query_columns)).replace('\'', '')
        # 기존 data 불러오기
        res_old = DH.executeAll(f'select * from {t8413_old} '
                                f'where idx_god_follow between {index_start} and {index_end};')
        shcode_df = pd.DataFrame(res_old)
        shcode_df.columns = header
        shcode_df.sort_values(by='c_date', inplace=True)
        res_old_sorted = shcode_df.values
        # idx column 제거
        res_old_sorted_non_index = [row[1:] for row in res_old_sorted]
        query_values = [str(tuple(row)) for row in res_old_sorted_non_index]
        query_values = ','.join(query_values)

        DH.execute(f'INSERT INTO {tables_cum[trcode]} {query_columns} VALUES {query_values}')
        DH.commit()


def main():

    trcode = 't8413'
    t8413_old = 'xing_t8413_outblock1_20210403_212121'
    t8413_new = 'xing_t8413_outblock1_20210613_114325'

    bookmarks = get_bookmarks(trcode)

    time_start = time.time()

    shcodes = list(set(bookmarks['c_shcode'].values.tolist()))
    shcodes.sort()
    for shcode in shcodes:
        print(shcode)
        bookmark_series = bookmarks.loc[bookmarks['c_shcode'] == shcode]
        index_start = bookmark_series['c_index_start'].values[0]
        index_end = bookmark_series['c_index_end'].values[0]

        header = [row[0] for row in DH.executeAll(f'DESC {t8413_old};')]
        # 기존 data 불러오기
        res_old = DH.executeAll(f'select * from {t8413_old} '
                            f'where idx_god_follow between {index_start} and {index_end};')
        old_df = pd.DataFrame(res_old)
        old_df.columns = header
        # 신규 data 불러오기
        res_new = DH.executeAll(f'select * from {t8413_new} '
                                f'where c_shcode = {shcode};')
        new_df = pd.DataFrame(res_new)
        new_df.columns = header
        # outer join dataframe
        outer_join_df = pd.merge(left=old_df, right=new_df, how='right', on='c_date')
        outer_join_df = outer_join_df[outer_join_df['idx_god_follow_x'].isnull()]

        # get right columns
        header_left = [column for column in outer_join_df.columns.tolist() if '_x' in column]
        header_right = [column for column in outer_join_df.columns.tolist() if '_x' not in column]
        header_right_without_y = [column.replace('_y', '') for column in header_right]
        outer_join_df.drop(columns=header_left, inplace=True)
        outer_join_df.columns = header_right_without_y
        outer_join_df.drop(columns=['idx_god_follow'], inplace=True)

        outer_join_df.sort_values(by='c_date', inplace=True)

        query_columns = str(tuple(outer_join_df.columns.tolist())).replace('\'', '')
        query_values = [str(tuple(row)) for row in outer_join_df.values.tolist()]
        query_values = ','.join(query_values)
        query_insert = f'INSERT INTO {t8413_old} {query_columns} VALUES {query_values}'

        DH.execute(query_insert)
        DH.commit()

        exit()

    print(time.time() - time_start)
    exit()

    res = DH.executeAll(f'select idx_god_follow, c_shcode from {t8413_old};')
    results = make_bookmark_initial(res)
    values = [str(tuple(row)) for row in results]
    values = ','.join(values)
    query_insert = f'INSERT INTO `xing_day_candle_chart_bookmark` (c_shcode, c_index_start, c_index_end) VALUES {values};'

    DH.execute(query_insert)
    DH.commit()

    exit()
    header = [row[0] for row in DH.executeAll(f'desc {t8413_old};')]


    result_dict = {}
    data_df = pd.DataFrame(res)
    # data_df.columns = header
    data_df.columns = ['idx', 'shcode']
    shcodes = list(set(data_df['shcode'].values.tolist()))
    shcodes.sort()

    groups_by_shcode = data_df.groupby(data_df.shcode)
    for shcode in shcodes:
        shcode_df = groups_by_shcode.get_group(shcode)
        result_dict[shcode] = shcode_df
        # print(shcode_df)




    print(len(res))


def get_first_date():
    trcode = 't8413'
    t8413_old = 'xing_t8413_outblock1_20210403_212121'

    bookmarks = get_bookmarks(trcode)

    time_start = time.time()

    shcodes = list(set(bookmarks['c_shcode'].values.tolist()))
    shcodes.sort()

    first_dates = []

    for shcode in shcodes:
        print(shcode)
        bookmark_series = bookmarks.loc[bookmarks['c_shcode'] == shcode]
        index_start = bookmark_series['c_index_start'].values[0]
        index_end = bookmark_series['c_index_end'].values[0]

        header = [row[0] for row in DH.executeAll(f'DESC {t8413_old};')]
        # 기존 data 불러오기
        res_old = DH.executeAll(f'select * from {t8413_old} '
                                f'where idx_god_follow between {index_start} and {index_end};')
        old_df = pd.DataFrame(res_old)
        old_df.columns = header
        old_df.sort_values(by='c_date', inplace=True)
        date_first = old_df['c_date'].values.tolist()[0]
        first_dates.append([date_first])

    with open('./first_dates.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(first_dates)


def temp_of_temp():
    print(f'temp_of_temp: {a}')


if __name__ == '__main__':
    # main()
    # copy_table_data()
    # get_first_date()
    # print(sys.argv)
    a = sys.argv[1]
    temp_of_temp()
