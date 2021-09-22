import os
import json
import pandas as pd
from datetime import datetime as dt

from app.module import database_handler


class TRUpdater:

    def __init__(self):
        self.DB = None
        self.structures = {
            't8413': {
                'table_cum': 'xing_day_candle_chart_t8413',
                'table_new': '',
                'column_key': 'c_shcode',
                'column_key_type': 'CHAR(6)',
            }
        }

    @staticmethod
    def get_root_dir():
        # root_dir 은 'god_follow_' 가 들어가야 한다.
        path_elements = os.getcwd().split('\\')
        index_root_dir = 0
        for idx, element in enumerate(path_elements):
            if element.find('god_follow_') > -1:
                index_root_dir = idx
                break
        root_dir = ('\\'.join(path_elements[:index_root_dir + 1])).replace('\\', '/')

        return root_dir

    def get_config(self):
        root_dir = self.get_root_dir()

        with open('{}/app/files/config.json'.format(root_dir), 'r') as f:
            json_data = json.load(f)
            return json_data

    def update_structures(self, trcode):
        table_new = [row[0] for row in self.DB.executeAll(f'SHOW TABLES LIKE "%%xing_{trcode}%%"')]
        table_new = [table for table in table_new if table.find('_bookmark') == -1][-1]
        self.structures[trcode]['table_new'] = table_new

    def get_bookmarks(self, trcode):
        table_cum_bookmark = self.structures[trcode]['table_cum'] + '_bookmark'
        table_new_bookmark = self.structures[trcode]['table_new'] + '_bookmark'

        cum_data = self.DB.executeAll(f'select * from {table_cum_bookmark}')
        cum_header = [row[0] for row in self.DB.executeAll(f'desc {table_cum_bookmark}')]
        cum_data_df = pd.DataFrame(cum_data)
        cum_data_df.columns = cum_header

        new_data = self.DB.executeAll(f'select * from {table_new_bookmark}')
        new_header = [row[0] for row in self.DB.executeAll(f'desc {table_new_bookmark}')]
        new_data_df = pd.DataFrame(new_data)
        new_data_df.columns = new_header

        return cum_data_df, new_data_df

    def initialize_new_table_bookmark(self, trcode):
        table_cum = self.structures[trcode]['table_cum']
        table_cum_bookmark = table_cum + '_bookmark'
        table_new = self.structures[trcode]['table_new']
        table_new_bookmark = table_new + '_bookmark'
        column_key = self.structures[trcode]['column_key']
        column_key_type = self.structures[trcode]['column_key_type']

        query_drop = f'DROP TABLE IF EXISTS {table_new_bookmark};'
        self.DB.execute(query_drop)
        self.DB.commit()

        query_create = f'CREATE TABLE `{table_new_bookmark}` (' \
                       f'`idx_god_follow` INT PRIMARY KEY AUTO_INCREMENT,'\
                       f'`{column_key}` {column_key_type} NOT NULL, '\
                       f'`c_index_start` INT NOT NULL, ' \
                       f'`c_index_end` INT NOT NULL );'
        self.DB.execute(query_create)
        self.DB.commit()

    def make_bookmark_initial(self, trcode):
        table_cum = self.structures[trcode]['table_cum']
        table_new = self.structures[trcode]['table_new']
        table_new_bookmark = table_new + '_bookmark'
        column_key = self.structures[trcode]['column_key']

        self.initialize_new_table_bookmark(trcode)

        data = self.DB.executeAll(f'SELECT idx_god_follow, {column_key} from {table_new};')

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
                    print(shcode_before)
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
        query_insert = f'INSERT INTO `{table_new_bookmark}` ' \
                       f'({column_key}, c_index_start, c_index_end) VALUES {values};'

        self.DB.execute(query_insert)
        self.DB.commit()

    def update_bookmark(self, trcode):
        table_cum = self.structures[trcode]['table_cum']
        table_cum_bookmark = table_cum + '_bookmark'
        column_key = self.structures[trcode]['column_key']

        # table 과 bookmark 에서 마지막 index 가져옴, bookmark 마지막 ~ table 마지막에 대해서 새로 bookmark 작성
        index_last_cum = self.DB.executeAll(
            f'SELECT idx_god_follow FROM {table_cum} ORDER BY idx_god_follow DESC LIMIT 1')[0][0]
        index_last_cum_bookmark = self.DB.executeAll(
            f'SELECT c_index_end FROM {table_cum_bookmark} ORDER BY idx_god_follow DESC LIMIT 1')[0][0]

        data = self.DB.executeAll(f'SELECT idx_god_follow, {column_key} FROM {table_cum} '
                                  f'WHERE idx_god_follow BETWEEN {index_last_cum_bookmark + 1} AND {index_last_cum}')

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
                    print(shcode_before)
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
        query_insert = f'INSERT INTO `{table_cum_bookmark}` ' \
                       f'({column_key}, c_index_start, c_index_end) VALUES {values};'

        self.DB.execute(query_insert)
        self.DB.commit()

    def update_cum_table(self, trcode):
        table_cum = self.structures[trcode]['table_cum']
        table_cum_bookmark = table_cum + '_bookmark'
        table_new = self.structures[trcode]['table_new']
        table_new_bookmark = table_new + '_bookmark'
        column_key = self.structures[trcode]['column_key']
        column_key_type = self.structures[trcode]['column_key_type']

        bookmarks_cum, bookmarks_new = self.get_bookmarks(trcode)

        column_keys = list(set(bookmarks_new[column_key].values.tolist()))
        column_keys.sort()

        # 기준 column 순환하며 cum table 업데이트
        for column in column_keys:
            print(column)

            # column 의 cum index 가져오기
            bookmark_series_cum = bookmarks_cum.loc[bookmarks_cum[column_key] == column]
            if len(bookmark_series_cum):
                index_start_cum = bookmark_series_cum['c_index_start'].values[0]
                index_end_cum = bookmark_series_cum['c_index_end'].values[0]
            else:
                index_start_cum = 0
                index_end_cum = 0
            # column 의 new index 가져오기
            bookmark_series_new = bookmarks_new.loc[bookmarks_new[column_key] == column]
            index_start_new = bookmark_series_new['c_index_start'].values[0]
            index_end_new = bookmark_series_new['c_index_end'].values[0]

            header = [row[0] for row in self.DB.executeAll(f'DESC {table_cum};')]

            if index_start_cum:
                # 기존 data 불러오기
                res_old = self.DB.executeAll(f'select * from {table_cum} '
                                        f'where idx_god_follow between {index_start_cum} and {index_end_cum};')
                old_df = pd.DataFrame(res_old)
                old_df.columns = header

            # 신규 data 불러오기
            res_new = self.DB.executeAll(f'select * from {table_new} '
                                    f'where idx_god_follow between {index_start_new} and {index_end_new};')
            new_df = pd.DataFrame(res_new)
            new_df.columns = header

            if index_start_cum:
                # outer join dataframe
                outer_join_df = pd.merge(left=old_df, right=new_df, how='right', on='c_date')
                outer_join_df = outer_join_df[outer_join_df['idx_god_follow_x'].isnull()]

                # get RIGHT columns
                header_left = [column for column in outer_join_df.columns.tolist() if '_x' in column]
                header_right = [column for column in outer_join_df.columns.tolist() if '_x' not in column]
                header_right_without_y = [column.replace('_y', '') for column in header_right]
                outer_join_df.drop(columns=header_left, inplace=True)
                outer_join_df.columns = header_right_without_y
                outer_join_df.drop(columns=['idx_god_follow'], inplace=True)
                outer_join_df.sort_values(by='c_date', inplace=True)
            else:
                outer_join_df = new_df
                outer_join_df.drop(columns=['idx_god_follow'], inplace=True)
                outer_join_df.sort_values(by='c_date', inplace=True)

            query_columns = str(tuple(outer_join_df.columns.tolist())).replace('\'', '')
            query_values = [str(tuple(row)) for row in outer_join_df.values.tolist()]
            query_values = ','.join(query_values)
            query_insert = f'INSERT INTO {table_cum} {query_columns} VALUES {query_values}'

            self.DB.execute(query_insert)
            self.DB.commit()

    def main(self, trcode):
        self.DB = database_handler.Database()
        date_today = dt.today().strftime('%Y%m%d')

        if trcode == 't1516':

            target_table = \
            [row[0] for row in self.DB.executeAll('show tables') if row[0].find('xing_{}'.format(trcode)) > -1][-1]

            # xing_t1516 교체
            if 'xing_shcode_list_by_sector_t1516' not in [row[0] for row in self.DB.executeAll('show tables')]:
                query_create = 'CREATE TABLE IF NOT EXISTS xing_shcode_list_by_sector_t1516 SELECT * FROM {};'.format(
                    target_table)
                self.DB.execute(query_create)
                self.DB.commit()
            else:
                query_delete = 'DROP TABLE xing_shcode_list_by_sector_t1516;'
                query_create = 'CREATE TABLE IF NOT EXISTS xing_shcode_list_by_sector_t1516 SELECT * FROM {};'.format(
                    target_table)
                self.DB.execute(query_delete)
                self.DB.commit()
                self.DB.execute(query_create)
                self.DB.commit()

        elif trcode == 't1533':

            target_table = \
            [row[0] for row in self.DB.executeAll('show tables') if row[0].find('xing_{}'.format(trcode)) > -1][-1]

            # xing_t1533 교체
            if 'xing_theme_list_t1533' not in [row[0] for row in self.DB.executeAll('show tables')]:
                query_create = 'CREATE TABLE IF NOT EXISTS xing_theme_list_t1533 SELECT * FROM {};'.format(target_table)
                self.DB.execute(query_create)
                self.DB.commit()
            else:
                query_delete = 'DROP TABLE xing_theme_list_t1533;'
                query_create = 'CREATE TABLE IF NOT EXISTS xing_theme_list_t1533 SELECT * FROM {};'.format(target_table)
                print(query_delete)
                self.DB.execute(query_delete)
                self.DB.commit()
                print(query_create)
                self.DB.execute(query_create)
                self.DB.commit()

        elif trcode == 't1537':

            target_table = \
            [row[0] for row in self.DB.executeAll('show tables') if row[0].find('xing_{}'.format(trcode)) > -1][-1]

            # xing_t1537 교체
            if 'xing_shcode_list_by_theme_t1537' not in [row[0] for row in self.DB.executeAll('show tables')]:
                query_create = 'CREATE TABLE IF NOT EXISTS xing_shcode_list_by_theme_t1537 SELECT * FROM {};'.format(
                    target_table)
                self.DB.execute(query_create)
                self.DB.commit()
            else:
                query_delete = 'DROP TABLE xing_shcode_list_by_theme_t1537;'
                query_create = 'CREATE TABLE IF NOT EXISTS xing_shcode_list_by_theme_t1537 SELECT * FROM {};'.format(
                    target_table)
                self.DB.execute(query_delete)
                self.DB.commit()
                self.DB.execute(query_create)
                self.DB.commit()

        elif trcode == 't8413':
            self.update_structures(trcode)
            self.make_bookmark_initial(trcode)
            self.update_cum_table(trcode)
            self.update_bookmark(trcode)

        elif trcode == 't8424':

            target_table = \
            [row[0] for row in self.DB.executeAll('show tables') if row[0].find('xing_{}'.format(trcode)) > -1][-1]

            # xing_t8424 교체
            if 'xing_sector_list_t8424' not in [row[0] for row in self.DB.executeAll('show tables')]:
                query_create = 'CREATE TABLE IF NOT EXISTS xing_sector_list_t8424 SELECT * FROM {};'.format(
                    target_table)
                self.DB.execute(query_create)
                self.DB.commit()
            else:
                query_delete = 'DROP TABLE xing_sector_list_t8424;'
                query_create = 'CREATE TABLE IF NOT EXISTS xing_sector_list_t8424 SELECT * FROM {};'.format(
                    target_table)
                self.DB.execute(query_delete)
                self.DB.commit()
                self.DB.execute(query_create)
                self.DB.commit()

        elif trcode == 't8430':

            target_table = \
                [row[0] for row in self.DB.executeAll('show tables') if row[0].find('xing_{}'.format(trcode)) > -1][-1]
            rows = [row[1:] for row in self.DB.executeAll('select * from {}'.format(target_table))]

            columns = [row[0] for row in self.DB.executeAll('desc xing_t8430')][1:]

            # xing_t8430 교체
            query_delete = 'DELETE FROM xing_t8430;'
            query_alter = 'ALTER TABLE xing_t8430 AUTO_INCREMENT = 1;'
            self.DB.execute(query_delete)
            self.DB.commit()
            self.DB.execute(query_alter)
            self.DB.commit()

            query_columns = str(tuple(columns)).replace('\'', '')
            query_head = 'INSERT INTO xing_t8430 {} VALUES'.format(query_columns)
            query_values = str([tuple(row) for row in rows])[1:-1]
            query_insert = query_head + query_values
            # print(query_insert)
            self.DB.execute(query_insert)
            self.DB.commit()

            # xing_t8430_cum 업데이트
            # 상장된 종목 추가
            sql_added = 'SELECT xing_t8430.hname, xing_t8430.shcode, xing_t8430.expcode, xing_t8430.etfgubun, xing_t8430.uplmtprice, xing_t8430.dnlmtprice, xing_t8430.jnilclose, xing_t8430.memedan, xing_t8430.recprice, xing_t8430.gubun \
                        FROM xing_t8430_cum RIGHT JOIN xing_t8430 ON xing_t8430_cum.shcode=xing_t8430.shcode WHERE xing_t8430_cum.shcode IS NULL ORDER BY xing_t8430.idx;'

            added_list = []

            res = self.DB.executeAll(sql_added)

            if res:
                for row in res:
                    temp_list = list(row)
                    # 상장되어 최초로 인식된 날 입력
                    temp_list.append(date_today)
                    added_list.append(tuple(temp_list))

                sql_values = str(added_list)[1:-1]
                sql_push = 'INSERT INTO xing_t8430_cum(hname, shcode, expcode, etfgubun, uplmtprice, dnlmtprice, jnilclose, memedan, recprice, gubun, added) VALUES {}'.format(
                    sql_values)
                self.DB.execute(sql_push)
                self.DB.commit()

            # 상폐된 종목 제거
            sql_dropped = 'SELECT xing_t8430_cum.shcode, xing_t8430_cum.dropped FROM xing_t8430_cum LEFT JOIN xing_t8430 ON xing_t8430_cum.shcode=xing_t8430.shcode WHERE xing_t8430.shcode IS NULL ORDER BY xing_t8430_cum.idx;'

            res = self.DB.executeAll(sql_dropped)

            if res:
                shcode_droped_list = [row[0] for row in res if row[1] == '0000-00-00']

                for shcode in shcode_droped_list:
                    sql_update_dropped = 'UPDATE xing_t8430_cum SET dropped="{}" WHERE shcode="{}";'.format(date_today,
                                                                                                            shcode)
                    print(sql_update_dropped)
                    self.DB.execute(sql_update_dropped)
                    self.DB.commit()

        elif trcode == 't8436':

            target_table = \
            [row[0] for row in self.DB.executeAll('show tables') if row[0].find('xing_{}'.format(trcode)) > -1][-1]
            rows = [row[1:] for row in self.DB.executeAll('select * from {}'.format(target_table))]

            columns = [row[0] for row in self.DB.executeAll('desc xing_shcode_list_t8436')][1:]

            # xing_t8436 교체
            query_delete = 'DELETE FROM xing_shcode_list_t8436;'
            query_alter = 'ALTER TABLE xing_shcode_list_t8436 AUTO_INCREMENT = 1;'
            self.DB.execute(query_delete)
            self.DB.commit()
            self.DB.execute(query_alter)
            self.DB.commit()

            query_columns = str(tuple(columns)).replace('\'', '')
            query_head = 'INSERT INTO xing_shcode_list_t8436 {} VALUES'.format(query_columns)
            query_values = str([tuple(row) for row in rows])[1:-1]
            query_insert = query_head + query_values
            # print(query_insert)
            self.DB.execute(query_insert)
            self.DB.commit()

            # xing_t8436_cum 업데이트
            # 상장된 종목 추가
            sql_added = 'SELECT xing_shcode_list_t8436.c_hname, xing_shcode_list_t8436.c_shcode, xing_shcode_list_t8436.c_expcode, xing_shcode_list_t8436.c_etfgubun, xing_shcode_list_t8436.c_uplmtprice, xing_shcode_list_t8436.c_dnlmtprice, xing_shcode_list_t8436.c_jnilclose, xing_shcode_list_t8436.c_memedan, xing_shcode_list_t8436.c_recprice, xing_shcode_list_t8436.c_gubun, xing_shcode_list_t8436.c_bu12gubun, xing_shcode_list_t8436.c_spac_gubun, xing_shcode_list_t8436.c_filler, xing_shcode_list_t8436.c_date_time \
                        FROM xing_shcode_list_t8436_cum RIGHT JOIN xing_shcode_list_t8436 ON xing_shcode_list_t8436_cum.c_shcode=xing_shcode_list_t8436.c_shcode WHERE xing_shcode_list_t8436_cum.c_shcode IS NULL ORDER BY xing_shcode_list_t8436.idx_god_follow;'

            added_list = []

            res = self.DB.executeAll(sql_added)

            if res:
                for row in res:
                    temp_list = list(row)
                    # 상장되어 최초로 인식된 날 입력
                    temp_list.append(date_today)
                    added_list.append(tuple(temp_list))

                sql_values = str(added_list)[1:-1]
                sql_push = 'INSERT INTO xing_shcode_list_t8436_cum(c_hname, c_shcode, c_expcode, c_etfgubun, c_uplmtprice, c_dnlmtprice, c_jnilclose, c_memedan, c_recprice, c_gubun, c_bu12gubun, c_spac_gubun, c_filler, c_date_time, added) VALUES {}'.format(
                    sql_values)
                self.DB.execute(sql_push)
                self.DB.commit()

            # 상폐된 종목 제거
            sql_dropped = 'SELECT xing_shcode_list_t8436_cum.c_shcode, xing_shcode_list_t8436_cum.dropped FROM xing_shcode_list_t8436_cum LEFT JOIN xing_shcode_list_t8436 ON xing_shcode_list_t8436_cum.c_shcode=xing_shcode_list_t8436.c_shcode WHERE xing_shcode_list_t8436.c_shcode IS NULL ORDER BY xing_shcode_list_t8436_cum.idx_god_follow;'

            res = self.DB.executeAll(sql_dropped)

            if res:
                shcode_droped_list = [row[0] for row in res if row[1] == '0000-00-00']

                for shcode in shcode_droped_list:
                    sql_update_dropped = 'UPDATE xing_shcode_list_t8436_cum SET dropped="{}" WHERE c_shcode="{}";'.format(date_today,
                                                                                                            shcode)
                    print(sql_update_dropped)
                    self.DB.execute(sql_update_dropped)
                    self.DB.commit()

        else:
            pass

        del self.DB

        return 1


if __name__ == '__main__':
    TRU = TRUpdater()
    TRU.main('t8413')