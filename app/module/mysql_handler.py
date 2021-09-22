import time
import sys
import math

from logger import Logger

from app.module import database_handler
from app.module import table_handler

log = Logger(__name__)


class MysqlHandler:

    def __init__(self):
        super().__init__()

        self.DB = database_handler.Database()
        self.TH = table_handler.TableHandler(self.DB)

        self.trcode = ''
        self.cnt_values = 0
        self.flag_create_table = 0
        self.table_name = ''
        self.time_before = 0
        self.values = []
        self.main_block = ''

        self.cnt = 0
        self.cnt_cum = 0

    _TIME_GAP = 10
    _MAX_ROWS_LEN = 10000

    _TABLE_MODIFY = {
        't1302': {
            'main_block': 'OutBlock1',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value',
                    'column_name': 'shcode',
                    'type': 'char',
                    'size': 6,
                    'extra': 'not null'
                }
            ]
        },
        't1305': {
            'main_block': 'OutBlock1',
            'column_add': []
        },
        't1403': {
            'main_block': 'OutBlock1',
            'column_add': []
        },
        't1404': {
            'main_block': 'OutBlock1',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'date_time|occur_listened',
                    'column_name': 'date_time',
                    'type': 'char',
                    'size': 22,
                    'extra': 'not null'
                },
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value',
                    'column_name': 'jongchk',
                    'type': 'char',
                    'size': 1,
                    'extra': 'not null'
                }
            ]
        },
        't1405': {
            'main_block': 'OutBlock1',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'date_time|occur_listened',
                    'column_name': 'date_time',
                    'type': 'char',
                    'size': 22,
                    'extra': 'not null'
                },
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value',
                    'column_name': 'jongchk',
                    'type': 'char',
                    'size': 1,
                    'extra': 'not null'
                }
            ]
        },
        't1410': {
            'main_block': 'OutBlock1',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'date_time|occur_listened',
                    'column_name': 'date_time',
                    'type': 'char',
                    'size': 22,
                    'extra': 'not null'
                }
            ]
        },
        't1411': {
            'main_block': 'OutBlock1',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'date_time|occur_listened',
                    'column_name': 'date_time',
                    'type': 'char',
                    'size': 22,
                    'extra': 'not null'
                }
            ]
        },
        't1602': {
            'main_block': 'OutBlock1',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value1',
                    'column_name': 'upcode',
                    'type': 'char',
                    'size': 3,
                    'extra': 'not null'
                },
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value2',
                    'column_name': 'volume_amount',
                    'type': 'char',
                    'size': 1,
                    'extra': 'not null'
                }
            ]
        },
        't1633': {
            'main_block': 'OutBlock1',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value1',
                    'column_name': 'market',
                    'type': 'char',
                    'size': 1,
                    'extra': 'not null'
                },
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value2',
                    'column_name': 'amount_volume',
                    'type': 'char',
                    'size': 1,
                    'extra': 'not null'
                }
            ]
        },
        't1636': {
            'main_block': 'OutBlock1',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value1',
                    'column_name': 'market',
                    'type': 'char',
                    'size': 1,
                    'extra': 'not null'
                },
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value2',
                    'column_name': 'volume_amount',
                    'type': 'char',
                    'size': 1,
                    'extra': 'not null'
                },
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value3',
                    'column_name': 'date',
                    'type': 'char',
                    'size': 8,
                    'extra': 'not null'
                }
            ]
        },
        't1637': {
            'main_block': 'OutBlock1',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value1',
                    'column_name': 'volume_amount',
                    'type': 'char',
                    'size': 1,
                    'extra': 'not null'
                },
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value2',
                    'column_name': 'time_period',
                    'type': 'char',
                    'size': 1,
                    'extra': 'not null'
                }
            ]
        },
        't1662': {
            'main_block': 'OutBlock',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value1',
                    'column_name': 'market',
                    'type': 'char',
                    'size': 1,
                    'extra': 'not null'
                },
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value2',
                    'column_name': 'amount_volume',
                    'type': 'char',
                    'size': 1,
                    'extra': 'not null'
                },
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value3',
                    'column_name': 'date',
                    'type': 'char',
                    'size': 8,
                    'extra': 'not null'
                }
            ]
        },
        't1663': {
            'main_block': 'OutBlock',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value1',
                    'column_name': 'market',
                    'type': 'char',
                    'size': 1,
                    'extra': 'not null'
                },
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value2',
                    'column_name': 'amount_volume',
                    'type': 'char',
                    'size': 1,
                    'extra': 'not null'
                },
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value3',
                    'column_name': 'date',
                    'type': 'char',
                    'size': 8,
                    'extra': 'not null'
                }
            ]
        },
        't1665': {
            'main_block': 'OutBlock1',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value',
                    'column_name': 'upcode',
                    'type': 'char',
                    'size': 3,
                    'extra': 'not null'
                }
            ]
        },
        't1717': {
            'main_block': 'OutBlock',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value',
                    'column_name': 'shcode',
                    'type': 'char',
                    'size': 6,
                    'extra': 'not null'
                }
            ]
        },
        't1921': {
            'main_block': 'OutBlock1',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value',
                    'column_name': 'gubun',
                    'type': 'char',
                    'size': 1,
                    'extra': 'not null'
                }
            ]
        },
        't1926': {
            'main_block': 'OutBlock',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value',
                    'column_name': 'shcode',
                    'type': 'char',
                    'size': 6,
                    'extra': 'not null'
                }
            ]
        },
        't1927': {
            'main_block': 'OutBlock1',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value',
                    'column_name': 'shcode',
                    'type': 'char',
                    'size': 6,
                    'extra': 'not null'
                }
            ]
        },
        't1941': {
            'main_block': 'OutBlock1',
            'column_add': []
        },
        't8411': {
            'main_block': 'OutBlock1',
            'column_add': [
                {
                    'source_table': 'OutBlock',
                    'column_name': 'shcode'
                }
            ]
        },
        't8412': {
            'main_block': 'OutBlock1',
            'column_add': [
                {
                    'source_table': 'OutBlock',
                    'column_name': 'shcode'
                }
            ]
        },
        't8413': {
            'main_block': 'OutBlock1',
            'column_add': [
                {
                    'source_table': 'OutBlock',
                    'column_name': 'shcode'
                }
            ]
        },
        't8414': {
            'main_block': 'OutBlock1',
            'column_add': [
                {
                    'source_table': 'OutBlock',
                    'column_name': 'shcode'
                }
            ]
        },
        't8419': {
            'main_block': 'OutBlock1',
            'column_add': [
                {
                    'source_table': 'OutBlock',
                    'column_name': 'shcode'
                }
            ]
        },
        't8430': {
            'main_block': 'OutBlock',
            'column_add': []
        },
        't8432': {
            'main_block': 'OutBlock',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value1',
                    'column_name': 'date',
                    'type': 'char',
                    'size': 8,
                    'extra': 'not null'
                }
            ]
        },
        't8433': {
            'main_block': 'OutBlock',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value1',
                    'column_name': 'date',
                    'type': 'char',
                    'size': 8,
                    'extra': 'not null'
                }
            ]
        },
        't8435': {
            'main_block': 'OutBlock',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value1',
                    'column_name': 'date',
                    'type': 'char',
                    'size': 8,
                    'extra': 'not null'
                }
            ]
        },
        't8436': {
            'main_block': 'OutBlock',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'date_time|occur_listened',
                    'column_name': 'date_time',
                    'type': 'char',
                    'size': 22,
                    'extra': 'not null'
                }
            ]
        },
        'BMT': {
            'main_block': 'OutBlock',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'date_time|occur_listened',
                    'column_name': 'date_time',
                    'type': 'char',
                    'size': 22,
                    'extra': 'not null'
                }
            ]
        },
        'S3_': {
            'main_block': 'OutBlock',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'date_time|occur_listened',
                    'column_name': 'date_time',
                    'type': 'char',
                    'size': 22,
                    'extra': 'not null'
                }
            ]
        },
        'JIF': {
            'main_block': 'OutBlock',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'date_time|occur_listened',
                    'column_name': 'date_time',
                    'type': 'char',
                    'size': 22,
                    'extra': 'not null'
                }
            ]
        },
        'H1_': {
            'main_block': 'OutBlock',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'date_time|occur_listened',
                    'column_name': 'date_time',
                    'type': 'char',
                    'size': 22,
                    'extra': 'not null'
                }
            ]
        },
        'K3_': {
            'main_block': 'OutBlock',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'date_time|occur_listened',
                    'column_name': 'date_time',
                    'type': 'char',
                    'size': 22,
                    'extra': 'not null'
                }
            ]
        },
        'HA_': {
            'main_block': 'OutBlock',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'date_time|occur_listened',
                    'column_name': 'date_time',
                    'type': 'char',
                    'size': 22,
                    'extra': 'not null'
                }
            ]
        },
        'OVC': {
            'main_block': 'OutBlock',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'date_time|occur_listened',
                    'column_name': 'date_time',
                    'type': 'char',
                    'size': 22,
                    'extra': 'not null'
                }
            ]
        },
        'YK3': {
            'main_block': 'OutBlock',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'date_time|occur_listened',
                    'column_name': 'date_time',
                    'type': 'char',
                    'size': 22,
                    'extra': 'not null'
                }
            ]
        },
        'YS3': {
            'main_block': 'OutBlock',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'date_time|occur_listened',
                    'column_name': 'date_time',
                    'type': 'char',
                    'size': 22,
                    'extra': 'not null'
                }
            ]
        },
        't1533': {
            'main_block': 'OutBlock1',
            'column_add': [
                {
                    'source_table': 'OutBlock',
                    'column_name': 'bdate'
                }
            ]
        },
        't1537': {
            'main_block': 'OutBlock1',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value',
                    'column_name': 'tmcode',
                    'type': 'char',
                    'size': 4,
                    'extra': 'not null'
                }
            ]
        },
        't8424': {
            'main_block': 'OutBlock',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'date_time|occur_listened',
                    'column_name': 'date_time',
                    'type': 'char',
                    'size': 22,
                    'extra': 'not null'
                }
            ]
        },
        't1516': {
            'main_block': 'OutBlock1',
            'column_add': [
                {
                    'source_table': 'nan',
                    'source_key': 'occur|value',
                    'column_name': 'upcode',
                    'type': 'char',
                    'size': 3,
                    'extra': 'not null'
                }
            ]
        }
    }

    def find_last_value(self, input_dict, input_list):
        for key, value in input_dict.items():
            if key == input_list[0]:
                if isinstance(value, dict):
                    result = self.find_last_value(value, input_list[1:])
                    return result
                else:
                    return value

    def update_mysql(self, xing_element):
        """
        xing_element = {
            'xing_name': self.xing_name,
            'trcode': self.trcode,
            'phase': 'listened',
            'date_time': dt.today().strftime('%Y%m%d %H%M%S'),
            'OutBlocks': self.output
        }
        """
        DB = self.DB
        TH = self.TH

        xing_name = xing_element['xing_name']
        trcode = xing_element['trcode']
        phase = xing_element['phase']
        cycle_start_date_time = xing_element['date_time']['cycle_start'].split('.')[0].replace(' ', '_')
        OutBlocks = xing_element['OutBlocks']

        block_name_main = MysqlHandler._TABLE_MODIFY[trcode]['main_block']

        # 해당 구문은 check_xing_element 로 대체될 것임.
        # 하나의 mysql 객체가 여러 trcode 로부터 수신 가능하도록 바꾸자.
        if not self.trcode:
            self.trcode = trcode
        elif self.trcode != trcode:
            log.critical('이전과 다른 trcode block 이 인식되었습니다. {}|{}'.format(self.trcode, trcode))

        if not self.flag_create_table:

            # initialize today's table
            is_exist = TH.check_table_exist(xing_element)
            if is_exist:
                TH.drop_table(xing_element)
            TH.create_table(xing_element)

            # drop non main table
            TH.drop_non_main_table(xing_element, MysqlHandler._TABLE_MODIFY)

            # add column if it needed
            TH.add_column(xing_element, MysqlHandler._TABLE_MODIFY)

            self.flag_create_table = 1

            del TH

            self.table_name = 'xing_{}_{}_{}'.format(xing_name, block_name_main, cycle_start_date_time).lower()

        if phase == 'listened':

            """
            # cnt test
            self.cnt += 1
            print('MH_cnt {}'.format(self.cnt))
            """

            self.values.append(xing_element)

            flag = self.check_time_len()

            if flag:

                query_insert = self.make_query()

                if query_insert:
                    if (self.trcode == 'BMT'):
                        print(query_insert)

                    DB.execute(query_insert)
                    DB.commit()

                    message = " - DB.commit ({}:{})".format(trcode, len(self.values))
                    print(message)
                    # log.debug(" - DB.commit ({}:{})".format(trcode, self.cnt_values))

                    """
                    # cnt test
                    self.cnt_cum += len(self.values)
                    print('MH_cnt_cum {}'.format(self.cnt_cum))
                    """

                    self.time_before = time.time()
                    self.values = []

                else:
                    message = 'not enough for DB.commit'

                return message

            else:
                message = 'not enough for DB.commit'

                return message

        elif phase == 'done':

            message = ''

            if self.values:

                query_insert = self.make_query()

                # print(query_insert)
                #
                # try:
                #     DB.execute(query_insert)
                #     DB.commit()
                # except Exception as err:
                #     print(f'mysql_handler.py DB.execute(query_insert): {err}')

                if query_insert:
                    DB.execute(query_insert)
                    DB.commit()

                    message = f' - DB.commit ({trcode}:{len(self.values)}) | cycle is done'
                    print(message)
                    # log.debug(" - DB.commit ({}:{})".format(trcode, self.cnt_values))

                else:
                    message = 'cycle is done, and nothing to do.'
                    print(message)

                self.time_before = time.time()
                self.values = []

                return message

            else:
                message = 'cycle is done, and nothing to do.'
                print(message)

            return message

        else:
            print(f'mysql_handler.py: unknown phase {phase}')

    def make_query(self):
        """
        xing_element = {
            'xing_name': self.xing_name,
            'trcode': self.trcode,
            'phase': 'listened',
            'date_time': dt.today().strftime('%Y%m%d %H%M%S'),
            'OutBlocks': self.output
        }
        """
        values = self.values
        # trcode = self.trcode
        # xing_name = values[0]['xing_name']
        # cycle_start_date_time = values[0]['date_time']['cycle_start'].split('.')[0].replace(' ', '_')
        # block_name_main = MysqlHandler._TABLE_MODIFY[trcode]['main_block']
        # table_name_main = 'xing_{}_{}_{}'.format(xing_name, block_name_main, cycle_start_date_time).lower()
        columns = []
        query_columns = ''
        value_strings = []
        query_values = ''

        for xing_element in values:
            # table name 합성
            trcode = xing_element['trcode']
            xing_name = xing_element['xing_name']
            cycle_start_date_time = xing_element['date_time']['cycle_start'].split('.')[0].replace(' ', '_')
            block_name_main = MysqlHandler._TABLE_MODIFY[trcode]['main_block']
            table_name_main = 'xing_{}_{}_{}'.format(xing_name, block_name_main, cycle_start_date_time).lower()

            # set columns
            if not columns:
                columns = ['c_' + column for column in xing_element['OutBlocks'][block_name_main]['columns_names']]
                column_descriptions = MysqlHandler._TABLE_MODIFY[trcode]['column_add']
                for column_description in column_descriptions:
                    columns.append('c_' + column_description['column_name'])
                query_columns = str(tuple(columns)).replace('\'', '')

            # get default main block data
            value_lump = xing_element['OutBlocks'][block_name_main]['listened_data']

            # get added column data
            for value in value_lump:
                column_descriptions = MysqlHandler._TABLE_MODIFY[trcode]['column_add']
                for column_description in column_descriptions:
                    if not column_description['source_table'] == 'nan':

                        # outblock 을 refer 할 경우 outblock 에서 몇 번째 column 을 참조하는 지 순번을 알아야 한다.
                        block_name_refer = column_description['source_table']
                        column_name_refer = column_description['column_name']
                        refer_block_columns = xing_element['OutBlocks'][block_name_refer]['columns_names']

                        target_column_num = 0
                        for idx, refer_column_name in enumerate(refer_block_columns):
                            if refer_column_name == column_name_refer:
                                target_column_num = idx

                        if block_name_refer == 'OutBlock':
                            refer_value = xing_element['OutBlocks'][block_name_refer]['listened_data'][0][target_column_num]
                            value.append(refer_value)
                        else:
                            print('should update code mysql_handler 183')

                    if column_description['source_table'] == 'nan':
                        key_name_refer = column_description['source_key'].split('|')

                        refer_value = self.find_last_value(xing_element, key_name_refer)

                        value.append(refer_value)

            for row in value_lump:
                # int 값에 "-" 입력될 경우 0으로 치환
                row = [0 if element == '-' else element for element in row]
                if (trcode == 'YS3') or (trcode == 'YK3'):
                    row = [0 if element == '' else element for element in row]

                value_strings.append(str(tuple(row)))

        query_values = ','.join(value_strings)
        query_insert = 'INSERT INTO {} {} VALUES {};'.format(table_name_main, query_columns, query_values)

        if query_values:
            return query_insert
        else:
            return 0

    def update_results(self, xing_element):
        DB = self.DB

        xing_id = xing_element['id']
        xing_name = xing_element['xing_name']
        xing_type = xing_element['type']
        trcode = xing_element['trcode']
        options = str(xing_element['options'])
        inblocks = str(xing_element['InBlocks'])
        cycle_start = xing_element['date_time']['cycle_start']
        element_start = xing_element['date_time']['element_start']
        occur_start = xing_element['date_time']['occur_start']
        occur_listened = xing_element['date_time']['occur_listened']
        occur_end = xing_element['date_time']['occur_end']
        elapse_time = xing_element['date_time']['elapse_time']
        occur_value = xing_element['occur']['value']
        occur_size = xing_element['occur']['size']
        status_final = xing_element['status']['final']
        status_reaction = xing_element['status']['reaction']

        query_head = 'INSERT INTO xing_listened_results (id, xing_name, type, trcode, options, inblocks, cycle_start, ' \
                     'element_start, occur_start, occur_listened, occur_end, elapse_time, occur_value, occur_size, status_final, status_reaction) VALUES '
        query_values = str((xing_id, xing_name, xing_type, trcode, options, inblocks, cycle_start, element_start, occur_start,
                        occur_listened, occur_end, elapse_time, occur_value, occur_size, status_final, status_reaction))
        query_insert = query_head + query_values

        DB.execute(query_insert)
        DB.commit()

        print(' - DB.commit listened results: {}'.format(query_values))

    def check_xing_element(self):
        pass
        # outblock, outblock1 개수 체크
        # 동일한 trcode, columns 체크

    def check_time_len(self):
        if (time.time() - self.time_before) > MysqlHandler._TIME_GAP:
            return 1

        if len(self.values) > MysqlHandler._MAX_ROWS_LEN:
            return 1

        return 0


if __name__ == '__main__':

    table_list = []

    MH = MysqlHandler()
    for table in table_list:
        MH.DB.execute('drop table {};'.format(table))
