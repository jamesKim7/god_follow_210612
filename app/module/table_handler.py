import re
import os
import pandas as pd


class TableHandler:

    def __init__(self, DB):
        self.stand_alone = ['S3_', 'H1_', 'OVC', 'OVH']
        self.DB = DB
        self.table_schema = None

    @staticmethod
    def get_res_file(trcode):
        path_xingapi = 'C:/eBEST/xingAPI/'

        meta = {}

        fnames = filter(
            lambda x: not re.match(r'.*\_\d+\.res$', x),
            os.listdir(os.path.join(path_xingapi, 'res'))
        )

        def parse_field(line):
            cols = line.split(',')
            return {
                'name': cols[1].strip(),
                'desc': cols[0].strip(),
                'type': cols[3].strip(),
                'size': cols[4].strip()
            }

        def parse_file(lines):
            parsed = {}
            lines = list(map(lambda x: x.replace('\t', '').replace('\n', '').replace(';', '').strip(), lines))
            lines = list(filter(lambda x: x, lines))
            for i in range(len(lines)):
                if '.Func' in lines[i] or '.Feed' in lines[i]:
                    parsed['desc'] = lines[i].split(',')[1].strip()
                elif lines[i] == 'begin':
                    latest_begin = i
                elif lines[i] == 'end':
                    block_info = lines[latest_begin - 1].split(',')

                    if not block_info[2] in parsed:
                        parsed[block_info[2]] = {}

                    parsed[block_info[2]][block_info[0]] = {
                        'occurs': 'occurs' in block_info,
                        'fields': list(map(parse_field, lines[latest_begin + 1:i]))
                    }
            return parsed

        for fname in fnames:
            if fname.split('.')[0] == trcode:
                meta[fname.replace('.res', '')] = parse_file(
                    open(os.path.join(path_xingapi, 'res/', fname)).readlines()
                )

        return meta

    @staticmethod
    def get_block_names(xing_element):
        block_names = []
        for key, values in xing_element['OutBlocks'].items():
            block_name = key
            block_names.append(block_name)

        return block_names

    def get_block_schema(self, trcode, block_name):
        dict_res = self.get_res_file(trcode)
        # real res 객체는 output 하위 key 값에 trcode 가 포함되지 않는다.
        df_res = None
        if f'{trcode}{block_name}' in dict_res[trcode]['output'].keys():
            df_res = pd.DataFrame(dict_res[trcode]['output'][f'{trcode}{block_name}']['fields'])
        elif f'{block_name}' in dict_res[trcode]['output'].keys():
            df_res = pd.DataFrame(dict_res[trcode]['output'][f'{block_name}']['fields'])
        else:
            print('ERR: can\'t get meta_res')
            exit()
        df_res['trcode'] = trcode
        df_res['block'] = block_name
        df_res = df_res.rename(columns={
            'name': 'field',
            'desc': 'descriptions'
        })
        df_res = df_res.astype({
            'size': float
        })

        return df_res

    @staticmethod
    def change_block_schema_type(block_schema):
        for idx, row in block_schema.iterrows():
            # 변수 type 과 size 변환
            if row['type'] == 'char':
                block_schema.loc[idx, 'type_mysql'] = '{}({})'.format(row['type'], int(row['size']))
            elif row['type'] == 'long':
                block_schema.loc[idx, 'type_mysql'] = 'int'
            elif row['type'] == 'double':
                block_schema.loc[idx, 'type_mysql'] = 'float'
            if row['size'] % 1 != 0:
                block_schema.loc[idx, 'type_mysql'] = 'float'
            elif row['type'] != 'char' and row['size'] > 9:
                block_schema.loc[idx, 'type_mysql'] = 'bigint'

            # 변수명에 "c_" 접두사로 추가
            block_schema.loc[idx, 'field'] = 'c_' + row['field']

        return block_schema

    def create_table(self, xing_element):
        """
        xing_element = {
            'xing_name': self.xing_name,
            'trcode': self.trcode,
            'phase': 'listened',
            'date_time': dt.today().strftime('%Y%m%d %H%M%S'),
            'xing_output_element': self._putData(szTrCode)
        }
        """
        DB = self.DB

        xing_name = xing_element['xing_name']
        trcode = xing_element['trcode']
        cycle_start_date_time = xing_element['date_time']['cycle_start'].split('.')[0].replace(' ', '_')
        phase = xing_element['phase']
        block_names = self.get_block_names(xing_element)

        if phase == 'old listened table':
            table_name = 'xing_{}_{}_{}'.format(xing_name, phase, cycle_start_date_time)

            sql_head = 'CREATE TABLE {}('.format(table_name)
            sql_record_idx = 'idx_god_follow INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,'
            sql_record_rest = 'listened_data MEDIUMTEXT NOT NULL'
            sql_tail = ');'

            sql_create = '{}{}{}{}'.format(
                sql_head,
                sql_record_idx,
                sql_record_rest,
                sql_tail
            )

            DB.execute(sql_create)
            DB.commit()

            print('TABLE {} CREATED'.format(table_name))

        elif (phase == 'listened') or (phase == 'done'):
            block_dfs = []
            for block_name in block_names:

                block_schema = self.get_block_schema(trcode, block_name)
                block_schema = self.change_block_schema_type(block_schema)

                block_dfs.append(block_schema)

            for df in block_dfs:
                block_name = df['block'].tolist()[0]

                table_name = 'xing_{}_{}_{}'.format(xing_name, block_name, cycle_start_date_time)

                sql_head = 'CREATE TABLE {}('.format(table_name)
                sql_record_idx = 'idx_god_follow INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,'
                sql_tail = ');'

                sql_record_list = []
                # default 를 NULL 로 바꾼다.
                for idx, row in df.iterrows():
                    # sql_record_list.append('{} {} {}'.format(row['field'], row['type_mysql'], 'NOT NULL'))
                    sql_record_list.append('{} {}'.format(row['field'], row['type_mysql']))
                sql_record_rest = ','.join(sql_record_list)

                sql_create = '{}{}{}{}'.format(
                    sql_head,
                    sql_record_idx,
                    sql_record_rest,
                    sql_tail
                )

                DB.execute(sql_create)
                DB.commit()

                print('TABLE {} CREATED'.format(table_name))

        else:
            print('create_table: what is my phase')

    def drop_table(self, xing_element):
        """
        xing_element = {
            'xing_name': self.xing_name,
            'trcode': self.trcode,
            'phase': 'listened',
            'date_time': dt.today().strftime('%Y%m%d %H%M%S'),
            'xing_output_element': self._putData(szTrCode)
        }
        """

        DB = self.DB
        xing_name = xing_element['xing_name']
        trcode = xing_element['trcode']
        cycle_start_date_time = xing_element['date_time']['cycle_start'].split('.')[0].replace(' ', '_')
        block_names = self.get_block_names(xing_element)

        for block in block_names:
            table_name = 'xing_{}_{}_{}'.format(xing_name, block, cycle_start_date_time).lower()

            table_list = [e[0] for e in DB.executeAll('show tables;')]

            if table_name in table_list:
                DB.execute('drop table {}'.format(table_name))
                DB.commit()
                print('drop table {}'.format(table_name))

    def check_table_exist(self, xing_element):
        """
        xing_element = {
            'xing_name': self.xing_name,
            'trcode': self.trcode,
            'phase': 'listened',
            'date_time': dt.today().strftime('%Y%m%d %H%M%S'),
            'xing_output_element': self._putData(szTrCode)
        }
        """

        DB = self.DB

        xing_name = xing_element['xing_name']
        trcode = xing_element['trcode']
        cycle_start_date_time = xing_element['date_time']['cycle_start'].split('.')[0].replace(' ', '_')
        block_names = self.get_block_names(xing_element)

        is_exist = 0

        for block in block_names:
            table_name = 'xing_{}_{}_{}'.format(xing_name, block, cycle_start_date_time).lower()

            table_list = [e[0] for e in DB.executeAll('show tables;')]

            if table_name in table_list:
                is_exist = 1
                print('{} is exist'.format(table_name))
            else:
                print('{} is not exist'.format(table_name))


        return is_exist

    def drop_non_main_table(self, xing_element, _TABLE_MODIFY):
        """
                xing_element = {
                    'xing_name': self.xing_name,
                    'trcode': self.trcode,
                    'phase': 'listened',
                    'date_time': dt.today().strftime('%Y%m%d %H%M%S'),
                    'xing_output_element': self._putData(szTrCode)
                }
                _TABLE_MODIFY = {
                    't8411': {
                        'main_block': 'OutBlock',
                        'column_add': [
                            {
                                'source_table' 'OutBlock'
                                'column_name': 'shcode'
                            }
                        ]
                    },
                    'HA_': {
                        'main_block': 'OutBlock',
                        'column_add': [
                            {
                                'source_table': 'nan',
                                'source_key': 'date_time',
                                'column_name': 'date_time',
                                'type': 'char',
                                'size': 15,
                                'extra': ''
                            }
                        ]
                    }
                }
                """
        DB = self.DB

        xing_name = xing_element['xing_name']
        trcode = xing_element['trcode']
        cycle_start_date_time = xing_element['date_time']['cycle_start'].split('.')[0].replace(' ', '_')
        block_names = self.get_block_names(xing_element)

        column_element = _TABLE_MODIFY[trcode]
        block_name_main = column_element['main_block']
        table_name_main = 'xing_{}_{}_{}'.format(xing_name, block_name_main, cycle_start_date_time)

        # drop non main block table
        for block_name in block_names:
            if block_name != block_name_main:
                table_name = 'xing_{}_{}_{}'.format(xing_name, block_name, cycle_start_date_time).lower()

                DB.execute('drop table {}'.format(table_name))
                DB.commit()
                print('drop non main table {}'.format(table_name))
            else:
                print('don\'t drop main table {}'.format(table_name_main))

    def add_column(self, xing_element, _TABLE_MODIFY):
        """
        xing_element = {
            'xing_name': self.xing_name,
            'trcode': self.trcode,
            'phase': 'listened',
            'date_time': dt.today().strftime('%Y%m%d %H%M%S'),
            'xing_output_element': self._putData(szTrCode)
        }
        _TABLE_MODIFY = {
            't8411': {
                'main_block': 'OutBlock',
                'column_add': [
                    {
                        'source_table' 'OutBlock'
                        'column_name': 'shcode'
                    }
                ]
            },
            'HA_': {
                'main_block': 'OutBlock',
                'column_add': [
                    {
                        'source_table': 'nan',
                        'source_key': 'date_time',
                        'column_name': 'date_time',
                        'type': 'char',
                        'size': 15,
                        'extra': ''
                    }
                ]
            }
        }
        """
        DB = self.DB

        xing_name = xing_element['xing_name']
        trcode = xing_element['trcode']
        cycle_start_date_time = xing_element['date_time']['cycle_start'].split('.')[0].replace(' ', '_')
        block_names = self.get_block_names(xing_element)

        column_element = _TABLE_MODIFY[trcode]
        block_name_main = column_element['main_block']
        table_name_main = 'xing_{}_{}_{}'.format(xing_name, block_name_main, cycle_start_date_time)

        # modify main block table
        if column_element['column_add']:
            for column_description in column_element['column_add']:
                if column_description['source_table'] == 'nan':
                    column_name = 'c_' + column_description['column_name']
                    column_type = column_description['type']
                    column_size = column_description['size']
                    column_extra = column_description['extra']
                    if column_size > 0:
                        column_type = '{}({})'.format(column_type, column_size)

                    query_alter = 'ALTER TABLE {} ADD {} {} {};'.format(table_name_main, column_name, column_type, column_extra)
                    DB.execute(query_alter)
                    DB.commit()
                    print('alter main table {} | {}'.format(table_name_main, query_alter))

                if not column_description['source_table'] == 'nan':
                    source_table = column_description['source_table']
                    column_name = column_description['column_name']

                    block_schema = self.get_block_schema(trcode, source_table)
                    block_schema = self.change_block_schema_type(block_schema)

                    for idx, row in block_schema.iterrows():
                        if row['field'] == 'c_' + column_name:
                            column_name = row['field']
                            column_type = row['type_mysql']
                            column_extra = 'not null'

                            query_alter = 'ALTER TABLE {} ADD {} {} {};'.format(table_name_main, column_name, column_type, column_extra)
                            DB.execute(query_alter)
                            DB.commit()
                            print('alter main table {} | {}'.format(table_name_main, query_alter))


if __name__ == '__main__':

    import database_handler
    DB = database_handler.Database()


    query = 'CREATE TABLE xing_listened_results(\
                idx_god_follow INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,\
                id varchar(72) not null,\
                xing_name varchar(50) not null,\
                type char(10) not null,\
                trcode char(10) not null,\
                options varchar(500) not null,\
                inblocks varchar(500) not null,\
                cycle_start char(22) not null,\
                element_start char(22) not null,\
                occur_start char(22) not null,\
                occur_listened char(22) not null,\
                occur_end char(22) not null,\
                elapse_time float not null,\
                occur_value varchar(20) not null,\
                occur_size int not null,\
                status_final varchar(200) not null,\
                status_reaction varchar(200)\
            );'

    DB.execute('DROP TABLE xing_listened_results')

    DB.execute(query)
    DB.commit()
