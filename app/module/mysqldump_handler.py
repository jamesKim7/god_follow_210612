import sys
import os
import json
import subprocess
from datetime import datetime as dt


class MysqlDumpHandler():

    def __init__(self):
        self.id = ''
        self.password = ''
        self.db_name = ''
        self.mysql_dir = ''
        self.backup_dir = ''
        self.restore_dir = ''
        self.backup_table_list = ''
        self.restore_table_list = ''

    @staticmethod
    def get_today_made_tables(DB):
        date_today = dt.today().strftime('%Y%m%d')
        tables = [row[0] for row in DB.executeAll('show tables;') if row[0].find(date_today) > -1]

        return tables

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
            self.id = json_data['mysqldump_handler']['id']
            self.password = json_data['mysqldump_handler']['password']
            self.db_name = json_data['mysqldump_handler']['db_name']
            self.mysql_dir = json_data['mysqldump_handler']['mysql_dir']
            self.backup_dir = json_data['mysqldump_handler']['backup_dir']
            self.restore_dir = json_data['mysqldump_handler']['restore_dir']
            # self.backup_table_list = json_data['mysqldump_handler']['backup_table_list']
            # self.restore_table_list = json_data['mysqldump_handler']['restore_table_list']

    def backup_table(self, options):
        self.backup_table_list = options['table_list']

        id = self.id
        password = self.password
        db_name = self.db_name
        mysql_dir = self.mysql_dir
        backup_dir = self.backup_dir
        backup_table_list = self.backup_table_list

        date_time = dt.today().strftime('%Y%m%d%H%M%S')

        if len(backup_table_list):
            os.chdir(mysql_dir)

            for table in backup_table_list:
                subprocess.call(
                    f'.\\mysqldump -u{id} -p{password} {db_name} {table} > {backup_dir}\\{table}_{date_time}.sql',
                    shell=True)

    def restore_table(self, options):
        self.restore_table_list = options['table_list']

        id = self.id
        password = self.password
        db_name = self.db_name
        mysql_dir = self.mysql_dir
        restore_dir = self.restore_dir
        restore_table_list = self.restore_table_list
        if len(restore_table_list):
            os.chdir(mysql_dir)

            for table in restore_table_list:
                subprocess.call(
                    f'mysql -u{id} -p{password} {db_name} -e "source {restore_dir}/{table}_{"20200829004301"}.sql"',
                    shell=True)


if __name__ == '__main__':

    # 수동으로 dump 하기
    """
    options = {
        'table_list': []
    }
    MDH = MysqlDumpHandler()
    MDH.get_config()
    MDH.backup_table(options)
    """

    # 자동으로 dump 하기
    import database_handler
    DB = database_handler.Database()
    MDH = MysqlDumpHandler()
    MDH.get_config()
    tables = MDH.get_today_made_tables(DB)
    options = {
        'table_list': tables
    }
    MDH.backup_table(options)
