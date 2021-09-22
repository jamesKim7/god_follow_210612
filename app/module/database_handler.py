# file name : database_handler.py
# pwd : /god_follow_{version}/app/module/database_handler.py

import pymysql


class Database:
    def __init__(self):
        self.db = pymysql.connect(host='localhost',
                                  port=3306,
                                  user='root',
                                  password='kibum1261711',
                                  db='god_follow',
                                  charset='utf8')
        self.cursor = self.db.cursor(pymysql.cursors.SSCursor)
        self.cursor.execute('SET GLOBAL max_allowed_packet=536870912;')
        # 활동중인 커넥션이 닫히기 전까지 서버가 대기하는 시간
        self.cursor.execute('SET GLOBAL interactive_timeout=31536000')
        # 활동하지 않는 커넥션을 끊을때까지 서버가 대기하는 시간
        self.cursor.execute('SET GLOBAL wait_timeout=31536000')

    @staticmethod
    def check_special_character(query):
        query = query.replace('%', '')
        return query

    def execute(self, query, args={}):
        query = self.check_special_character(query)
        self.cursor.execute(query, args)

    def executeOne(self, query, args={}):
        self.cursor.execute(query, args)
        row = self.cursor.fetchone()
        return row

    def executeAll(self, query, args={}):
        self.cursor.execute(query, args)
        row = self.cursor.fetchall()
        return row

    def commit(self):
        self.db.commit()

    def close(self):
        self.db.close()
