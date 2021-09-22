from app.module import database_handler

import pandas as pd

DB = database_handler.Database()

if __name__ == '__main__':
    res = DB.executeAll('select shcode from xing_s3__outblock group by shcode;')
    s3_shcodes = [row[0] for row in res]
    pd.DataFrame(s3_shcodes).to_csv('s3_shcodes.csv')

    res = DB.executeAll('select shcode, gubun from xing_t8430;')
    pd.DataFrame(res).to_csv('t8430_shcodes.csv')