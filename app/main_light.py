import os
import sys
import time
import json
import pythoncom
import pandas as pd
import pprint
import signal
import re
import psutil

import datetime
from datetime import datetime as dt

from multiprocessing import Process, Queue

from app.module import database_handler
from app.module import table_handler
from app.module import mysql_handler
# from app import main_real_listener
from app.module import tr_updater
# from app import main_backup
from app.module import xing

mode = ''


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


def get_config():
    root_dir = get_root_dir()

    with open('{}/app/files/config.json'.format(root_dir), 'r') as f:
        json_data = json.load(f)
        return json_data


def build_outblocks_refined():
    def build_meta_res():
        """ res 파일들의 meta data

            Example
            -------
            >>> build_meta_res()
            {
                't8413': {
                    'desc': '주식챠트(일주월)',
                    'input': {
                        't8413InBlock': {
                            'occurs': False,
                            'fields': [
                                {
                                    'name': 'shcode',
                                    'desc': '단축코드',
                                    'type': 'char',
                                    'size': 6
                                },
                                { ... },
                                ...
                            ]
                        }
                    },
                    'output': {
                        't8413OutBlock1': {
                            'occurs': True,
                            'fields': [ 'price', ... ]
                        },
                        ...
                    }
                },
                ...
            }
        """
        XINGAPI_PATH = 'C:/eBEST/xingAPI/'
        meta = {}

        fnames = filter(
            lambda x: not re.match(r'.*\_\d+\.res$', x),
            os.listdir(os.path.join(XINGAPI_PATH, 'res'))
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
            meta[fname.replace('.res', '')] = parse_file(
                open(os.path.join(XINGAPI_PATH, 'res/', fname)).readlines()
            )

        return meta

    res = build_meta_res()
    # print(res)
    outblocks_refined = {}
    for trcode in res.keys():
        intblocks = res[trcode]['input']
        outblocks = res[trcode]['output']
        # print(outblocks)
        outblocks_refined[trcode] = {}
        for outblock_name in outblocks.keys():
            # print(outblock_name)
            outblock_name_without_trcode = 'Out' + ''.join(outblock_name.split('Out')[1])
            outblocks_refined[trcode][outblock_name_without_trcode] = {}

            occurs = res[trcode]['output'][outblock_name]['occurs']
            if occurs:
                outblocks_refined[trcode][outblock_name_without_trcode]['type'] = 'occurs'
            else:
                outblocks_refined[trcode][outblock_name_without_trcode]['type'] = 'non_occurs'

            columns = res[trcode]['output'][outblock_name]['fields']
            columns_names = []
            for column in columns:
                columns_names.append(column['name'])
            outblocks_refined[trcode][outblock_name_without_trcode]['columns_names'] = columns_names
            outblocks_refined[trcode][outblock_name_without_trcode]['listened_data'] = []
            # print(outblocks_refined[trcode][outblock_name_without_trcode])

    return outblocks_refined


def get_config_xing_elements():
    # 대상 tables
    xing_elements_archive = {
        'all': {
            't1302': {
                'type': 'tr',
                'trcode': 't1302',
                'options': {
                    'request_continue': {
                        'criteria': '',
                        'overlap': ''
                    },
                    'shcode': 'all',
                    'gubun_t1302': '30sec'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'gubun': 0,
                            'time': ' ',
                            'cnt': 900
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['cts_time'],
                        'listened_data': []
                    },
                    'OutBlock1': {
                        'type': 'occurs',
                        'columns_names': ['chetime', 'close', 'sign', 'change', 'diff', 'chdegree', 'mdvolume',
                                          'msvolume',
                                          'revolume', 'mdchecnt', 'mschecnt', 'rechecnt', 'volume', 'open', 'high',
                                          'low',
                                          'cvolume', 'mdchecnttm', 'mschecnttm', 'totofferrem', 'totbidrem',
                                          'mdvolumetm', 'msvolumetm'],

                        'listened_data': []
                    }
                }
            },
            't1305': {
                'type': 'tr',
                'trcode': 't1305',
                'options': {
                    'request_continue': {
                        'criteria': 'shcode',
                        'overlap': 'yes'
                    },
                    'shcode': 'all',
                    'dwmcode': 'day'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'dwmcode': 1,
                            'date': ' ',
                            'idx': ' ',
                            'cnt': 500
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['cnt', 'date', 'idx'],
                        'listened_data': []
                    },
                    'OutBlock1': {
                        'type': 'occurs',
                        'columns_names': ['date', 'open', 'high', 'low', 'close', 'sign', 'change', 'diff', 'volume',
                                          'diff_vol', 'chdegree', 'sojinrate', 'changerate', 'fpvolume', 'covolume',
                                          'shcode', 'value', 'ppvolume', 'o_sign', 'o_change', 'o_diff', 'h_sign',
                                          'h_change', 'h_diff',
                                          'l_sign', 'l_change', 'l_diff', 'marketcap'],
                        'listened_data': []
                    }
                }
            },
            't1411': {
                'type': 'tr',
                'trcode': 't1411',
                'options': {
                    'gubun_t1411': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun': '',
                            'jongchk': '',
                            'jkrate': '',
                            'shcode': '',
                            'idx': ''
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['jkrate', 'sjkrate', 'idx'],
                        'listened_data': []
                    },
                    'OutBlock1': {
                        'type': 'occurs',
                        'columns_names': ['shcode', 'hname', 'jkrate', 'sjkrate', 'subprice', 'recprice', 'price',
                                          'sign',
                                          'change', 'diff', 'volume'],
                        'listened_data': []
                    }
                }
            },
            't8411': {
                'type': 'tr',
                'trcode': 't8411',
                'options': {
                    'request_continue': {
                        'criteria': '',
                        'overlap': ''
                    },
                    'shcode': 'all',
                    'ncnt': 1,
                    'date': 'today'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'ncnt': 1,
                            'qrycnt': 2000,
                            'nday': 0,
                            'sdate': '',
                            'stime': ' ',
                            'edate': '',
                            'etime': ' ',
                            'cts_date': ' ',
                            'cts_time': ' ',
                            'comp_yn': 'Y'
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['shcode', 'jisiga', 'jihigh', 'jilow', 'jiclose', 'jivolume', 'disiga',
                                          'dihigh',
                                          'dilow',
                                          'diclose', 'highend', 'lowend', 'cts_date', 'cts_time', 's_time', 'e_time',
                                          'dshmin', 'rec_count'],
                        'listened_data': []
                    },
                    'OutBlock1': {
                        'type': 'occurs',
                        'columns_names': ['date', 'time', 'open', 'high', 'low', 'close', 'jdiff_vol', 'jongchk',
                                          'rate',
                                          'pricechk'],
                        'listened_data': []

                    }
                }
            },
            't8412': {
                'type': 'tr',
                'trcode': 't8412',
                'options': {
                    'request_continue': {
                        'criteria': 'shcode',
                        'overlap': 'yes'
                    },
                    'shcode': 'all',
                    'ncnt': 1,
                    'date': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'ncnt': 1,
                            'qrycnt': 2000,
                            'nday': 0,
                            'sdate': '',
                            'stime': ' ',
                            'edate': '',
                            'etime': ' ',
                            'cts_date': ' ',
                            'cts_time': ' ',
                            'comp_yn': 'Y'
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['shcode', 'jisiga', 'jihigh', 'jilow', 'jiclose', 'jivolume', 'disiga',
                                          'dihigh',
                                          'dilow',
                                          'diclose', 'highend', 'lowend', 'cts_date', 'cts_time', 's_time', 'e_time',
                                          'dshmin', 'rec_count'],
                        'listened_data': []
                    },
                    'OutBlock1': {
                        'type': 'occurs',
                        'columns_names': ['date', 'time', 'open', 'high', 'low', 'close', 'jdiff_vol', 'value',
                                          'jongchk',
                                          'rate', 'sign'],
                        'listened_data': []

                    }
                }
            },
            't8413': {
                'type': 'tr',
                'trcode': 't8413',
                'options': {
                    'shcode': 'all',
                    'gubun_t8413': 'day',
                    'date': '20200904'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'gubun': 2,
                            'qrycnt': 2000,
                            'sdate': '',
                            'edate': '',
                            'cts_date': ' ',
                            'comp_yn': 'Y'
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['shcode', 'jisiga', 'jihigh', 'jilow', 'jiclose', 'jivolume', 'disiga',
                                          'dihigh',
                                          'dilow',
                                          'diclose', 'highend', 'lowend', 'cts_date', 's_time', 'e_time', 'dshmin',
                                          'rec_count'],
                        'listened_data': []
                    },
                    'OutBlock1': {
                        'type': 'occurs',
                        'columns_names': ['date', 'open', 'high', 'low', 'close', 'jdiff_vol', 'value', 'jongchk',
                                          'rate',
                                          'pricechk', 'ratevalue', 'sign'],
                        'listened_data': []

                    }
                }
            },
            't8430': {
                'type': 'tr',
                'trcode': 't8430',
                'options': {
                    'gubun_t8430': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun': ''
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'occurs',
                        'columns_names': ['hname', 'shcode', 'expcode', 'etfgubun', 'uplmtprice', 'dnlmtprice',
                                          'jnilclose',
                                          'memedan', 'recprice', 'gubun'],
                        'listened_data': []

                    }
                }
            },
            'S3_': {
                'type': 'real',
                'trcode': 'S3_',
                'options': {
                    'shcode': '005930'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': ''
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['chetime', 'sign', 'change', 'drate', 'price', 'opentime', 'open', 'hightime',
                                          'high',
                                          'lowtime', 'low', 'cgubun', 'cvolume', 'volume', 'value', 'mdvolume',
                                          'mdchecnt',
                                          'msvolume', 'mschecnt', 'cpower', 'w_avrg', 'offerho', 'bidho', 'status',
                                          'jnilvolume', 'shcode'],
                        'listened_data': []
                    }
                }
            },
            'H1_': {
                'type': 'real',
                'trcode': 'H1_',
                'options': {
                    'shcode': '005930'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': ''
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['hotime', 'offerho1', 'bidho1', 'offerrem1', 'bidrem1', 'offerho2', 'bidho2',
                                          'offerrem2', 'bidrem2',
                                          'offerho3', 'bidho3', 'offerrem3', 'bidrem3', 'offerho4', 'bidho4',
                                          'offerrem4',
                                          'bidrem4', 'offerho5',
                                          'bidho5', 'offerrem5', 'bidrem5', 'offerho6', 'bidho6', 'offerrem6',
                                          'bidrem6',
                                          'offerho7', 'bidho7',
                                          'offerrem7', 'bidrem7', 'offerho8', 'bidho8', 'offerrem8', 'bidrem8',
                                          'offerho9',
                                          'bidho9', 'offerrem9',
                                          'bidrem9', 'offerho10', 'bidho10', 'offerrem10', 'bidrem10', 'totofferrem',
                                          'totbidrem', 'donsigubun',
                                          'shcode', 'alloc_gubun'],
                        'listened_data': []
                    }
                }
            },
            'K3_': {
                'type': 'real',
                'trcode': 'K3_',
                'options': {
                    'shcode': 'kosdaq'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': ''
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['chetime', 'sign', 'change', 'drate', 'price', 'opentime', 'open', 'hightime',
                                          'high', 'lowtime', 'low', 'cgubun', 'cvolume', 'volume', 'value', 'mdvolume',
                                          'mdchecnt', 'msvolume', 'mschecnt', 'cpower', 'w_avrg', 'offerho', 'bidho',
                                          'status', 'jnilvolume', 'shcode'],
                        'listened_data': []
                    }
                }
            },
            'HA_': {
                'type': 'real',
                'trcode': 'HA_',
                'options': {
                    'shcode': 'kosdaq'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': ''
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['hotime', 'offerho1', 'bidho1', 'offerrem1', 'bidrem1', 'offerho2', 'bidho2',
                                          'offerrem2', 'bidrem2',
                                          'offerho3', 'bidho3', 'offerrem3', 'bidrem3', 'offerho4', 'bidho4',
                                          'offerrem4',
                                          'bidrem4',
                                          'offerho5', 'bidho5', 'offerrem5', 'bidrem5', 'offerho6', 'bidho6',
                                          'offerrem6',
                                          'bidrem6',
                                          'offerho7', 'bidho7', 'offerrem7', 'bidrem7', 'offerho8', 'bidho8',
                                          'offerrem8',
                                          'bidrem8',
                                          'offerho9', 'bidho9', 'offerrem9', 'bidrem9', 'offerho10', 'bidho10',
                                          'offerrem10', 'bidrem10',
                                          'totofferrem', 'totbidrem', 'donsigubun', 'shcode', 'alloc_gubun'],
                        'listened_data': []
                    }
                }
            },
            'OVC': {
                'type': 'real',
                'trcode': 'OVC',
                'options': {
                    'symbol': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'symbol': ''
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['symbol', 'ovsdate', 'kordate', 'trdtm', 'kortm', 'curpr', 'ydiffpr',
                                          'ydiffSign',
                                          'open', 'high', 'low', 'chgrate', 'trdq', 'totq', 'cgubun', 'mdvolume',
                                          'msvolume',
                                          'ovsmkend'],
                        'listened_data': []
                    }
                }
            },
            'OVH': {
                'type': 'real',
                'trcode': 'OVH',
                'options': {
                    'symbol': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'symbol': ''
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['symbol', 'hotime', 'offerho1', 'bidho1', 'offerrem1', 'bidrem1', 'offerno1',
                                          'bidno1',
                                          'offerho2', 'bidho2', 'offerrem2', 'bidrem2', 'offerno2', 'bidno2',
                                          'offerho3',
                                          'bidho3',
                                          'offerrem3', 'bidrem3', 'offerno3', 'bidno3', 'offerho4', 'bidho4',
                                          'offerrem4',
                                          'bidrem4',
                                          'offerno4', 'bidno4', 'offerho5', 'bidho5', 'offerrem5', 'bidrem5',
                                          'offerno5',
                                          'bidno5',
                                          'totoffercnt', 'totbidcnt', 'totofferrem', 'totbidrem'],
                        'listened_data': []
                    }
                }
            }
        },
        'day_by_day_tr': {
            't1302': {
                'type': 'tr',
                'trcode': 't1302',
                'options': {
                    'shcode': 'all',
                    'gubun_t1302': '30sec'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'gubun': 0,
                            'time': ' ',
                            'cnt': 900
                        }
                    }
                }
            },
            't1305': {
                'type': 'tr',
                'trcode': 't1305',
                'options': {
                    'shcode': 'all',
                    'dwmcode': 'day'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'dwmcode': 1,
                            'date': ' ',
                            'idx': ' ',
                            'cnt': 500
                        }
                    }
                }
            },
            't1403': {
                'type': 'tr',
                'trcode': 't1403',
                'options': {
                    'gubun_t1403': 'all',
                    'date_t1403': 'today'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun': '',
                            'styymm': '',
                            'enyymm': '',
                            'idx': ''
                        }
                    }
                }
            },
            't1404': {
                'type': 'tr',
                'trcode': 't1404',
                'options': {
                    'gubun_t1404': 'all',
                    'jongchk_t1404': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun': '',
                            'jongchk': '',
                            'cts_shcode': ''
                        }
                    }
                }
            },
            't1405': {
                'type': 'tr',
                'trcode': 't1405',
                'options': {
                    'gubun_t1405': 'all',
                    'jongchk_t1405': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun': '',
                            'jongchk': '',
                            'cts_shcode': ''
                        }
                    }
                }
            },
            't1410': {
                'type': 'tr',
                'trcode': 't1410',
                'options': {
                    'gubun_t1410': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun': '',
                            'cts_shcode': ''
                        }
                    }
                }
            },
            't1411': {
                'type': 'tr',
                'trcode': 't1411',
                'options': {
                    'gubun_t1411': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun': '',
                            'jongchk': '',
                            'jkrate': '',
                            'shcode': '',
                            'idx': ''
                        }
                    }
                }
            },
            't1602': {
                'type': 'tr',
                'trcode': 't1602',
                'options': {
                    'upcode': 'all',
                    'gubun1_t1602': 'all',
                    'cnt': 900
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'market': '',
                            'upcode': '',
                            'gubun1': '',
                            'gubun2': '',
                            'cts_time': '',
                            'cts_idx': '',
                            'cnt': '',
                            'gubun3': ''
                        }
                    }
                }
            },
            't1633': {
                'type': 'tr',
                'trcode': 't1633',
                'options': {
                    'gubun_t1633': 'all',
                    'gubun1_t1633': 'all',
                    'gubun2_t1633': 'non_cum',
                    'gubun3_t1633': 'day',
                    'gubun4_t1633': '0',
                    'date_t1633': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun': '',
                            'gubun1': '',
                            'gubun2': '',
                            'gubun3': '',
                            'fdate': '',
                            'tdate': '',
                            'gubun4': '0',
                            'date': ''
                        }
                    }
                }
            },
            't1636': {
                'type': 'tr',
                'trcode': 't1636',
                'options': {
                    'gubun_t1636': 'all',
                    'gubun1_t1636': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun': '',
                            'gubun1': '',
                            'gubun2': '',
                            'shcode': '',
                            'cts_idx': ''
                        }
                    }
                }
            },
            't1637': {
                'type': 'tr',
                'trcode': 't1637',
                'options': {
                    'gubun1_t1637': 'all',
                    'gubun2_t1637': 'all',
                    'shcode': 'all',
                    'date_t1637': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun1': '',
                            'gubun2': '',
                            'shcode': '',
                            'date': '',
                            'time': '',
                            'cts_idx': ''
                        }
                    }
                }
            },
            't1662': {
                'type': 'tr',
                'trcode': 't1662',
                'options': {
                    'gubun_t1662': 'all',
                    'gubun1_t1662': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun': '',
                            'gubun1': '',
                            'gubun3': ''
                        }
                    }
                }
            },
            't1665': {
                'type': 'tr',
                'trcode': 't1665',
                'options': {
                    'upcode': 'all',
                    'gubun2_t1665': 'non_cum',
                    'gubun3_t1665': 'day',
                    'date_t1665': 'temp'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'market': '',
                            'upcode': '',
                            'gubun2': '',
                            'gubun3': '',
                            'from_date': '',
                            'to_date': ''
                        }
                    }
                }
            },
            't1717': {
                'type': 'tr',
                'trcode': 't1717',
                'options': {
                    'shcode': 'all',
                    'gubun_t1717': 'non_cum',
                    'date_t1717': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'gubun': '',
                            'fromdt': '',
                            'todt': ''
                        }
                    }
                }
            },
            't1921': {
                'type': 'tr',
                'trcode': 't1921',
                'options': {
                    'shcode': 'all',
                    'gubun_t1921': 'all',
                    'date_t1921': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'gubun': '',
                            'date': '',
                            'idx': ''
                        }
                    }
                }
            },
            't1926': {
                'type': 'tr',
                'trcode': 't1926',
                'options': {
                    'shcode': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': ''
                        }
                    }
                }
            },
            't1927': {
                'type': 'tr',
                'trcode': 't1927',
                'options': {
                    'shcode': 'all',
                    'date': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'date': '',
                            'sdate': '',
                            'edate': ''
                        }
                    }
                }
            },
            't1941': {
                'type': 'tr',
                'trcode': 't1941',
                'options': {
                    'shcode': 'all',
                    'date_t1941': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'sdate': '',
                            'edate': ''
                        }
                    }
                }
            },
            't8413': {
                'type': 'tr',
                'trcode': 't8413',
                'options': {
                    'shcode': 'all',
                    'gubun_t8413': 'day',
                    'date': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'gubun': 2,
                            'qrycnt': 2000,
                            'sdate': '',
                            'edate': '',
                            'cts_date': ' ',
                            'comp_yn': 'Y'
                        }
                    }
                }
            },
            't8419': {
                'type': 'tr',
                'trcode': 't8419',
                'options': {
                    'upcode': 'all',
                    'ncnt': 2000,
                    'gubun': 'day',
                    'date': 'all',
                    'comp_yn': 'Y'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'gubun': '',
                            'qrycnt': '',
                            'sdate': '',
                            'edate': '',
                            'cts_date': '',
                            'comp_yn': 'Y'
                        }
                    }
                }
            },
            't8436': {
                'type': 'tr',
                'trcode': 't8436',
                'options': {
                    'gubun_t8436': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun': ''
                        }
                    }
                }
            }
        },
        'day_by_day_basic': {
            't1403': {
                'type': 'tr',
                'trcode': 't1403',
                'options': {
                    'gubun_t1403': 'all',
                    'date_t1403': 'today'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun': '',
                            'styymm': '',
                            'enyymm': '',
                            'idx': ''
                        }
                    }
                }
            },
            't1404': {
                'type': 'tr',
                'trcode': 't1404',
                'options': {
                    'gubun_t1404': 'all',
                    'jongchk_t1404': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun': '',
                            'jongchk': '',
                            'cts_shcode': ''
                        }
                    }
                }
            },
            't1405': {
                'type': 'tr',
                'trcode': 't1405',
                'options': {
                    'gubun_t1405': 'all',
                    'jongchk_t1405': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun': '',
                            'jongchk': '',
                            'cts_shcode': ''
                        }
                    }
                }
            },
            't1410': {
                'type': 'tr',
                'trcode': 't1410',
                'options': {
                    'gubun_t1410': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun': '',
                            'cts_shcode': ''
                        }
                    }
                }
            },
            't1411': {
                'type': 'tr',
                'trcode': 't1411',
                'options': {
                    'gubun_t1411': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun': '',
                            'jongchk': '',
                            'jkrate': '',
                            'shcode': '',
                            'idx': ''
                        }
                    }
                }
            },
            't8436': {
                'type': 'tr',
                'trcode': 't8436',
                'options': {
                    'gubun_t8436': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun': ''
                        }
                    }
                }
            }
        },
        'day_by_day_candle_chart': {
            't1302': {
                'type': 'tr',
                'trcode': 't1302',
                'options': {
                    'shcode': 'all',
                    'gubun_t1302': '30sec'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'gubun': 0,
                            'time': ' ',
                            'cnt': 900
                        }
                    }
                }
            },
            't1305': {
                'type': 'tr',
                'trcode': 't1305',
                'options': {
                    'shcode': 'all',
                    'dwmcode': 'day'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'dwmcode': 1,
                            'date': ' ',
                            'idx': ' ',
                            'cnt': 5
                        }
                    }
                }
            },
            't8413': {
                'type': 'tr',
                'trcode': 't8413',
                'options': {
                    'shcode': 'all',
                    'gubun_t8413': 'day',
                    'date': 'today'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'gubun': 2,
                            'qrycnt': 2000,
                            'sdate': '',
                            'edate': '',
                            'cts_date': ' ',
                            'comp_yn': 'Y'
                        }
                    }
                }
            },
        },
        'day_by_day_yeche_real': {
            't8436': {
                'type': 'tr',
                'trcode': 't8436',
                'options': {
                    'gubun_t8436': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun': ''
                        }
                    }
                }
            },
            'YS3_kospi': {
                'type': 'real',
                'trcode': 'YS3',
                'options': {
                    'shcode': 'kospi'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': ''
                        }
                    }
                }
            },
            'YK3_kosdaq': {
                'type': 'real',
                'trcode': 'YK3',
                'options': {
                    'shcode': 'kosdaq'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': ''
                        }
                    }
                }
            }
        },
        'derivatives': {
            # 't8401': {
            #     'type': 'tr',
            #     'trcode': 't8401',
            #     'options': {
            #     },
            #     'InBlocks': {
            #         'InBlock': {
            #             'type': 'non_occurs',
            #             'columns_names_and_values': {
            #                 'dummy': ''
            #             }
            #         }
            #     }
            # },
            't8414': {
                'type': 'tr',
                'trcode': 't8414',
                'options': {
                    'shcode_t8414': '20210715',
                    'date': 'yesterday'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'ncnt': 1,
                            'qrycnt': 2000,
                            'nday': '0',
                            'sdate': '',
                            'stime': '',
                            'edate': '',
                            'etime': '',
                            'cts_date': '',
                            'cts_time': '',
                            'comp_yn': 'Y'
                        }
                    }
                }
            },
            # 't8426': {
            #     'type': 'tr',
            #     'trcode': 't8426',
            #     'options': {
            #     },
            #     'InBlocks': {
            #         'InBlock': {
            #             'type': 'non_occurs',
            #             'columns_names_and_values': {
            #                 'dummy': ''
            #             }
            #         }
            #     }
            # },
            # 't8432': {
            #     'type': 'tr',
            #     'trcode': 't8432',
            #     'options': {
            #         'gubun_t8432': 'all'
            #     },
            #     'InBlocks': {
            #         'InBlock': {
            #             'type': 'non_occurs',
            #             'columns_names_and_values': {
            #                 'gubun': ''
            #             }
            #         }
            #     }
            # },
            # 't8433': {
            #     'type': 'tr',
            #     'trcode': 't8433',
            #     'options': {
            #     },
            #     'InBlocks': {
            #         'InBlock': {
            #             'type': 'non_occurs',
            #             'columns_names_and_values': {
            #                 'dummy': ''
            #             }
            #         }
            #     }
            # },
            # 't8435': {
            #     'type': 'tr',
            #     'trcode': 't8435',
            #     'options': {
            #         'gubun_t8435': 'all'
            #     },
            #     'InBlocks': {
            #         'InBlock': {
            #             'type': 'non_occurs',
            #             'columns_names_and_values': {
            #                 'gubun': ''
            #             }
            #         }
            #     }
            # },

        },
        'update_cum_data': {
            # 't1403': {
            #     'type': 'tr',
            #     'trcode': 't1403',
            #     'options': {
            #         'gubun_t1403': 'all',
            #         'date_t1403': 'all'
            #     },
            #     'InBlocks': {
            #         'InBlock': {
            #             'type': 'non_occurs',
            #             'columns_names_and_values': {
            #                 'gubun': '',
            #                 'styymm': '',
            #                 'enyymm': '',
            #                 'idx': ''
            #             }
            #         }
            #     }
            # },
            # 't1404': {
            #     'type': 'tr',
            #     'trcode': 't1404',
            #     'options': {
            #         'gubun_t1404': 'all',
            #         'jongchk_t1404': 'all'
            #     },
            #     'InBlocks': {
            #         'InBlock': {
            #             'type': 'non_occurs',
            #             'columns_names_and_values': {
            #                 'gubun': '',
            #                 'jongchk': '',
            #                 'cts_shcode': ''
            #             }
            #         }
            #     }
            # },
            # 't1405': {
            #     'type': 'tr',
            #     'trcode': 't1405',
            #     'options': {
            #         'gubun_t1405': 'all',
            #         'jongchk_t1405': 'all'
            #     },
            #     'InBlocks': {
            #         'InBlock': {
            #             'type': 'non_occurs',
            #             'columns_names_and_values': {
            #                 'gubun': '',
            #                 'jongchk': '',
            #                 'cts_shcode': ''
            #             }
            #         }
            #     }
            # },
            # 't1410': {
            #     'type': 'tr',
            #     'trcode': 't1410',
            #     'options': {
            #         'gubun_t1410': 'all'
            #     },
            #     'InBlocks': {
            #         'InBlock': {
            #             'type': 'non_occurs',
            #             'columns_names_and_values': {
            #                 'gubun': '',
            #                 'cts_shcode': ''
            #             }
            #         }
            #     }
            # },
            # 't1411': {
            #     'type': 'tr',
            #     'trcode': 't1411',
            #     'options': {
            #         'gubun_t1411': 'all'
            #     },
            #     'InBlocks': {
            #         'InBlock': {
            #             'type': 'non_occurs',
            #             'columns_names_and_values': {
            #                 'gubun': '',
            #                 'jongchk': '',
            #                 'jkrate': '',
            #                 'shcode': '',
            #                 'idx': ''
            #             }
            #         }
            #     }
            # },
            # 't1926': {
            #     'type': 'tr',
            #     'trcode': 't1926',
            #     'options': {
            #         'shcode': 'all'
            #     },
            #     'InBlocks': {
            #         'InBlock': {
            #             'type': 'non_occurs',
            #             'columns_names_and_values': {
            #                 'shcode': ''
            #             }
            #         }
            #     }
            # },
            't1927': {
                'type': 'tr',
                'trcode': 't1927',
                'options': {
                    'shcode': 'all',
                    'date': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'date': '',
                            'sdate': '',
                            'edate': ''
                        }
                    }
                }
            },
            't1941': {
                'type': 'tr',
                'trcode': 't1941',
                'options': {
                    'shcode': 'all',
                    'date_t1941': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'sdate': '',
                            'edate': ''
                        }
                    }
                }
            },
            # 't8436': {
            #     'type': 'tr',
            #     'trcode': 't8436',
            #     'options': {
            #         'gubun_t8436': 'all'
            #     },
            #     'InBlocks': {
            #         'InBlock': {
            #             'type': 'non_occurs',
            #             'columns_names_and_values': {
            #                 'gubun': ''
            #             }
            #         }
            #     }
            # }
        },
        'long_tr': {
            't8412': {
                'type': 'tr',
                'trcode': 't8412',
                'options': {
                    'request_continue': {
                        'criteria': 'shcode',
                        'overlap': 'yes'
                    },
                    'shcode': 'all',
                    'ncnt': 1,
                    'date': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'ncnt': 1,
                            'qrycnt': 2000,
                            'nday': 0,
                            'sdate': '',
                            'stime': ' ',
                            'edate': '',
                            'etime': ' ',
                            'cts_date': ' ',
                            'cts_time': ' ',
                            'comp_yn': 'Y'
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['shcode', 'jisiga', 'jihigh', 'jilow', 'jiclose', 'jivolume', 'disiga',
                                          'dihigh',
                                          'dilow',
                                          'diclose', 'highend', 'lowend', 'cts_date', 'cts_time', 's_time', 'e_time',
                                          'dshmin', 'rec_count'],
                        'listened_data': []
                    },
                    'OutBlock1': {
                        'type': 'occurs',
                        'columns_names': ['date', 'time', 'open', 'high', 'low', 'close', 'jdiff_vol', 'value',
                                          'jongchk',
                                          'rate', 'sign'],
                        'listened_data': []

                    }
                }
            }
        },
        'test': {
            't8436': {
                'type': 'tr',
                'trcode': 't8436',
                'options': {
                    'gubun_t8436': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun': ''
                        }
                    }
                }
            },
            't8411': {
                'type': 'tr',
                'trcode': 't8411',
                'options': {
                    'shcode': '207940',
                    'ncnt': 1,
                    'date': '20210830'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'ncnt': 1,
                            'qrycnt': 2000,
                            'nday': 0,
                            'sdate': '',
                            'stime': ' ',
                            'edate': '',
                            'etime': ' ',
                            'cts_date': ' ',
                            'cts_time': ' ',
                            'comp_yn': 'Y'
                        }
                    }
                }
            }
            # 'JIF': {
            #     'type': 'real',
            #     'trcode': 'JIF',
            #     'options': {
            #         'shcode': 'nan'
            #     },
            #     'InBlocks': {
            #         'InBlock': {
            #             'type': 'non_occurs',
            #             'columns_names_and_values': {
            #                 'jangubun': ''
            #             }
            #         }
            #     },
            #     'OutBlocks': {
            #         'OutBlock': {
            #             'type': 'non_occurs',
            #             'columns_names': ['jangubun', 'jstatus'],
            #             'listened_data': []
            #         }
            #     }
            # },
            # 'BMT': {
            #     'type': 'real',
            #     'trcode': 'BMT',
            #     'options': {
            #         'shcode': 'nan'
            #     },
            #     'InBlocks': {
            #         'InBlock': {
            #             'type': 'non_occurs',
            #             'columns_names_and_values': {
            #                 'upcode': ''
            #             }
            #         }
            #     },
            #     'OutBlocks': {
            #         'OutBlock': {
            #             'type': 'non_occurs',
            #             'columns_names': ['tjjtime', 'tjjcode1', 'msvolume1', 'mdvolume1', 'msvol1', 'msvalue1',
            #                               'mdvalue1', 'msval1',
            #                               'tjjcode2', 'msvolume2', 'mdvolume2', 'msvol2', 'msvalue2', 'mdvalue2',
            #                               'msval2',
            #                               'tjjcode3', 'msvolume3', 'mdvolume3', 'msvol3', 'msvalue3', 'mdvalue3',
            #                               'msval3',
            #                               'tjjcode4', 'msvolume4', 'mdvolume4', 'msvol4', 'msvalue4', 'mdvalue4',
            #                               'msval4',
            #                               'tjjcode5', 'msvolume5', 'mdvolume5', 'msvol5', 'msvalue5', 'mdvalue5',
            #                               'msval5',
            #                               'tjjcode6', 'msvolume6', 'mdvolume6', 'msvol6', 'msvalue6', 'mdvalue6',
            #                               'msval6',
            #                               'tjjcode7', 'msvolume7', 'mdvolume7', 'msvol7', 'msvalue7', 'mdvalue7',
            #                               'msval7',
            #                               'tjjcode8', 'msvolume8', 'mdvolume8', 'msvol8', 'msvalue8', 'mdvalue8',
            #                               'msval8',
            #                               'tjjcode9', 'msvolume9', 'mdvolume9', 'msvol9', 'msvalue9', 'mdvalue9',
            #                               'msval9',
            #                               'tjjcode10', 'msvolume10', 'mdvolume10', 'msvol10', 'msvalue10', 'mdvalue10',
            #                               'msval10',
            #                               'tjjcode11', 'msvolume11', 'mdvolume11', 'msvol11', 'msvalue11', 'mdvalue11',
            #                               'msval11',
            #                               'upcode', 'tjjcode0', 'msvolume0', 'mdvolume0', 'msvol0', 'msvalue0',
            #                               'mdvalue0', 'msval0'],
            #             'listened_data': []
            #         }
            #     }
            # }

        },
        'update_t1302': {
            't1302': {
                'type': 'tr',
                'trcode': 't1302',
                'options': {
                    'request_continue': {
                        'criteria': '',
                        'overlap': ''
                    },
                    'shcode': 'all',
                    'gubun_t1302': '30sec'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'gubun': 0,
                            'time': ' ',
                            'cnt': 900
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['cts_time'],
                        'listened_data': []
                    },
                    'OutBlock1': {
                        'type': 'occurs',
                        'columns_names': ['chetime', 'close', 'sign', 'change', 'diff', 'chdegree', 'mdvolume',
                                          'msvolume',
                                          'revolume', 'mdchecnt', 'mschecnt', 'rechecnt', 'volume', 'open', 'high',
                                          'low',
                                          'cvolume', 'mdchecnttm', 'mschecnttm', 'totofferrem', 'totbidrem',
                                          'mdvolumetm', 'msvolumetm'],

                        'listened_data': []
                    }
                }
            }
        },
        'update_t8412': {
            't8412': {
                'type': 'tr',
                'trcode': 't8412',
                'options': {
                    'request_continue': {
                        'criteria': '',  # 'shcode',
                        'overlap': 'yes'
                    },
                    'shcode': 'all',
                    'ncnt': 60,
                    'date': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'ncnt': 1,
                            'qrycnt': 2000,
                            'nday': 0,
                            'sdate': '',
                            'stime': ' ',
                            'edate': '',
                            'etime': ' ',
                            'cts_date': ' ',
                            'cts_time': ' ',
                            'comp_yn': 'Y'
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['shcode', 'jisiga', 'jihigh', 'jilow', 'jiclose', 'jivolume', 'disiga',
                                          'dihigh',
                                          'dilow',
                                          'diclose', 'highend', 'lowend', 'cts_date', 'cts_time', 's_time', 'e_time',
                                          'dshmin', 'rec_count'],
                        'listened_data': []
                    },
                    'OutBlock1': {
                        'type': 'occurs',
                        'columns_names': ['date', 'time', 'open', 'high', 'low', 'close', 'jdiff_vol', 'value',
                                          'jongchk',
                                          'rate', 'sign'],
                        'listened_data': []

                    }
                }
            }
        },
        'update_t8411': {
            't8411': {
                'type': 'tr',
                'trcode': 't8411',
                'options': {
                    'request_continue': {
                        'criteria': '',  # 'shcode',
                        'overlap': 'yes'
                    },
                    'shcode': 'all',
                    'ncnt': 1,
                    'date': '20210611'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'ncnt': 1,
                            'qrycnt': 2000,
                            'nday': 0,
                            'sdate': '',
                            'stime': ' ',
                            'edate': '',
                            'etime': ' ',
                            'cts_date': ' ',
                            'cts_time': ' ',
                            'comp_yn': 'Y'
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['shcode', 'jisiga', 'jihigh', 'jilow', 'jiclose', 'jivolume', 'disiga',
                                          'dihigh',
                                          'dilow',
                                          'diclose', 'highend', 'lowend', 'cts_date', 'cts_time', 's_time', 'e_time',
                                          'dshmin', 'rec_count'],
                        'listened_data': []
                    },
                    'OutBlock1': {
                        'type': 'occurs',
                        'columns_names': ['date', 'time', 'open', 'high', 'low', 'close', 'jdiff_vol', 'jongchk',
                                          'rate',
                                          'pricechk'],
                        'listened_data': []

                    }
                }
            },
        },
        'update_t8413': {
            't8413': {
                'type': 'tr',
                'trcode': 't8413',
                'options': {
                    'shcode': 'all',
                    'gubun_t8413': 'day',
                    'date': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'shcode': '',
                            'gubun': 2,
                            'qrycnt': 2000,
                            'sdate': '',
                            'edate': '',
                            'cts_date': ' ',
                            'comp_yn': 'Y'
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['shcode', 'jisiga', 'jihigh', 'jilow', 'jiclose', 'jivolume', 'disiga',
                                          'dihigh',
                                          'dilow',
                                          'diclose', 'highend', 'lowend', 'cts_date', 's_time', 'e_time', 'dshmin',
                                          'rec_count'],
                        'listened_data': []
                    },
                    'OutBlock1': {
                        'type': 'occurs',
                        'columns_names': ['date', 'open', 'high', 'low', 'close', 'jdiff_vol', 'value', 'jongchk',
                                          'rate',
                                          'pricechk', 'ratevalue', 'sign'],
                        'listened_data': []

                    }
                }
            }
        },
        'order_samsung': {
            'CSPAT00600': {
                'type': 'tr',
                'trcode': 'CSPAT00600',
                'options': {
                    'request_continue': {
                        'criteria': '',  # 'shcode',
                        'overlap': 'yes'
                    },
                    'shcode': 'all',
                    'ncnt': 60,
                    'date': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'AcntNo': '',
                            'InptPwd': '',
                            'IsuNo': '',
                            'OrdQty': '',
                            'OrdPrc': '',
                            'BnsTpCode': '',
                            'OrdprcPtnCode': '',
                            'MgntrnCode': '',
                            'LoanDt': '',
                            'OrdCndiTpCode': ''

                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['shcode', 'jisiga', 'jihigh', 'jilow', 'jiclose', 'jivolume', 'disiga',
                                          'dihigh',
                                          'dilow',
                                          'diclose', 'highend', 'lowend', 'cts_date', 'cts_time', 's_time', 'e_time',
                                          'dshmin', 'rec_count'],
                        'listened_data': []
                    },
                    'OutBlock1': {
                        'type': 'occurs',
                        'columns_names': ['date', 'time', 'open', 'high', 'low', 'close', 'jdiff_vol', 'value',
                                          'jongchk',
                                          'rate', 'sign'],
                        'listened_data': []

                    }
                }
            }
        },
        'update_theme_sector': {
            't1533': {
                'type': 'tr',
                'trcode': 't1533',
                'options': {
                    'request_continue': {
                        'criteria': '',
                        'overlap': ''
                    },
                    'gubun_t1533': ''
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun': '',
                            'chgdate': ''
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['bdate'],
                        'listened_data': []
                    },
                    'OutBlock1': {
                        'type': 'occurs',
                        'columns_names': ['tmname', 'totcnt', 'upcnt', 'dncnt', 'uprate', 'diff_vol', 'avgdiff',
                                          'chgdiff', 'tmcode'],
                        'listened_data': []
                    }
                }
            },
            't1537': {
                'type': 'tr',
                'trcode': 't1537',
                'options': {
                    'request_continue': {
                        'criteria': '',
                        'overlap': ''
                    },
                    'tmcode': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'tmcode': ''
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['upcnt', 'tmcnt', 'uprate', 'tmname'],
                        'listened_data': []
                    },
                    'OutBlock1': {
                        'type': 'occurs',
                        'columns_names': ['hname', 'price', 'sign', 'change', 'diff', 'volume', 'jniltime', 'shcode',
                                          'yeprice', 'open', 'high', 'low', 'value', 'marketcap'],
                        'listened_data': []
                    }
                }
            },
            't8424': {
                'type': 'tr',
                'trcode': 't8424',
                'options': {
                    'request_continue': {
                        'criteria': '',
                        'overlap': ''
                    }
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'gubun1': ''
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'occurs',
                        'columns_names': ['hname', 'upcode'],
                        'listened_data': []
                    }
                }
            },
            't1516': {
                'type': 'tr',
                'trcode': 't1516',
                'options': {
                    'request_continue': {
                        'criteria': '',
                        'overlap': ''
                    },
                    'upcode': 'all'
                },
                'InBlocks': {
                    'InBlock': {
                        'type': 'non_occurs',
                        'columns_names_and_values': {
                            'upcode': '',
                            'gubun': '',
                            'shcode': ''
                        }
                    }
                },
                'OutBlocks': {
                    'OutBlock': {
                        'type': 'non_occurs',
                        'columns_names': ['shcode', 'pricejisu', 'sign', 'change', 'jdiff'],
                        'listened_data': []
                    },
                    'OutBlock1': {
                        'type': 'occurs',
                        'columns_names': ['hname', 'price', 'sign', 'change', 'diff', 'volume', 'open', 'high', 'low',
                                          'sojinrate', 'beta', 'perx', 'frgsvolume', 'orgsvolume', 'diff_vol', 'shcode',
                                          'total', 'value'],
                        'listened_data': []
                    }
                }
            }
        }
    }

    outblocks = build_outblocks_refined()

    config_dict = get_config()
    xing_elements_type = config_dict['main'][mode]['xing_elements_type']
    xing_trcodes = xing_elements_archive[xing_elements_type]

    for xing_name in xing_trcodes:
        trcode = xing_trcodes[xing_name]['trcode']
        trcode_outblocks = outblocks[trcode]
        xing_trcodes[xing_name]['OutBlocks'] = trcode_outblocks

    result = xing_trcodes

    return result


def update_initial_xing_element(xing_elements):
    """
    id
    xing_name
    date_time
    occur
    status
    """
    for key, element_description in xing_elements.items():
        element_description['xing_name'] = key

        element_description['date_time'] = {}
        element_description['date_time']['cycle_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')
        element_description['date_time']['element_start'] = ''
        element_description['date_time']['occur_start'] = ''
        element_description['date_time']['occur_listened'] = ''
        element_description['date_time']['occur_end'] = ''
        element_description['date_time']['elapse_time'] = ''

        element_description['id'] = element_description['xing_name'] + '_' + \
                                    element_description['date_time']['cycle_start']

        element_description['occur'] = {}
        element_description['occur']['criteria'] = ''
        element_description['occur']['value'] = ''
        element_description['occur']['size'] = ''

        element_description['status'] = {}
        element_description['status']['final'] = ''
        element_description['status']['reaction'] = ''

        element_description['done'] = 0

        element_description['mode'] = mode

    return xing_elements


def child_real(queue_mother_out, queue_mother_in, queue_mysql_out, queue_mysql_in, xing_element, mode):
    print('child_real run, id:{}'.format(xing_element['id']))
    Xing = xing.Xing()
    Xing.login('demo')

    xing_name = xing_element['xing_name']
    trcode = xing_element['trcode']

    DB = database_handler.Database()

    option_shcode = ''
    shcodes = []

    if 'shcode' in xing_element['options']:
        option_shcode = xing_element['options']['shcode']

        if option_shcode == 'all':
            shcodes = [row[0] for row in DB.executeAll('SELECT c_shcode FROM xing_shcode_list_t8436;')]
        elif option_shcode == 'kospi':
            shcodes = [row[0] for row in DB.executeAll('SELECT c_shcode FROM xing_shcode_list_t8436 WHERE c_gubun = 1;')]
        elif option_shcode == 'kosdaq':
            shcodes = [row[0] for row in DB.executeAll('SELECT c_shcode FROM xing_shcode_list_t8436 WHERE c_gubun = 2;')]
        elif option_shcode == '005930':
            shcodes = ['005930']
        elif option_shcode == 'nan':
            shcodes = []
        else:
            print('Err: unknown shcode option | {}'.format(option_shcode))

    del DB

    if trcode in ['S3_', 'H1_', 'K3_', 'HA_', 'YK3', 'YS3']:
        while 1:
            listened_flag = queue_mother_in.get()
            if listened_flag == 'start':
                print('{}:\tget from mother START'.format(xing_name))
                print('{}:\tstart process'.format(xing_name))

                # InBlock 세팅
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['shcode'] = option_shcode

                columns = tuple(xing_element['OutBlocks']['OutBlock']['columns_names'])

                REAL = xing.Real_light(xing_name, trcode, columns, queue_mysql_out).addTarget(shcodes, xing_element,
                                                                                              'shcode')

                print('Listen {}| {}, {}...'.format(trcode, len(shcodes), shcodes[:10]))
                REAL.run()

                break
            else:
                pass

    elif trcode == 'OVC':
        option_symbol = xing_element['options']['symbol']

        symbols = []

        # InBlock 에 들어갈 변수 추출
        if option_symbol == 'all':
            symbols = ['VXU20', 'FDXMU20', 'FDAXU20', 'FESXU20', 'SIZ20', 'SSGU20', 'YIZ20', 'PLV20', 'RTYU20',
                       'M2KU20', 'HGZ20', 'MYMU20', 'QCZ20', 'YMU20', 'DXU20', 'SINU20', 'EMDU20', 'YGZ20', 'GCZ20',
                       'MGCZ20', 'GCZ20', 'ESU20', 'ZNZ20', 'ZFZ20', 'TNZ20', 'ZBZ20', 'STWNU20', 'ZTZ20', 'EDU20',
                       'SKUU20', 'SCHU20', 'FOAMZ20', 'HCHHU20', 'SKRWU20', 'SSIU20', 'SNUU20', 'SNSU20', 'FGBMU20',
                       'JYU20', 'MJYU20', 'J7U20', 'FGBLU20', 'FOATU20', 'NKDU20', 'NIYU20', 'FGBSU20', 'SUCU20',
                       'BRV20', 'CUSU20', 'ADU20', 'FBTPU20', 'M6AU20', 'RYU20', 'E7U20', 'M6EU20', 'UROU20', 'STWU20',
                       'SFU20', 'HSIU20', 'HMHU20', 'CDU20', 'MPU20', 'MCDU20', 'SIUU20', 'NEU20', 'PAZ20', 'HMCEU20',
                       'HCEIU20', 'NQU20', 'MNQU20', 'M6BU20', 'BPU20', 'RBV20', 'QMV20', 'SCNU20', 'CLV20', 'BZX20',
                       'HOV20', 'NGV20', 'QGV20', 'FVSU20']
        elif option_symbol == 'S&P500':
            symbols = ['ESU20']
        else:
            print('Err: unknown symbol option | {}'.format(option_symbol))

        # InBlock 세팅
        xing_element['InBlocks']['InBlock']['columns_names_and_values']['symbol'] = symbols

        InBlocks = xing_element['InBlocks']
        OutBlocks = xing_element['OutBlocks']

        symbols = xing_element['InBlocks']['InBlock']['columns_names_and_values']['symbol']
        columns = tuple(xing_element['OutBlocks']['OutBlock']['columns_names'])

        REAL = xing.Real_light(xing_name, trcode, columns, queue_mysql_out).addTarget(symbols, 'symbol')

        REAL.run()

        print('Listen {}_ {}'.format(trcode, str(symbols)))

    elif trcode == 'OVH':
        option_symbol = xing_element['options']['symbol']

        symbols = []

        # InBlock 에 들어갈 변수 추출
        if option_symbol == 'all':
            symbols = ['VXU20', 'FDXMU20', 'FDAXU20', 'FESXU20', 'SIZ20', 'SSGU20', 'YIZ20', 'PLV20', 'RTYU20',
                       'M2KU20', 'HGZ20', 'MYMU20', 'QCZ20', 'YMU20', 'DXU20', 'SINU20', 'EMDU20', 'YGZ20', 'GCZ20',
                       'MGCZ20', 'GCZ20', 'ESU20', 'ZNZ20', 'ZFZ20', 'TNZ20', 'ZBZ20', 'STWNU20', 'ZTZ20', 'EDU20',
                       'SKUU20', 'SCHU20', 'FOAMZ20', 'HCHHU20', 'SKRWU20', 'SSIU20', 'SNUU20', 'SNSU20', 'FGBMU20',
                       'JYU20', 'MJYU20', 'J7U20', 'FGBLU20', 'FOATU20', 'NKDU20', 'NIYU20', 'FGBSU20', 'SUCU20',
                       'BRV20', 'CUSU20', 'ADU20', 'FBTPU20', 'M6AU20', 'RYU20', 'E7U20', 'M6EU20', 'UROU20', 'STWU20',
                       'SFU20', 'HSIU20', 'HMHU20', 'CDU20', 'MPU20', 'MCDU20', 'SIUU20', 'NEU20', 'PAZ20', 'HMCEU20',
                       'HCEIU20', 'NQU20', 'MNQU20', 'M6BU20', 'BPU20', 'RBV20', 'QMV20', 'SCNU20', 'CLV20', 'BZX20',
                       'HOV20', 'NGV20', 'QGV20', 'FVSU20']
        elif option_symbol == 'S&P500':
            symbols = ['ESU20']
        else:
            print('Err: unknown symbol option | {}'.format(option_symbol))

        # InBlock 세팅
        xing_element['InBlocks']['InBlock']['columns_names_and_values']['symbol'] = symbols

        InBlocks = xing_element['InBlocks']
        OutBlocks = xing_element['OutBlocks']

        symbols = xing_element['InBlocks']['InBlock']['columns_names_and_values']['symbol']
        columns = tuple(xing_element['OutBlocks']['OutBlock']['columns_names'])

        # manager = xing.RealManager()

        # manager.addTask("OVC", columns, 0, queue_txt_writer).addTarget(symbols, 'symbol')

        # q1 = manager.getQueue('OVC')

        REAL = xing.Real_light(xing_name, trcode, columns, queue_mysql_out).addTarget(symbols, 'symbol')

        REAL.run()

        print('Listen {}_ {}'.format(trcode, str(symbols)))

    elif trcode == 'JIF':
        while 1:
            listened_flag = queue_mother_in.get()
            if listened_flag == 'start':
                print('{}:\tget from mother START'.format(xing_name))
                print('{}:\tstart process'.format(xing_name))

                # InBlock 세팅
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['jangubun'] = '0'

                symbols = ['0']
                columns = tuple(xing_element['OutBlocks']['OutBlock']['columns_names'])

                # xing application 생성, InBlocks 전달
                REAL = xing.Real_light(xing_name, trcode, columns, queue_mysql_out).addTarget(symbols, xing_element,
                                                                                              'jangubun')

                print('Listen {}_{}'.format(trcode, str(symbols)))
                REAL.run()

                break
            else:
                pass

    elif trcode == 'BMT':
        while 1:
            listened_flag = queue_mother_in.get()
            if listened_flag == 'start':
                print('{}:\tget from mother START'.format(xing_name))
                print('{}:\tstart process'.format(xing_name))

                # InBlock 세팅
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['upcode'] = '001'

                symbols = ['001']
                columns = tuple(xing_element['OutBlocks']['OutBlock']['columns_names'])

                # xing application 생성, InBlocks 전달
                REAL = xing.Real_light(xing_name, trcode, columns, queue_mysql_out).addTarget(symbols, xing_element,
                                                                                              'upcode')

                print('Listen {}_{}'.format(trcode, str(symbols)))
                REAL.run()

                break
            else:
                pass

    queue_mysql_out.put('done')
    print('sent done')

    return 1


def child_tr(queue_mother_out, queue_mother_in, queue_mysql_out, queue_mysql_in, xing_element, mode):
    print('child_tr run, id: {}'.format(xing_element['id']))

    xing_name = xing_element['xing_name']
    trcode = xing_element['trcode']

    Xing = xing.Xing()
    Xing.login('demo')

    # options update
    DB = database_handler.Database()

    shcodes = []
    upcodes = []
    sdate = ''
    edate = ''
    ncnt = 0

    if 'shcode' in xing_element['options'].keys():
        option_shcode = xing_element['options']['shcode']

        if option_shcode == 'all':
            res = DB.executeAll('select c_shcode from xing_shcode_list_t8436;')
            shcodes = [element[0] for element in res]
        elif option_shcode == '005930':
            shcodes = ['005930']
        elif option_shcode == '207940':
            shcodes = ['207940']
        else:
            print('Err: unknown shcode option | {}'.format(option_shcode))

    if 'upcode' in xing_element['options'].keys():
        option_shcode = xing_element['options']['upcode']

        if option_shcode == 'all':
            res = DB.executeAll('select c_upcode from xing_sector_list_t8424;')
            upcodes = [element[0] for element in res]
        else:
            print('Err: unknown shcode option | {}'.format(option_shcode))

    if 'date' in xing_element['options'].keys():
        option_date = xing_element['options']['date']

        if option_date == 'today':
            sdate = dt.today().strftime('%Y%m%d')
            edate = dt.today().strftime('%Y%m%d')
        elif option_date == 'yesterday':
            sdate = (dt.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
            edate = (dt.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
        elif option_date == 'all':
            sdate = ' '
            edate = '당일'
        elif option_date.isdigit() and len(option_date) == 8:
            sdate = option_date
            edate = option_date
        elif option_date.isdigit() and len(option_date) == 16:
            sdate = option_date[:8]
            edate = option_date[8:]
        else:
            print('Err: unknown date option | {}'.format(option_date))

    if 'ncnt' in xing_element['options'].keys():
        option_ncnt = xing_element['options']['ncnt']

        if isinstance(option_ncnt, int):
            ncnt = option_ncnt
        else:
            print('Err: unknown ncnt option | {}'.format(option_ncnt))

    del DB

    if trcode == 't0000':
        print('testing')
        while 1:
            time.sleep(10)

    elif trcode == 't1302':

        option_gubun = xing_element['options']['gubun_t1302']

        gubun = ''

        # InBlock 에 들어갈 변수 추출
        if option_gubun == '30sec':
            gubun = '0'
        elif option_gubun == '1min':
            gubun = '1'
        elif option_gubun == '3min':
            gubun = '2'
        else:
            print('Err: unknown dwmcode option | {}'.format(option_gubun))

        # InBlock 에 들어갈 변수 추출

        # InBlock 세팅
        xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun'] = gubun

        xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

        flag_start = 0
        for shcode in shcodes:
            if flag_start:
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['shcode'] = shcode
                xing_element['occur']['value'] = shcode

                xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)
            if shcode == '008470':
                flag_start = 1

        queue_mysql_out.put(xing_element)
        print('t1302:\tput to child_mysql DONE')

    elif trcode == 't1305':

        option_dwmcode = xing_element['options']['dwmcode']

        dwmcode = ''

        # InBlock 에 들어갈 변수 추출
        if option_dwmcode == 'day':
            dwmcode = '1'
        elif option_dwmcode == 'week':
            dwmcode = '2'
        elif option_dwmcode == 'month':
            dwmcode = '3'
        else:
            print('Err: unknown dwmcode option | {}'.format(option_dwmcode))

        # InBlock 에 들어갈 변수 추출

        # InBlock 세팅
        xing_element['InBlocks']['InBlock']['columns_names_and_values']['dwmcode'] = dwmcode

        xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

        for shcode in shcodes:
            xing_element['InBlocks']['InBlock']['columns_names_and_values']['shcode'] = shcode
            xing_element['InBlocks']['InBlock']['columns_names_and_values']['date'] = ''
            xing_element['occur']['value'] = shcode

            xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

            xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

        queue_mysql_out.put(xing_element)
        print('t1305:\tput to child_mysql DONE')

    elif trcode == 't1403':

        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':
                print(f'{trcode}:\tlistened START')
                print(f'{trcode}:\tstart process')

                option_gubun = xing_element['options']['gubun_t1403']
                option_date = xing_element['options']['date_t1403']

                gubun = ''
                dates = []

                # InBlock 에 들어갈 변수 추출
                if option_gubun == 'all':
                    gubun = '0'
                elif option_gubun == 'kospi':
                    gubun = '1'
                elif option_gubun == 'kosdaq':
                    gubun = '2'
                else:
                    print(f'Err: unknown gubun option | {option_gubun}')

                if option_date == 'all':
                    for i in range(40):
                        period = {
                            'styymm': str(int(dt.today().strftime('%Y')) - i) + '0101',
                            'enyymm': str(int(dt.today().strftime('%Y')) - i) + '1231'
                        }
                        dates.append(period)
                elif option_date == 'today':
                    dates = [{
                        'styymm': dt.today().strftime('%Y%m%d'),
                        'enyymm': dt.today().strftime('%Y%m%d')
                    }]
                else:
                    print(f'Err: unknown jongchk option | {option_date}')

                # InBlock 세팅
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun'] = gubun

                xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                for date in dates:
                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['styymm'] = date['styymm']
                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['enyymm'] = date['enyymm']
                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['idx'] = 0

                    xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mother_out.put('finished')
                print(f'{trcode}:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print(f'{trcode}:\tlistened from mother END')
                print(f'{trcode}:\tend process')
                break
            else:
                print(f'{trcode}:\tlistened from mother odd flag | {listened_flag}')
                print(f'{trcode}:\tend process')
                break

    elif trcode == 't1404':

        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':
                print(f'{trcode}:\tlistened START')
                print(f'{trcode}:\tstart process')

                option_gubun = xing_element['options']['gubun_t1404']
                option_jongchk = xing_element['options']['jongchk_t1404']

                gubun = ''
                jongchks = ''

                # InBlock 에 들어갈 변수 추출
                if option_gubun == 'all':
                    gubun = '0'
                elif option_gubun == 'kospi':
                    gubun = '1'
                elif option_gubun == 'kosdaq':
                    gubun = '2'
                else:
                    print(f'Err: unknown gubun option | {option_gubun}')

                if option_jongchk == 'all':
                    jongchks = [1, 2, 3, 4]
                else:
                    print(f'Err: unknown jongchk option | {option_jongchk}')

                # InBlock 세팅
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun'] = gubun

                xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                for jongchk in jongchks:
                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['jongchk'] = jongchk
                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['cts_shcode'] = ''
                    xing_element['occur']['value'] = jongchk

                    xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mother_out.put('finished')
                print(f'{trcode}:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print(f'{trcode}:\tlistened from mother END')
                print(f'{trcode}:\tend process')
                break
            else:
                print(f'{trcode}:\tlistened from mother odd flag | {listened_flag}')
                print(f'{trcode}:\tend process')
                break

    elif trcode == 't1405':

        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':
                print(f'{trcode}:\tlistened START')
                print(f'{trcode}:\tstart process')

                option_gubun = xing_element['options']['gubun_t1405']
                option_jongchk = xing_element['options']['jongchk_t1405']

                gubun = ''
                jongchks = ''

                # InBlock 에 들어갈 변수 추출
                if option_gubun == 'all':
                    gubun = '0'
                elif option_gubun == 'kospi':
                    gubun = '1'
                elif option_gubun == 'kosdaq':
                    gubun = '2'
                else:
                    print(f'Err: unknown gubun option | {option_gubun}')

                if option_jongchk == 'all':
                    jongchks = [1, 2, 3, 4, 5, 6, 7, 8, 9]
                else:
                    print(f'Err: unknown jongchk option | {option_jongchk}')

                # InBlock 세팅
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun'] = gubun

                xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                for jongchk in jongchks:
                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['jongchk'] = jongchk
                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['cts_shcode'] = ''
                    xing_element['occur']['value'] = jongchk

                    xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mother_out.put('finished')
                print(f'{trcode}:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print(f'{trcode}:\tlistened from mother END')
                print(f'{trcode}:\tend process')
                break
            else:
                print(f'{trcode}:\tlistened from mother odd flag | {listened_flag}')
                print(f'{trcode}:\tend process')
                break

    elif trcode == 't1410':

        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':
                print(f'{trcode}:\tlistened START')
                print(f'{trcode}:\tstart process')

                option_gubun = xing_element['options']['gubun_t1410']

                gubun = ''

                # InBlock 에 들어갈 변수 추출
                if option_gubun == 'all':
                    gubun = '0'
                elif option_gubun == 'kospi':
                    gubun = '1'
                elif option_gubun == 'kosdaq':
                    gubun = '2'
                else:
                    print(f'Err: unknown gubun option | {option_gubun}')

                # InBlock 세팅
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun'] = gubun

                xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mother_out.put('finished')
                print(f'{trcode}:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print(f'{trcode}:\tlistened from mother END')
                print(f'{trcode}:\tend process')
                break
            else:
                print(f'{trcode}:\tlistened from mother odd flag | {listened_flag}')
                print(f'{trcode}:\tend process')
                break

    elif trcode == 't1411':

        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':
                print(f'{trcode}:\tlistened START')
                print(f'{trcode}:\tstart process')

                option_gubun = xing_element['options']['gubun_t1411']

                gubun = ''

                # InBlock 에 들어갈 변수 추출
                if option_gubun == 'all':
                    gubun = '0'
                elif option_gubun == 'kospi':
                    gubun = '1'
                elif option_gubun == 'kosdaq':
                    gubun = '2'
                else:
                    print(f'Err: unknown gubun option | {option_gubun}')

                # InBlock 세팅
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun'] = gubun

                xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mother_out.put('finished')
                print(f'{trcode}:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print(f'{trcode}:\tlistened from mother END')
                print(f'{trcode}:\tend process')
                break
            else:
                print(f'{trcode}:\tlistened from mother odd flag | {listened_flag}')
                print(f'{trcode}:\tend process')
                break

    elif trcode == 't1602':

        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':
                print(f'{trcode}:\tlistened START')
                print(f'{trcode}:\tstart process')

                option_gubun1 = xing_element['options']['gubun1_t1602']
                cnt = xing_element['options']['cnt']

                upcodes = upcodes + ['550', '560', '700', '800', '900']
                gubun1s = []

                # InBlock 에 들어갈 변수 추출
                if option_gubun1 == 'all':
                    gubun1s = ['1', '2']
                elif option_gubun1 == 'volume':
                    gubun1s = ['1']
                elif option_gubun1 == 'amount':
                    gubun1s = ['2']
                else:
                    print(f'Err: unknown gubun option | {option_gubun1}')

                # InBlock 세팅
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['cnt'] = cnt

                # DO IT
                for upcode in upcodes:
                    for gubun1 in gubun1s:
                        xing_element['InBlocks']['InBlock']['columns_names_and_values']['upcode'] = upcode
                        xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun1'] = gubun1
                        xing_element['occur']['value1'] = upcode
                        xing_element['occur']['value2'] = gubun1
                        xing_element['InBlocks']['InBlock']['columns_names_and_values']['cts_time'] = ''

                        xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                        xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mother_out.put('finished')
                print(f'{trcode}:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print(f'{trcode}:\tlistened from mother END')
                print(f'{trcode}:\tend process')
                break
            else:
                print(f'{trcode}:\tlistened from mother odd flag | {listened_flag}')
                print(f'{trcode}:\tend process')
                break

    elif trcode == 't1633':

        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':
                print(f'{trcode}:\tlistened START')
                print(f'{trcode}:\tstart process')


                option_gubun = xing_element['options']['gubun_t1633']
                option_gubun1 = xing_element['options']['gubun1_t1633']
                option_gubun2 = xing_element['options']['gubun2_t1633']
                option_gubun3 = xing_element['options']['gubun3_t1633']
                option_gubun4 = xing_element['options']['gubun4_t1633']
                option_date = xing_element['options']['date_t1633']

                gubuns = []
                gubun1s = []
                gubun2 = ''
                gubun3 = ''
                gubun4 = ''
                dates = []

                # InBlock 에 들어갈 변수 추출
                if option_gubun == 'all':
                    gubuns = ['0', '1']
                elif option_gubun == 'kospi':
                    gubuns = ['0']
                elif option_gubun == 'kosdaq':
                    gubuns = ['1']
                else:
                    print(f'Err: unknown gubun option | {option_gubun}')

                if option_gubun1 == 'all':
                    gubun1s = ['0', '1']
                elif option_gubun1 == 'amount':
                    gubun1s = ['0']
                elif option_gubun1 == 'volume':
                    gubun1s = ['1']
                else:
                    print(f'Err: unknown gubun option | {option_gubun1}')

                if option_gubun2 == 'non_cum':
                    gubun2 = '0'
                elif option_gubun2 == 'cum':
                    gubun2 = '1'
                else:
                    print(f'Err: unknown gubun option | {option_gubun2}')

                if option_gubun3 == 'day':
                    gubun3 = 1
                elif option_gubun3 == 'week':
                    gubun3 = 2
                elif option_gubun3 == 'month':
                    gubun3 = 3
                else:
                    print(f'Err: unknown gubun option | {option_gubun3}')

                if option_date == 'all':
                    total_days = (dt.today() - dt.strptime('20010801', '%Y%m%d')).days
                    loop_range = 500
                    loop_count = int(total_days / loop_range) + 1
                    for i in range(loop_count):
                        period = {
                            # 해당 tr code의 연속조회 로직에 문제가 있어서 요청 데이터간 겹치는 구간 있도록 요청함.
                            'fdate': '',
                            'tdate': (dt.today() - datetime.timedelta(days=loop_range * i)).strftime('%Y%m%d')
                        }
                        dates.append(period)
                elif option_date == 'today':
                    dates = [{
                        'fdate': '',
                        'tdate': dt.today().strftime('%Y%m%d')
                    }]
                else:
                    print(f'Err: unknown gubun option | {option_date}')

                # InBlock 세팅
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun2'] = gubun2
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun3'] = gubun3
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun4'] = gubun4

                # DO IT
                for gubun in gubuns:
                    for gubun1 in gubun1s:
                        for date in dates:
                            xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun'] = gubun
                            xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun1'] = gubun1
                            xing_element['InBlocks']['InBlock']['columns_names_and_values']['fdate'] = date['fdate']
                            xing_element['InBlocks']['InBlock']['columns_names_and_values']['tdate'] = date['tdate']
                            xing_element['occur']['value1'] = gubun
                            xing_element['occur']['value2'] = gubun1

                            xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                            xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mother_out.put('finished')
                print(f'{trcode}:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print(f'{trcode}:\tlistened from mother END')
                print(f'{trcode}:\tend process')
                break
            else:
                print(f'{trcode}:\tlistened from mother odd flag | {listened_flag}')
                print(f'{trcode}:\tend process')
                break

    elif trcode == 't1636':

        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':
                print(f'{trcode}:\tlistened START')
                print(f'{trcode}:\tstart process')


                option_gubun = xing_element['options']['gubun_t1636']
                option_gubun1 = xing_element['options']['gubun1_t1636']

                gubuns = []
                gubun1s = []

                # InBlock 에 들어갈 변수 추출
                if option_gubun == 'all':
                    gubuns = ['0', '1']
                elif option_gubun == 'kospi':
                    gubuns = ['0']
                elif option_gubun == 'kosdaq':
                    gubuns = ['1']
                else:
                    print(f'Err: unknown gubun option | {option_gubun}')

                if option_gubun1 == 'all':
                    gubun1s = ['0', '1']
                elif option_gubun1 == 'volume':
                    gubun1s = ['0']
                elif option_gubun1 == 'amount':
                    gubun1s = ['1']
                else:
                    print(f'Err: unknown gubun option | {option_gubun1}')

                # InBlock 세팅

                # DO IT
                for gubun in gubuns:
                    for gubun1 in gubun1s:
                        xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun'] = gubun
                        xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun1'] = gubun1
                        xing_element['occur']['value1'] = gubun
                        xing_element['occur']['value2'] = gubun1
                        xing_element['occur']['value3'] = dt.today().strftime('%Y%m%d')

                        xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                        xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mother_out.put('finished')
                print(f'{trcode}:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print(f'{trcode}:\tlistened from mother END')
                print(f'{trcode}:\tend process')
                break
            else:
                print(f'{trcode}:\tlistened from mother odd flag | {listened_flag}')
                print(f'{trcode}:\tend process')
                break

    elif trcode == 't1637':

        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':
                print(f'{trcode}:\tlistened START')
                print(f'{trcode}:\tstart process')


                option_gubun1 = xing_element['options']['gubun1_t1637']
                option_gubun2 = xing_element['options']['gubun2_t1637']

                gubun1s = []
                gubun2s = []

                if option_gubun1 == 'all':
                    gubun1s = ['0', '1']
                elif option_gubun1 == 'volume':
                    gubun1s = ['0']
                elif option_gubun1 == 'amount':
                    gubun1s = ['1']
                else:
                    print(f'Err: unknown gubun option | {option_gubun1}')

                # InBlock 에 들어갈 변수 추출
                if option_gubun2 == 'all':
                    gubun2s = ['0', '1']
                elif option_gubun2 == 'today':
                    gubun2s = ['0']
                elif option_gubun2 == 'period':
                    gubun2s = ['1']
                else:
                    print(f'Err: unknown gubun option | {option_gubun2}')

                # InBlock 세팅

                # DO IT
                for gubun1 in gubun1s:
                    for gubun2 in gubun2s:
                        for shcode in shcodes:
                            xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun1'] = gubun1
                            xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun2'] = gubun2
                            xing_element['InBlocks']['InBlock']['columns_names_and_values']['shcode'] = shcode
                            xing_element['occur']['value1'] = gubun1
                            xing_element['occur']['value2'] = gubun2

                            xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                            # 시간 조회
                            if gubun2 == '0':
                                xing_element['InBlocks']['InBlock']['columns_names_and_values']['cts_idx'] = 9999
                                xing_element['InBlocks']['InBlock']['columns_names_and_values']['date'] = ''

                                xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                            # 기간 조회
                            elif gubun2 == '1':
                                xing_element['InBlocks']['InBlock']['columns_names_and_values']['cts_idx'] = ''

                                xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)


                queue_mother_out.put('finished')
                print(f'{trcode}:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print(f'{trcode}:\tlistened from mother END')
                print(f'{trcode}:\tend process')
                break
            else:
                print(f'{trcode}:\tlistened from mother odd flag | {listened_flag}')
                print(f'{trcode}:\tend process')
                break

    elif trcode == 't1662':

        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':
                print(f'{trcode}:\tlistened START')
                print(f'{trcode}:\tstart process')


                option_gubun = xing_element['options']['gubun_t1662']
                option_gubun1 = xing_element['options']['gubun1_t1662']

                gubuns = []
                gubun1s = []

                # InBlock 에 들어갈 변수 추출
                if option_gubun == 'all':
                    gubuns = ['0', '1']
                elif option_gubun == 'kospi':
                    gubuns = ['0']
                elif option_gubun == 'kosdaq':
                    gubuns = ['1']
                else:
                    print(f'Err: unknown gubun option | {option_gubun}')

                if option_gubun1 == 'all':
                    gubun1s = ['0', '1']
                elif option_gubun1 == 'volume':
                    gubun1s = ['1']
                elif option_gubun1 == 'amount':
                    gubun1s = ['0']
                else:
                    print(f'Err: unknown gubun option | {option_gubun1}')

                # InBlock 세팅

                # DO IT
                for gubun in gubuns:
                    for gubun1 in gubun1s:
                        xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun'] = gubun
                        xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun1'] = gubun1
                        xing_element['occur']['value1'] = gubun
                        xing_element['occur']['value2'] = gubun1
                        xing_element['occur']['value3'] = dt.today().strftime('%Y%m%d')

                        xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                        xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mother_out.put('finished')
                print(f'{trcode}:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print(f'{trcode}:\tlistened from mother END')
                print(f'{trcode}:\tend process')
                break
            else:
                print(f'{trcode}:\tlistened from mother odd flag | {listened_flag}')
                print(f'{trcode}:\tend process')
                break

    elif trcode == 't1665':

        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':
                print(f'{trcode}:\tlistened START')
                print(f'{trcode}:\tstart process')

                option_gubun2 = xing_element['options']['gubun2_t1665']
                option_gubun3 = xing_element['options']['gubun3_t1665']
                option_date = xing_element['options']['date_t1665']

                upcodes = upcodes + ['550', '560', '700', '800', '900']
                dates = []
                gubun2 = ''
                gubun3 = ''

                # InBlock 에 들어갈 변수 추출
                if option_gubun2 == 'non_cum':
                    gubun2 = '1'
                elif option_gubun2 == 'cum':
                    gubun2 = '2'
                else:
                    print(f'Err: unknown gubun option | {option_gubun2}')

                if option_gubun3 == 'day':
                    gubun3 = '1'
                elif option_gubun3 == 'week':
                    gubun3 = '2'
                elif option_gubun3 == 'month':
                    gubun3 = '3'
                else:
                    print(f'Err: unknown gubun option | {option_gubun3}')

                if option_date == 'all':
                    total_days = (dt.today() - dt.strptime('20010101', '%Y%m%d')).days
                    loop_range = 250
                    loop_count = int(total_days / loop_range) + 1
                    for i in range(loop_count):
                        period = {
                            'from_date': (dt.today() - (datetime.timedelta(days=loop_range * (i + 1) - 1))).strftime('%Y%m%d'),
                            'to_date': (dt.today() - datetime.timedelta(days=loop_range * i)).strftime('%Y%m%d')
                        }
                        dates.append(period)
                elif option_date == 'today':
                    dates = [{
                        'from_date': dt.today().strftime('%Y%m%d'),
                        'to_date': dt.today().strftime('%Y%m%d')
                    }]
                elif option_date == 'temp':
                    dates = [{
                        'from_date': '',
                        'to_date': dt.today().strftime('%Y%m%d')
                    }]
                else:
                    print(f'Err: unknown gubun option | {option_date}')

                # InBlock 세팅
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun2'] = gubun2
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun3'] = gubun3

                # DO IT
                flag_start = 1
                for upcode in upcodes:
                    if upcode == '652':
                        flag_start = 1
                    if flag_start:
                        date = dates[0]
                        if not gubun3 == 1:
                            xing_element['InBlocks']['InBlock']['columns_names_and_values']['upcode'] = upcode
                            xing_element['InBlocks']['InBlock']['columns_names_and_values']['from_date'] = date['from_date']
                            xing_element['InBlocks']['InBlock']['columns_names_and_values']['to_date'] = date['to_date']
                            xing_element['occur']['value'] = upcode

                            while xing_element['InBlocks']['InBlock']['columns_names_and_values']['to_date']:
                                xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                                xing_element = xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                                # t1665 는 연속조회 기능이 활성화 되어 있지 않아서 조회기간 update 해서 "조회" 해주어야 함.
                                to_date = xing_element['InBlocks']['InBlock']['columns_names_and_values']['to_date']
                                if to_date:
                                    to_date = (dt.strptime(to_date, '%Y%m%d') - datetime.timedelta(days=1)).strftime('%Y%m%d')
                                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['to_date'] = to_date
                        else:
                            print(f'gubun3 가 1이 아닐 때는 아직 준비되지 않았음.')
                            exit()

                        queue_mother_out.put('finished')
                        print(f'{trcode}:\tput to mother FINISHED')


            elif listened_flag == 'end':
                print(f'{trcode}:\tlistened from mother END')
                print(f'{trcode}:\tend process')
                break
            else:
                print(f'{trcode}:\tlistened from mother odd flag | {listened_flag}')
                print(f'{trcode}:\tend process')
                break

    elif trcode == 't1717':

        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':
                print(f'{trcode}:\tlistened START')
                print(f'{trcode}:\tstart process')

                option_gubun = xing_element['options']['gubun_t1717']
                option_date = xing_element['options']['date_t1717']

                dates = []
                gubun = ''
                fromdt: ''
                todt: ''

                # InBlock 에 들어갈 변수 추출
                if option_gubun == 'non_cum':
                    gubun = '0'
                elif option_gubun == 'cum':
                    gubun = '1'
                else:
                    print(f'Err: unknown gubun option | {option_gubun}')

                if option_date == 'all':
                    fromdt = ' '
                    todt = dt.today().strftime('%Y%m%d')
                elif option_date == 'today':
                    fromdt = dt.today().strftime('%Y%m%d')
                    todt = dt.today().strftime('%Y%m%d')
                else:
                    print(f'Err: unknown gubun option | {option_date}')

                # InBlock 세팅
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun'] = gubun

                # DO IT
                flag_start = 0
                for shcode in shcodes:
                    if shcode == '043910':
                        flag_start = 1
                    if flag_start:
                        if gubun == '0':
                            xing_element['InBlocks']['InBlock']['columns_names_and_values']['shcode'] = shcode
                            xing_element['InBlocks']['InBlock']['columns_names_and_values']['fromdt'] = fromdt
                            xing_element['InBlocks']['InBlock']['columns_names_and_values']['todt'] = todt
                            xing_element['occur']['value'] = shcode

                            while xing_element['InBlocks']['InBlock']['columns_names_and_values']['todt']:
                                xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                                xing_element = xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                                # t1717 는 연속조회 기능이 활성화 되어 있지 않아서 조회기간 update 해서 "조회" 해주어야 함.
                                to_date = xing_element['InBlocks']['InBlock']['columns_names_and_values']['todt']
                                if to_date:
                                    to_date = (dt.strptime(to_date, '%Y%m%d') - datetime.timedelta(days=1)).strftime('%Y%m%d')
                                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['todt'] = to_date
                        else:
                            print(f'gubun3 가 0이 아닐 때는 아직 준비되지 않았음.')
                            exit()

                        queue_mother_out.put('finished')
                        print(f'{trcode}:\tput to mother FINISHED')


            elif listened_flag == 'end':
                print(f'{trcode}:\tlistened from mother END')
                print(f'{trcode}:\tend process')
                break
            else:
                print(f'{trcode}:\tlistened from mother odd flag | {listened_flag}')
                print(f'{trcode}:\tend process')
                break

    elif trcode == 't1921':
        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':

                # InBlock 에 들어갈 변수 추출
                option_gubun = xing_element['options']['gubun_t1921']
                option_date = xing_element['options']['date_t1921']

                gubuns = []
                date = ''

                # InBlock 에 들어갈 변수 추출

                if option_gubun == 'all':
                    gubuns = [1, 2]
                elif option_gubun == 'loan':
                    gubuns = [1]
                elif option_gubun == 'borrow_stock':
                    gubuns = [2]
                else:
                    print(f'Err: unknown gubun option | {option_gubun}')

                if option_date == 'all':
                    date = ' '
                elif option_date == 'today':
                    date = dt.today().strftime('%Y%m%d')
                else:
                    print(f'Err: unknown gubun option | {option_date}')

                # InBlock 세팅 except shcode

                xing_element['InBlocks']['InBlock']['columns_names_and_values']['date'] = date

                # DO IT
                for shcode in shcodes:
                    for gubun in gubuns:
                        xing_element['InBlocks']['InBlock']['columns_names_and_values']['shcode'] = shcode
                        xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun'] = gubun
                        xing_element['InBlocks']['InBlock']['columns_names_and_values']['date'] = ' '
                        xing_element['occur']['value'] = gubun

                        xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                        xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mother_out.put('finished')
                print('t8411:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print('t8411:\tlistened from mother END')
                print('t8411:\tend process')
                break
            else:
                print('t8411:\tlistened from mother odd flag | {}'.format(listened_flag))
                print('t8411:\tend process')
                break

    elif trcode == 't1926':
        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':

                # InBlock 에 들어갈 변수 추출

                # InBlock 세팅 except shcode
                # DO IT
                for shcode in shcodes:
                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['shcode'] = shcode
                    xing_element['occur']['value'] = shcode

                    xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                    xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mother_out.put('finished')
                print('t1926:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print('t1926:\tlistened from mother END')
                print('t1926:\tend process')
                break
            else:
                print(f't1926:\tlistened from mother odd flag | {listened_flag}')
                print('t1926:\tend process')
                break

    elif trcode == 't1927':
        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':

                # InBlock 에 들어갈 변수 추출

                # InBlock 세팅 except shcode
                # t1927 은 2008 이후 자료만 제공하는 것 같다.
                if sdate == ' ':
                    sdate = '20080101'
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['sdate'] = sdate
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['edate'] = edate

                flag_start = 0
                # DO IT
                for shcode in shcodes:
                    if flag_start:
                        xing_element['InBlocks']['InBlock']['columns_names_and_values']['shcode'] = shcode
                        xing_element['InBlocks']['InBlock']['columns_names_and_values']['date'] = ''
                        xing_element['occur']['value'] = shcode

                        xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                        xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)
                    if shcode == '000050':
                        flag_start = 1

                queue_mother_out.put('finished')
                print('t1927:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print('t1927:\tlistened from mother END')
                print('t1927:\tend process')
                break
            else:
                print(f't1927:\tlistened from mother odd flag | {listened_flag}')
                print('t1927:\tend process')
                break

    elif trcode == 't1941':
        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':

                # InBlock 에 들어갈 변수 추출
                option_date = xing_element['options']['date_t1941']

                dates = []

                # InBlock 에 들어갈 변수 추출
                if option_date == 'all':
                    total_days = (dt.today() - dt.strptime('20120101', '%Y%m%d')).days
                    loop_count = int(total_days / 100) + 1
                    for i in range(loop_count):
                        period = {
                            'sdate': (dt.today() - (datetime.timedelta(days=100 * (i + 1) - 1))).strftime('%Y%m%d'),
                            'edate': (dt.today() - datetime.timedelta(days=100 * i)).strftime('%Y%m%d')
                        }
                        dates.append(period)
                elif option_date == 'today':
                    dates = [{
                        'sdate': dt.today().strftime('%Y%m%d'),
                        'edate': dt.today().strftime('%Y%m%d')
                    }]
                else:
                    print(f'Err: unknown gubun option | {option_date}')

                # InBlock 세팅 except shcode

                # DO IT
                for shcode in shcodes:
                    for date in dates:
                        xing_element['InBlocks']['InBlock']['columns_names_and_values']['shcode'] = shcode
                        xing_element['InBlocks']['InBlock']['columns_names_and_values']['sdate'] = date['sdate']
                        xing_element['InBlocks']['InBlock']['columns_names_and_values']['edate'] = date['edate']

                        xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                        xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mother_out.put('finished')
                print('t1927:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print('t1927:\tlistened from mother END')
                print('t1927:\tend process')
                break
            else:
                print(f't1927:\tlistened from mother odd flag | {listened_flag}')
                print('t1927:\tend process')
                break

    elif trcode == 't8411':
        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':

                # InBlock 에 들어갈 변수 추출

                # InBlock 세팅 except shcode
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['ncnt'] = ncnt
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['sdate'] = sdate
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['edate'] = edate

                # InBlock 세팅 except shcode
                # DO IT
                for shcode in shcodes:
                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['shcode'] = shcode
                    xing_element['occur']['criteria'] = 'shcode'
                    xing_element['occur']['value'] = shcode

                    xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                    xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mother_out.put('finished')
                print('t8411:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print('t8411:\tlistened from mother END')
                print('t8411:\tend process')
                break
            else:
                print('t8411:\tlistened from mother odd flag | {}'.format(listened_flag))
                print('t8411:\tend process')
                break

    elif trcode == 't8412':

        # InBlock 에 들어갈 변수 추출

        xing_element['InBlocks']['InBlock']['columns_names_and_values']['ncnt'] = ncnt
        xing_element['InBlocks']['InBlock']['columns_names_and_values']['sdate'] = sdate
        xing_element['InBlocks']['InBlock']['columns_names_and_values']['edate'] = edate

        # InBlock 세팅 except shcode

        # DO IT

        for shcode in shcodes:
            xing_element['InBlocks']['InBlock']['columns_names_and_values']['shcode'] = shcode

            xing_element['occur']['value'] = shcode

            xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

            xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

        queue_mysql_out.put(xing_element)
        print('t8412:\tput to child_mysql DONE')

    elif trcode == 't8413':

        option_gubun = xing_element['options']['gubun_t8413']

        gubun = 0

        # InBlock 에 들어갈 변수 추출

        if option_gubun == 'day':
            gubun = '2'
        elif option_gubun == 'week':
            gubun = '3'
        elif option_gubun == 'month':
            gubun = '4'
        else:
            print('Err: unknown ncnt option | {}'.format(option_gubun))

        # InBlock 세팅 except shcode
        xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun'] = gubun
        # 공통 date 논리 적용
        xing_element['InBlocks']['InBlock']['columns_names_and_values']['sdate'] = sdate
        xing_element['InBlocks']['InBlock']['columns_names_and_values']['edate'] = edate

        # InBlock 세팅 except shcode
        # DO IT
        for shcode in shcodes:
            xing_element['InBlocks']['InBlock']['columns_names_and_values']['shcode'] = shcode
            xing_element['InBlocks']['InBlock']['columns_names_and_values']['cts_date'] = ''
            xing_element['occur']['value'] = shcode

            xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

            xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

        queue_mysql_out.put(xing_element)
        print('t8413:\tput to child_mysql DONE')

    elif trcode == 't8414':
        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':
                print('t8414:\tlistened START')
                print('t8414:\tstart process')

                option_shcode = xing_element['options']['shcode_t8414']

                shcodes = []

                # InBlock 에 들어갈 변수 추출
                if option_shcode == 'all':
                    DB = database_handler.Database()
                    trcodes_table = ['t8432', 't8433', 't8435']
                    for trcode_table in trcodes_table:
                        table_name = DB.executeAll(f'show tables like "%%{trcode_table}%%"')[-1][0]
                        res = DB.executeAll(f'select c_shcode from {table_name}')
                        shcodes_added = [element[0] for element in res]
                        shcodes.extend(shcodes_added)
                    del DB
                elif option_shcode == '20210715':
                    data = pd.read_csv("E:\projects\pykrx_20210622\data\derivatives.csv")
                    shcodes = data['ISU_SRT_CD'].tolist()
                else:
                    print('Err: unknown shcode option | {}'.format(option_shcode))

                # InBlock 세팅 except shcode
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['sdate'] = sdate
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['edate'] = edate

                # InBlock 세팅 except shcode
                # DO IT
                for shcode in shcodes:
                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['shcode'] = shcode
                    xing_element['occur']['criteria'] = 'shcode'
                    xing_element['occur']['value'] = shcode

                    xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                    xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mother_out.put('finished')
                print('t8414:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print('t8414:\tlistened from mother END')
                print('t8414:\tend process')
                break
            else:
                print('t8414:\tlistened from mother odd flag | {}'.format(listened_flag))
                print('t8414:\tend process')
                break

    elif trcode == 't8419':
        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':

                option_gubun = xing_element['options']['gubun']

                gubun = 0

                # InBlock 에 들어갈 변수 추출

                if option_gubun == 'day':
                    gubun = '2'
                elif option_gubun == 'week':
                    gubun = '3'
                elif option_gubun == 'month':
                    gubun = '4'
                else:
                    print('Err: unknown ncnt option | {}'.format(option_gubun))

                # InBlock 세팅 except shcode
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun'] = gubun
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['qrycnt'] = ncnt
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['sdate'] = sdate
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['edate'] = edate

                # InBlock 세팅 except shcode
                # DO IT
                for upcode in upcodes:
                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['shcode'] = upcode
                    xing_element['occur']['value'] = upcode

                    xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                    xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mother_out.put('finished')
                print('t8411:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print('t8411:\tlistened from mother END')
                print('t8411:\tend process')
                break
            else:
                print('t8411:\tlistened from mother odd flag | {}'.format(listened_flag))
                print('t8411:\tend process')
                break

    elif trcode == 't8430':

        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':
                print('t8430:\tlistened START')
                print('t8430:\tstart process')

                option_gubun = xing_element['options']['gubun_t8430']

                gubun = ''

                # InBlock 에 들어갈 변수 추출
                if option_gubun == 'all':
                    gubun = '0'
                elif option_gubun == 'kospi':
                    gubun = '1'
                elif option_gubun == 'kosdaq':
                    gubun = '2'
                else:
                    print('Err: unknown gubun option | {}'.format(option_gubun))

                # InBlock 세팅
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun'] = gubun

                xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mysql_out.put(xing_element)
                print('t8430:\tput to child_mysql DONE')

                queue_mother_out.put('finished')
                print('t8430:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print('t8430:\tlistened from mother END')
                print('t8430:\tend process')
                break
            else:
                print('t8430:\tlistened from mother odd flag | {}'.format(listened_flag))
                print('t8430:\tend process')
                break

    elif trcode == 't8432':

        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':
                print('t8432:\tlistened START')
                print('t8432:\tstart process')

                option_gubun = xing_element['options']['gubun_t8432']

                gubuns = []

                # InBlock 에 들어갈 변수 추출
                if option_gubun == 'all':
                    gubuns = ['V', 'S', '']
                else:
                    print('Err: unknown ncnt option | {}'.format(option_gubun))

                # InBlock 세팅
                xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')
                xing_element['occur']['value1'] = dt.today().strftime('%Y%m%d')

                for gubun in gubuns:

                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun'] = gubun

                    xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mysql_out.put(xing_element)
                print('t8432:\tput to child_mysql DONE')

                queue_mother_out.put('finished')
                print('t8432:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print('t8432:\tlistened from mother END')
                print('t8432:\tend process')
                break
            else:
                print('t8432:\tlistened from mother odd flag | {}'.format(listened_flag))
                print('t8432:\tend process')
                break

    elif trcode == 't8433':

        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':
                print('t8433:\tlistened START')
                print('t8433:\tstart process')


                # InBlock 에 들어갈 변수 추출
                # InBlock 세팅
                xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')
                xing_element['occur']['value1'] = dt.today().strftime('%Y%m%d')

                xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mysql_out.put(xing_element)
                print('t8433:\tput to child_mysql DONE')

                queue_mother_out.put('finished')
                print('t8433:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print('t8433:\tlistened from mother END')
                print('t8433:\tend process')
                break
            else:
                print('t8433:\tlistened from mother odd flag | {}'.format(listened_flag))
                print('t8433:\tend process')
                break

    elif trcode == 't8435':

        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':
                print('t8435:\tlistened START')
                print('t8435:\tstart process')

                option_gubun = xing_element['options']['gubun_t8435']

                gubuns = []

                # InBlock 에 들어갈 변수 추출
                if option_gubun == 'all':
                    gubuns = ['MF', 'MO', 'WK', 'SF']
                else:
                    print('Err: unknown ncnt option | {}'.format(option_gubun))

                # InBlock 세팅
                xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')
                xing_element['occur']['value1'] = dt.today().strftime('%Y%m%d')

                for gubun in gubuns:

                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun'] = gubun

                    xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mysql_out.put(xing_element)
                print('t8435:\tput to child_mysql DONE')

                queue_mother_out.put('finished')
                print('t8435:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print('t8435:\tlistened from mother END')
                print('t8435:\tend process')
                break
            else:
                print('t8435:\tlistened from mother odd flag | {}'.format(listened_flag))
                print('t8435:\tend process')
                break

    elif trcode == 't8436':

        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':
                print(f'{trcode}:\tlistened START')
                print(f'{trcode}:\tstart process')

                option_gubun = xing_element['options']['gubun_t8436']

                gubun = ''

                # InBlock 에 들어갈 변수 추출
                if option_gubun == 'all':
                    gubun = '0'
                elif option_gubun == 'kospi':
                    gubun = '1'
                elif option_gubun == 'kosdaq':
                    gubun = '2'
                else:
                    print(f'Err: unknown gubun option | {option_gubun}')

                # InBlock 세팅
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun'] = gubun

                xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mother_out.put('finished')
                print(f'{trcode}:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print(f'{trcode}:\tlistened from mother END')
                print(f'{trcode}:\tend process')
                break
            else:
                print(f'{trcode}:\tlistened from mother odd flag | {listened_flag}')
                print(f'{trcode}:\tend process')
                break

    elif trcode == 't1533':

        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':
                print('t1533:\tlistened START')
                print('t1533:\tstart process')

                gubun = ''

                # InBlock 에 들어갈 변수 추출

                # InBlock 세팅

                xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mysql_out.put(xing_element)
                print('t1533:\tput to child_mysql DONE')

                queue_mother_out.put('finished')
                print('t1533:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print('t1533:\tlistened from mother END')
                print('t1533:\tend process')
                break
            else:
                print('t1533:\tlistened from mother odd flag | {}'.format(listened_flag))
                print('t1533:\tend process')
                break

    elif trcode == 't1537':

        while 1:
            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':
                print('t1537:\tlistened START')
                print('t1537:\tstart process')

                option_tmcode = xing_element['options']['tmcode']

                tmcodes = ''

                # InBlock 에 들어갈 변수 추출
                if option_tmcode == 'all':
                    DB = database_handler.Database()
                    res = DB.executeAll('select c_tmcode from xing_t1533_outblock1_20210509_115612;')
                    tmcodes = [element[0] for element in res]
                else:
                    print('Err: unknown gubun option | {}'.format(option_tmcode))

                # InBlock 세팅
                # xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun'] = gubun

                # InBlock 세팅 except shcode
                # DO IT
                for tmcode in tmcodes:
                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['tmcode'] = tmcode
                    xing_element['occur']['criteria'] = 'tmcode'
                    xing_element['occur']['value'] = tmcode

                    xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                    xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mysql_out.put(xing_element)
                print('t1537:\tput to child_mysql DONE')

                queue_mother_out.put('finished')
                print('t1537:\tput to mother FINISHED')

            elif listened_flag == 'end':
                print('t1537:\tlistened from mother END')
                print('t1537:\tend process')
                break
            else:
                print('t1537:\tlistened from mother odd flag | {}'.format(listened_flag))
                print('t1537:\tend process')
                break

    elif trcode == 't8424':

        while 1:

            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':

                print('t8424:\tlistened START')

                print('t8424:\tstart process')

                gubun = ''

                # InBlock 에 들어갈 변수 추출

                # InBlock 세팅

                xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mysql_out.put(xing_element)

                print('t8424:\tput to child_mysql DONE')

                queue_mother_out.put('finished')

                print('t8424:\tput to mother FINISHED')


            elif listened_flag == 'end':

                print('t8424:\tlistened from mother END')

                print('t8424:\tend process')

                break

            else:

                print('t8424:\tlistened from mother odd flag | {}'.format(listened_flag))

                print('t8424:\tend process')

                break

    elif trcode == 't1516':

        while 1:

            listened_flag = queue_mother_in.get()

            if listened_flag == 'start':

                print('t1516:\tlistened START')

                print('t1516:\tstart process')

                option_upcode = xing_element['options']['upcode']

                upcodes = ''

                # InBlock 에 들어갈 변수 추출

                if option_upcode == 'all':

                    DB = database_handler.Database()

                    res = DB.executeAll('select c_upcode from xing_t8424_outblock_20210509_121421;')

                    upcodes = [element[0] for element in res]

                else:

                    print('Err: unknown gubun option | {}'.format(option_upcode))

                # InBlock 세팅

                # xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun'] = gubun

                # InBlock 세팅 except shcode

                # DO IT

                for upcode in upcodes:
                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['upcode'] = upcode

                    xing_element['occur']['criteria'] = 'upcode'

                    xing_element['occur']['value'] = upcode

                    xing_element['date_time']['element_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['shcode'] = ''

                    xing.XQuery(xing_name, trcode, queue_mysql_out, True).request(xing_element)

                queue_mysql_out.put(xing_element)

                print('t1516:\tput to child_mysql DONE')

                queue_mother_out.put('finished')

                print('t1516:\tput to mother FINISHED')


            elif listened_flag == 'end':

                print('t1516:\tlistened from mother END')

                print('t1516:\tend process')

                break

            else:

                print('t1516:\tlistened from mother odd flag | {}'.format(listened_flag))

                print('t1516:\tend process')

                break
    
    return 1


def child_mysql(queue_mysql_out, queue_mysql_in):
    print('child_mysql run')

    MH = mysql_handler.MysqlHandler()

    cnt = 0

    while 1:

        xing_element = queue_mysql_out.get()

        if not xing_element['phase'] == 'done':

            phase = xing_element['phase']

            if phase == 'listened':
                returned_message = MH.update_mysql(xing_element)
                # cnt test
                """
                cnt += 1
                print('main_light_cnt {}'.format(cnt))
                """

                xing_element['date_time']['occur_end'] = dt.today().strftime('%Y%m%d %H%M%S.%f')
                xing_element['date_time']['elapse_time'] = (
                            dt.strptime(xing_element['date_time']['occur_end'], '%Y%m%d %H%M%S.%f') -
                            dt.strptime(xing_element['date_time']['occur_start'], '%Y%m%d %H%M%S.%f')).total_seconds()
                xing_element['status']['final'] += '|' + returned_message
                xing_element['phase'] = 'mysql_finished'

                if not xing_element['type'] == 'real':
                    MH.update_results(xing_element)
                if xing_element['type'] == 'real':
                    if xing_element['status']['final'].find('DB.commit (') > -1:
                        MH.update_results(xing_element)

        elif xing_element['phase'] == 'done':
            print('done received')

            returned_message = MH.update_mysql(xing_element)

            xing_element['date_time']['occur_end'] = dt.today().strftime('%Y%m%d %H%M%S.%f')
            xing_element['date_time']['elapse_time'] = (
                    dt.strptime(xing_element['date_time']['occur_end'], '%Y%m%d %H%M%S.%f') -
                    dt.strptime(xing_element['date_time']['occur_start'], '%Y%m%d %H%M%S.%f')).total_seconds()
            xing_element['status']['final'] += '|' + returned_message
            xing_element['phase'] = 'mysql_finished'

            if not xing_element['type'] == 'real':
                MH.update_results(xing_element)
            if xing_element['type'] == 'real':
                if xing_element['status']['final'].find('DB.commit (') > -1:
                    MH.update_results(xing_element)


def mother(queues, processes_pid, mode):
    print('mother run')
    config_dict = get_config()
    print(mode)
    xing_elements_type = config_dict['main'][mode]['xing_elements_type']

    if xing_elements_type == 'test':

        # queues['real']['JIF']['in'].put('start')
        # queues['real']['BMT']['in'].put('start')
        
        if 't8436' in queues['tr']:
            queues['tr']['t8436']['in'].put('start')
            print('mother:\tsend to t8436 START')

            while 1:
                listened_flag = queues['tr']['t8436']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t8436 FINISHED')
                    queues['tr']['t8436']['in'].put('end')
                    print('mother:\tsend to t8436 END')
                    break
                else:
                    print('mother:\tlistened from t8436 odd flag | {}'.format(listened_flag))
                    break

            # t8436 수집 내역 업데이트
            print('mother:\tUPDATE t8436 by TRU')
            TRU = tr_updater.TRUpdater()
            TRU.main('t8436')
            print('mother:\tFINISHED to update t8436')
            
        if 't8411' in queues['tr']:
            queues['tr']['t8411']['in'].put('start')
            print('mother:\tsend to t8411 START')

            while 1:
                listened_flag = queues['tr']['t8411']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t8411 FINISHED')
                    queues['tr']['t8411']['in'].put('end')
                    print('mother:\tsend to t8411 END')
                    break
                else:
                    print('mother:\tlistened from t8411 odd flag | {}'.format(listened_flag))
                    break

            # t8411 수집 내역 업데이트
            print('mother:\tUPDATE t8411 by TRU')
            TRU = tr_updater.TRUpdater()
            TRU.main('t8411')
            print('mother:\tFINISHED to update t8411')

        count = 0
        while count <= 10:
            print(f'All process gonna die after {10 - count} seconds...')
            count += 1
            time.sleep(1)

        for process_group in processes_pid.keys():
            print(process_group)

            if process_group == 'children':

                for process_tr_real in processes_pid[process_group].keys():

                    if process_tr_real == 'tr':

                        for process_tr in processes_pid[process_group][process_tr_real].keys():

                            pid_tr = processes_pid[process_group][process_tr_real][process_tr]

                            if psutil.pid_exists(pid_tr):

                                os.kill(pid_tr, signal.SIGTERM)

                                print(f'KILL tr process {pid_tr}')

                            else:
                                print(f'TR process {pid_tr} already dead')

            elif process_group == 'mysql':

                for process_mysql in processes_pid[process_group].keys():

                    pid_mysql = processes_pid[process_group][process_mysql]

                    if psutil.pid_exists(pid_mysql):

                        os.kill(pid_mysql, signal.SIGTERM)

                        print(f'KILL mysql process {pid_mysql}')

                    else:

                        print(f'TR process {pid_mysql} already dead')

    elif xing_elements_type == 'update_t8412_t1305':

        while dt.today().time() < datetime.time(7, 0) or dt.today().time() > datetime.time(7, 30):
            time.sleep(10)

        exit()

    elif xing_elements_type == 'day_by_day_tr':

        # 종목 리스트 수집
        # t8436 으로 종목 data 수집
        if 't8436' in queues['tr']:
            queues['tr']['t8436']['in'].put('start')
            print('mother:\tsend to t8436 START')

            while 1:
                listened_flag = queues['tr']['t8436']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t8436 FINISHED')
                    queues['tr']['t8436']['in'].put('end')
                    print('mother:\tsend to t8436 END')
                    break
                else:
                    print('mother:\tlistened from t8436 odd flag | {}'.format(listened_flag))
                    break

            # t8436 수집 내역 업데이트
            print('mother:\tUPDATE t8436 by TRU')
            TRU = tr_updater.TRUpdater()
            TRU.main('t8436')
            print('mother:\tFINISHED to update t8436')

            # t8436 mysql 종료
            # pid_mysql = processes_pid['mysql']['t8436']
            # os.kill(pid_mysql, signal.SIGTERM)

        # 종목별 증거금율조회 수집
        # t1411 으로 종목 data 수집
        if 't1411' in queues['tr']:
            queues['tr']['t1411']['in'].put('start')
            print('mother:\tsend to t1411 START')

            while 1:
                listened_flag = queues['tr']['t1411']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1411 FINISHED')
                    queues['tr']['t1411']['in'].put('end')
                    print('mother:\tsend to t1411 END')
                    break
                else:
                    print(f'mother:\tlistened from t1411 odd flag | {listened_flag}')
                    break

        # 초저유동성조회 수집
        # t1410 으로 종목 data 수집
        if 't1410' in queues['tr']:
            queues['tr']['t1410']['in'].put('start')
            print('mother:\tsend to t1410 START')

            while 1:
                listened_flag = queues['tr']['t1410']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1410 FINISHED')
                    queues['tr']['t1410']['in'].put('end')
                    print('mother:\tsend to t1410 END')
                    break
                else:
                    print(f'mother:\tlistened from t1410 odd flag | {listened_flag}')
                    break

        # 투자경고/매매정지/정리매매조회 수집
        # t1405 으로 종목 data 수집
        if 't1405' in queues['tr']:
            queues['tr']['t1405']['in'].put('start')
            print('mother:\tsend to t1405 START')

            while 1:
                listened_flag = queues['tr']['t1405']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1405 FINISHED')
                    queues['tr']['t1405']['in'].put('end')
                    print('mother:\tsend to t1405 END')
                    break
                else:
                    print(f'mother:\tlistened from t1405 odd flag | {listened_flag}')
                    break

        # 관리/불성실/투자유의조회 수집
        # t1404 으로 종목 data 수집
        if 't1404' in queues['tr']:
            queues['tr']['t1404']['in'].put('start')
            print('mother:\tsend to t1404 START')

            while 1:
                listened_flag = queues['tr']['t1404']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1404 FINISHED')
                    queues['tr']['t1404']['in'].put('end')
                    print('mother:\tsend to t1404 END')
                    break
                else:
                    print(f'mother:\tlistened from t1404 odd flag | {listened_flag}')
                    break

        # 신규상장종목조회 수집
        # t1403 으로 종목 data 수집
        if 't1403' in queues['tr']:
            queues['tr']['t1403']['in'].put('start')
            print('mother:\tsend to t1403 START')

            while 1:
                listened_flag = queues['tr']['t1403']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1403 FINISHED')
                    queues['tr']['t1403']['in'].put('end')
                    print('mother:\tsend to t1403 END')
                    break
                else:
                    print(f'mother:\tlistened from t1403 odd flag | {listened_flag}')
                    break

        # 주식분별주가조회 수집
        # t1302 으로 종목 data 수집
        if 't1302' in queues['tr']:
            queues['tr']['t1302']['in'].put('start')
            print('mother:\tsend to t1302 START')

            while 1:
                listened_flag = queues['tr']['t1302']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1302 FINISHED')
                    queues['tr']['t1302']['in'].put('end')
                    print('mother:\tsend to t1302 END')
                    break
                else:
                    print(f'mother:\tlistened from t1302 odd flag | {listened_flag}')
                    break

        # 기간별주가 (일봉) 수집
        # t1305 으로 종목 data 수집
        if 't1305' in queues['tr']:
            queues['tr']['t1305']['in'].put('start')
            print('mother:\tsend to t1305 START')

            while 1:
                listened_flag = queues['tr']['t1305']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1305 FINISHED')
                    queues['tr']['t1305']['in'].put('end')
                    print('mother:\tsend to t1305 END')
                    break
                else:
                    print(f'mother:\tlistened from t1305 odd flag | {listened_flag}')
                    break

        # 주식챠트(일주월) (일봉) 수집
        # t8413 으로 종목 data 수집
        if 't8413' in queues['tr']:
            queues['tr']['t8413']['in'].put('start')
            print('mother:\tsend to t8413 START')

            while 1:
                listened_flag = queues['tr']['t8413']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t8413 FINISHED')
                    queues['tr']['t8413']['in'].put('end')
                    print('mother:\tsend to t8413 END')
                    break
                else:
                    print(f'mother:\tlistened from t8413 odd flag | {listened_flag}')
                    break

        # 신용거래동향 수집
        # t1921 으로 종목 data 수집
        if 't1921' in queues['tr']:
            queues['tr']['t1921']['in'].put('start')
            print('mother:\tsend to t1921 START')

            while 1:
                listened_flag = queues['tr']['t1921']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1921 FINISHED')
                    queues['tr']['t1921']['in'].put('end')
                    print('mother:\tsend to t1921 END')
                    break
                else:
                    print(f'mother:\tlistened from t1921 odd flag | {listened_flag}')
                    break

        # 종목별신용정보 수집
        # t1926 으로 종목 data 수집
        if 't1926' in queues['tr']:
            queues['tr']['t1926']['in'].put('start')
            print('mother:\tsend to t1926 START')

            while 1:
                listened_flag = queues['tr']['t1926']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1926 FINISHED')
                    queues['tr']['t1926']['in'].put('end')
                    print('mother:\tsend to t1926 END')
                    break
                else:
                    print(f'mother:\tlistened from t1926 odd flag | {listened_flag}')
                    break

        # 공매도일별추이 수집
        # t1927 으로 종목 data 수집
        if 't1927' in queues['tr']:
            queues['tr']['t1927']['in'].put('start')
            print('mother:\tsend to t1927 START')

            while 1:
                listened_flag = queues['tr']['t1927']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1927 FINISHED')
                    queues['tr']['t1927']['in'].put('end')
                    print('mother:\tsend to t1927 END')
                    break
                else:
                    print(f'mother:\tlistened from t1927 odd flag | {listened_flag}')
                    break

        # 종목별대차거래일간추이 수집
        # t1941 으로 종목 data 수집
        if 't1941' in queues['tr']:
            queues['tr']['t1941']['in'].put('start')
            print('mother:\tsend to t1941 START')

            while 1:
                listened_flag = queues['tr']['t1941']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1941 FINISHED')
                    queues['tr']['t1941']['in'].put('end')
                    print('mother:\tsend to t1941 END')
                    break
                else:
                    print(f'mother:\tlistened from t1941 odd flag | {listened_flag}')
                    break

        # 시간대별투자자매매추이 수집
        # t1602 으로 종목 data 수집
        if 't1602' in queues['tr']:
            queues['tr']['t1602']['in'].put('start')
            print('mother:\tsend to t1602 START')

            while 1:
                listened_flag = queues['tr']['t1602']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1602 FINISHED')
                    queues['tr']['t1602']['in'].put('end')
                    print('mother:\tsend to t1602 END')
                    break
                else:
                    print(f'mother:\tlistened from t1602 odd flag | {listened_flag}')
                    break

        # 기간별투자자매매추이 수집
        # t1665 으로 종목 data 수집
        if 't1665' in queues['tr']:
            queues['tr']['t1665']['in'].put('start')
            print('mother:\tsend to t1665 START')

            while 1:
                listened_flag = queues['tr']['t1665']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1665 FINISHED')
                    queues['tr']['t1665']['in'].put('end')
                    print('mother:\tsend to t1665 END')
                    break
                else:
                    print(f'mother:\tlistened from t1665 odd flag | {listened_flag}')
                    break

        # 시간대별프로그램매매추이(차트) 수집
        # t1662 으로 종목 data 수집
        if 't1662' in queues['tr']:
            queues['tr']['t1662']['in'].put('start')
            print('mother:\tsend to t1662 START')

            while 1:
                listened_flag = queues['tr']['t1662']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1662 FINISHED')
                    queues['tr']['t1662']['in'].put('end')
                    print('mother:\tsend to t1662 END')
                    break
                else:
                    print(f'mother:\tlistened from t1662 odd flag | {listened_flag}')
                    break

        # 기간별프로그램매매추이(차트) 수집
        # t1633 으로 종목 data 수집
        if 't1633' in queues['tr']:
            queues['tr']['t1633']['in'].put('start')
            print('mother:\tsend to t1633 START')

            while 1:
                listened_flag = queues['tr']['t1633']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1633 FINISHED')
                    queues['tr']['t1633']['in'].put('end')
                    print('mother:\tsend to t1633 END')
                    break
                else:
                    print(f'mother:\tlistened from t1633 odd flag | {listened_flag}')
                    break

        # 종목별프로그램매매동향 수집
        # t1636 으로 종목 data 수집
        if 't1636' in queues['tr']:
            queues['tr']['t1636']['in'].put('start')
            print('mother:\tsend to t1636 START')

            while 1:
                listened_flag = queues['tr']['t1636']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1636 FINISHED')
                    queues['tr']['t1636']['in'].put('end')
                    print('mother:\tsend to t1636 END')
                    break
                else:
                    print(f'mother:\tlistened from t1636 odd flag | {listened_flag}')
                    break

        # 종목별프로그램매매추이(차트) 수집
        # t1637 으로 종목 data 수집
        if 't1637' in queues['tr']:
            queues['tr']['t1637']['in'].put('start')
            print('mother:\tsend to t1637 START')

            while 1:
                listened_flag = queues['tr']['t1637']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1637 FINISHED')
                    queues['tr']['t1637']['in'].put('end')
                    print('mother:\tsend to t1637 END')
                    break
                else:
                    print(f'mother:\tlistened from t1637 odd flag | {listened_flag}')
                    break

        # 외인기관종목별동향 수집
        # t1717 으로 종목 data 수집
        if 't1717' in queues['tr']:
            queues['tr']['t1717']['in'].put('start')
            print('mother:\tsend to t1717 START')

            while 1:
                listened_flag = queues['tr']['t1717']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1717 FINISHED')
                    queues['tr']['t1717']['in'].put('end')
                    print('mother:\tsend to t1717 END')
                    break
                else:
                    print(f'mother:\tlistened from t1717 odd flag | {listened_flag}')
                    break

        # 업종챠트 수집
        # t8419 으로 종목 data 수집
        if 't8419' in queues['tr']:
            queues['tr']['t8419']['in'].put('start')
            print('mother:\tsend to t8419 START')

            while 1:
                listened_flag = queues['tr']['t8419']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t8419 FINISHED')
                    queues['tr']['t8419']['in'].put('end')
                    print('mother:\tsend to t8419 END')
                    break
                else:
                    print(f'mother:\tlistened from t8419 odd flag | {listened_flag}')
                    break

        # t8411 로 종목 data 수집
        if 't8411' in queues['tr']:
            queues['tr']['t8411']['in'].put('start')
            print('mother:\tsend to t8411 START')
            while 1:
                listened_flag = queues['tr']['t8411']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t8411 FINISHED')
                    queues['tr']['t8411']['in'].put('end')
                    print('mother:\tsend to t8411 END')
                    break
                else:
                    print('mother:\tlistened from t8411 odd flag | {}'.format(listened_flag))
                    break

            # t8411 mysql 종료
            pid_mysql = processes_pid['mysql']['t8411']
            os.kill(pid_mysql, signal.SIGTERM)

        for pid in list(processes_pid['mysql'].values()):
            pass
            # os.kill(pid, signal.SIGTERM)

        print('kill mysqls')

        print('mother:\tall work done')

    elif xing_elements_type == 'day_by_day_basic':

        # 종목 리스트 수집
        # t8436 으로 종목 data 수집
        if 't8436' in queues['tr']:
            queues['tr']['t8436']['in'].put('start')
            print('mother:\tsend to t8436 START')

        # 관리/불성실/투자유의조회 수집
        # t1404 으로 종목 data 수집
        if 't1404' in queues['tr']:
            queues['tr']['t1404']['in'].put('start')
            print('mother:\tsend to t1404 START')

        # 투자경고/매매정지/정리매매조회 수집
        # t1405 으로 종목 data 수집
        if 't1405' in queues['tr']:
            queues['tr']['t1405']['in'].put('start')
            print('mother:\tsend to t1405 START')

        # 초저유동성조회 수집
        # t1410 으로 종목 data 수집
        if 't1410' in queues['tr']:
            queues['tr']['t1410']['in'].put('start')
            print('mother:\tsend to t1410 START')

        # 종목별 증거금율조회 수집
        # t1411 으로 종목 data 수집
        if 't1411' in queues['tr']:
            queues['tr']['t1411']['in'].put('start')
            print('mother:\tsend to t1411 START')

        # child 로부터 완료 내역 들을 때까지 대기.
        if 't8436' in queues['tr']:
            while 1:
                listened_flag = queues['tr']['t8436']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t8436 FINISHED')
                    queues['tr']['t8436']['in'].put('end')
                    print('mother:\tsend to t8436 END')
                    break
                else:
                    print('mother:\tlistened from t8436 odd flag | {}'.format(listened_flag))
                    break

        if 't1404' in queues['tr']:
            while 1:
                listened_flag = queues['tr']['t1404']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1404 FINISHED')
                    queues['tr']['t1404']['in'].put('end')
                    print('mother:\tsend to t1404 END')
                    break
                else:
                    print(f'mother:\tlistened from t1404 odd flag | {listened_flag}')
                    break

        if 't1405' in queues['tr']:
            while 1:
                listened_flag = queues['tr']['t1405']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1405 FINISHED')
                    queues['tr']['t1405']['in'].put('end')
                    print('mother:\tsend to t1405 END')
                    break
                else:
                    print(f'mother:\tlistened from t1405 odd flag | {listened_flag}')
                    break

        if 't1410' in queues['tr']:
            while 1:
                listened_flag = queues['tr']['t1410']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1410 FINISHED')
                    queues['tr']['t1410']['in'].put('end')
                    print('mother:\tsend to t1410 END')
                    break
                else:
                    print(f'mother:\tlistened from t1410 odd flag | {listened_flag}')
                    break

        if 't1411' in queues['tr']:
            while 1:
                listened_flag = queues['tr']['t1411']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1411 FINISHED')
                    queues['tr']['t1411']['in'].put('end')
                    print('mother:\tsend to t1411 END')
                    break
                else:
                    print(f'mother:\tlistened from t1411 odd flag | {listened_flag}')
                    break

        # cum table 업데이트
        # t8436 수집 내역 업데이트
        if 't8436' in queues['tr']:
            print('mother:\tUPDATE t8436 by TRU')
            TRU = tr_updater.TRUpdater()
            TRU.main('t8436')
            print('mother:\tFINISHED to update t8436')

            # t8436 mysql 종료
            # pid_mysql = processes_pid['mysql']['t8436']
            # os.kill(pid_mysql, signal.SIGTERM)

        for pid in list(processes_pid['mysql'].values()):
            pass
            # os.kill(pid, signal.SIGTERM)

        print('kill mysqls')

        print('mother:\tall work done')

    elif xing_elements_type == 'day_by_day_candle_chart':

        # 주식분별주가조회 수집
        # t1302 으로 종목 data 수집
        if 't1302' in queues['tr']:
            queues['tr']['t1302']['in'].put('start')
            print('mother:\tsend to t1302 START')

        # 기간별주가 (일봉) 수집
        # t1305 으로 종목 data 수집
        if 't1305' in queues['tr']:
            queues['tr']['t1305']['in'].put('start')
            print('mother:\tsend to t1305 START')

        # 주식챠트(일주월) (일봉) 수집
        # t8413 으로 종목 data 수집
        if 't8413' in queues['tr']:
            queues['tr']['t8413']['in'].put('start')
            print('mother:\tsend to t8413 START')

        if 't1302' in queues['tr']:
            while 1:
                listened_flag = queues['tr']['t1302']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1302 FINISHED')
                    queues['tr']['t1302']['in'].put('end')
                    print('mother:\tsend to t1302 END')
                    break
                else:
                    print(f'mother:\tlistened from t1302 odd flag | {listened_flag}')
                    break

        if 't1305' in queues['tr']:
            while 1:
                listened_flag = queues['tr']['t1305']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1305 FINISHED')
                    queues['tr']['t1305']['in'].put('end')
                    print('mother:\tsend to t1305 END')
                    break
                else:
                    print(f'mother:\tlistened from t1305 odd flag | {listened_flag}')
                    break

        if 't8413' in queues['tr']:
            while 1:
                listened_flag = queues['tr']['t8413']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t8413 FINISHED')
                    queues['tr']['t8413']['in'].put('end')
                    print('mother:\tsend to t8413 END')
                    break
                else:
                    print(f'mother:\tlistened from t8413 odd flag | {listened_flag}')
                    break

        for pid in list(processes_pid['mysql'].values()):
            pass
            # os.kill(pid, signal.SIGTERM)

        print('kill mysqls')

        print('mother:\tall work done')

    elif xing_elements_type == 'day_by_day_basic_backup':

        # 종목 리스트 수집
        # t8436 으로 종목 data 수집
        if 't8436' in queues['tr']:
            queues['tr']['t8436']['in'].put('start')
            print('mother:\tsend to t8436 START')

            while 1:
                listened_flag = queues['tr']['t8436']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t8436 FINISHED')
                    queues['tr']['t8436']['in'].put('end')
                    print('mother:\tsend to t8436 END')
                    break
                else:
                    print('mother:\tlistened from t8436 odd flag | {}'.format(listened_flag))
                    break

            # t8436 수집 내역 업데이트
            print('mother:\tUPDATE t8436 by TRU')
            TRU = tr_updater.TRUpdater()
            TRU.main('t8436')
            print('mother:\tFINISHED to update t8436')

            # t8436 mysql 종료
            # pid_mysql = processes_pid['mysql']['t8436']
            # os.kill(pid_mysql, signal.SIGTERM)

        # 신규상장종목조회 수집
        # t1403 으로 종목 data 수집
        if 't1403' in queues['tr']:
            queues['tr']['t1403']['in'].put('start')
            print('mother:\tsend to t1403 START')

            while 1:
                listened_flag = queues['tr']['t1403']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1403 FINISHED')
                    queues['tr']['t1403']['in'].put('end')
                    print('mother:\tsend to t1403 END')
                    break
                else:
                    print(f'mother:\tlistened from t1403 odd flag | {listened_flag}')
                    break

        # 관리/불성실/투자유의조회 수집
        # t1404 으로 종목 data 수집
        if 't1404' in queues['tr']:
            queues['tr']['t1404']['in'].put('start')
            print('mother:\tsend to t1404 START')

            while 1:
                listened_flag = queues['tr']['t1404']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1404 FINISHED')
                    queues['tr']['t1404']['in'].put('end')
                    print('mother:\tsend to t1404 END')
                    break
                else:
                    print(f'mother:\tlistened from t1404 odd flag | {listened_flag}')
                    break

        # 투자경고/매매정지/정리매매조회 수집
        # t1405 으로 종목 data 수집
        if 't1405' in queues['tr']:
            queues['tr']['t1405']['in'].put('start')
            print('mother:\tsend to t1405 START')

            while 1:
                listened_flag = queues['tr']['t1405']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1405 FINISHED')
                    queues['tr']['t1405']['in'].put('end')
                    print('mother:\tsend to t1405 END')
                    break
                else:
                    print(f'mother:\tlistened from t1405 odd flag | {listened_flag}')
                    break

        # 초저유동성조회 수집
        # t1410 으로 종목 data 수집
        if 't1410' in queues['tr']:
            queues['tr']['t1410']['in'].put('start')
            print('mother:\tsend to t1410 START')

            while 1:
                listened_flag = queues['tr']['t1410']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1410 FINISHED')
                    queues['tr']['t1410']['in'].put('end')
                    print('mother:\tsend to t1410 END')
                    break
                else:
                    print(f'mother:\tlistened from t1410 odd flag | {listened_flag}')
                    break

        # 종목별 증거금율조회 수집
        # t1411 으로 종목 data 수집
        if 't1411' in queues['tr']:
            queues['tr']['t1411']['in'].put('start')
            print('mother:\tsend to t1411 START')

            while 1:
                listened_flag = queues['tr']['t1411']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1411 FINISHED')
                    queues['tr']['t1411']['in'].put('end')
                    print('mother:\tsend to t1411 END')
                    break
                else:
                    print(f'mother:\tlistened from t1411 odd flag | {listened_flag}')
                    break

        # 종목별신용정보 수집
        # t1926 으로 종목 data 수집
        if 't1926' in queues['tr']:
            queues['tr']['t1926']['in'].put('start')
            print('mother:\tsend to t1926 START')

            while 1:
                listened_flag = queues['tr']['t1926']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1926 FINISHED')
                    queues['tr']['t1926']['in'].put('end')
                    print('mother:\tsend to t1926 END')
                    break
                else:
                    print(f'mother:\tlistened from t1926 odd flag | {listened_flag}')
                    break

        # 공매도일별추이 수집
        # t1927 으로 종목 data 수집
        if 't1927' in queues['tr']:
            queues['tr']['t1927']['in'].put('start')
            print('mother:\tsend to t1927 START')

            while 1:
                listened_flag = queues['tr']['t1927']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1927 FINISHED')
                    queues['tr']['t1927']['in'].put('end')
                    print('mother:\tsend to t1927 END')
                    break
                else:
                    print(f'mother:\tlistened from t1927 odd flag | {listened_flag}')
                    break

        # 종목별대차거래일간추이 수집
        # t1941 으로 종목 data 수집
        if 't1941' in queues['tr']:
            queues['tr']['t1941']['in'].put('start')
            print('mother:\tsend to t1941 START')

            while 1:
                listened_flag = queues['tr']['t1941']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1941 FINISHED')
                    queues['tr']['t1941']['in'].put('end')
                    print('mother:\tsend to t1941 END')
                    break
                else:
                    print(f'mother:\tlistened from t1941 odd flag | {listened_flag}')
                    break

        for pid in list(processes_pid['mysql'].values()):
            pass
            # os.kill(pid, signal.SIGTERM)

        print('kill mysqls')

        print('mother:\tall work done')

    elif xing_elements_type == 'day_by_day_yeche_real':

        # 종목 리스트 수집
        # t8436 으로 종목 data 수집
        if 't8436' in queues['tr']:
            queues['tr']['t8436']['in'].put('start')
            print('mother:\tsend to t8436 START')

        # child 로부터 완료 내역 들을 때까지 대기.
        if 't8436' in queues['tr']:
            while 1:
                listened_flag = queues['tr']['t8436']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t8436 FINISHED')
                    queues['tr']['t8436']['in'].put('end')
                    print('mother:\tsend to t8436 END')
                    break
                else:
                    print('mother:\tlistened from t8436 odd flag | {}'.format(listened_flag))
                    break

        while dt.today().time() < datetime.time(8, 0):
            time.sleep(10)

        # S3_kospi, H1_kospi, S3_kosdaq, H1_kosdaq,
        if 'YS3_kospi' in queues['real']:
            queues['real']['YS3_kospi']['in'].put('start')
        if 'YK3_kosdaq' in queues['real']:
            queues['real']['YK3_kosdaq']['in'].put('start')

        while dt.today().time() < datetime.time(10, 0):
            time.sleep(10)

        if 'YS3_kospi' in processes_pid['children']['real']:
            pid_YS3_kospi = processes_pid['children']['real']['YS3_kospi']
            os.kill(pid_YS3_kospi, signal.SIGTERM)
            print('kill YS3_kospi')
        if 'YK3_kosdaq' in processes_pid['children']['real']:
            pid_YK3_kosdaq = processes_pid['children']['real']['YK3_kosdaq']
            os.kill(pid_YK3_kosdaq, signal.SIGTERM)
            print('kill YK3_kosdaq')

        print('kill the children')

        for pid in list(processes_pid['mysql'].values()):
            os.kill(pid, signal.SIGTERM)
        print('kill mysqls')

        print('mother:\tall work done')

    elif xing_elements_type == 'derivatives':

        # 지수선물 종목 리스트 수집
        # t8432 으로 종목 data 수집
        if 't8432' in queues['tr']:
            queues['tr']['t8432']['in'].put('start')
            print('mother:\tsend to t8432 START')

        # child 로부터 완료 내역 들을 때까지 대기.
        if 't8432' in queues['tr']:
            while 1:
                listened_flag = queues['tr']['t8432']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t8436 FINISHED')
                    queues['tr']['t8432']['in'].put('end')
                    print('mother:\tsend to t8432 END')
                    break
                else:
                    print('mother:\tlistened from t8432 odd flag | {}'.format(listened_flag))
                    break

        # 지수옵션 종목 리스트 수집
        # t8433 으로 종목 data 수집
        if 't8433' in queues['tr']:
            queues['tr']['t8433']['in'].put('start')
            print('mother:\tsend to t8433 START')

        # child 로부터 완료 내역 들을 때까지 대기.
        if 't8433' in queues['tr']:
            while 1:
                listened_flag = queues['tr']['t8433']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t8436 FINISHED')
                    queues['tr']['t8433']['in'].put('end')
                    print('mother:\tsend to t8433 END')
                    break
                else:
                    print('mother:\tlistened from t8433 odd flag | {}'.format(listened_flag))
                    break

        # 기타파생 종목 리스트 수집
        # t8435 으로 종목 data 수집
        if 't8435' in queues['tr']:
            queues['tr']['t8435']['in'].put('start')
            print('mother:\tsend to t8435 START')

        # child 로부터 완료 내역 들을 때까지 대기.
        if 't8435' in queues['tr']:
            while 1:
                listened_flag = queues['tr']['t8435']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t8436 FINISHED')
                    queues['tr']['t8435']['in'].put('end')
                    print('mother:\tsend to t8435 END')
                    break
                else:
                    print('mother:\tlistened from t8435 odd flag | {}'.format(listened_flag))
                    break

        # 파생상품 틱 데이터 수집
        # t8414 으로 종목 data 수집
        if 't8414' in queues['tr']:
            queues['tr']['t8414']['in'].put('start')
            print('mother:\tsend to t8414 START')

        # child 로부터 완료 내역 들을 때까지 대기.
        if 't8414' in queues['tr']:
            while 1:
                listened_flag = queues['tr']['t8414']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t8414 FINISHED')
                    queues['tr']['t8414']['in'].put('end')
                    print('mother:\tsend to t8414 END')
                    break
                else:
                    print('mother:\tlistened from t8414 odd flag | {}'.format(listened_flag))
                    break

        while dt.today().time() < datetime.time(8, 0):
            time.sleep(10)

        print('mother:\tall work done')

    elif xing_elements_type == 'update_cum_data':

        # 종목 리스트 수집
        # t8436 으로 종목 data 수집
        if 't8436' in queues['tr']:
            queues['tr']['t8436']['in'].put('start')
            print('mother:\tsend to t8436 START')

            while 1:
                listened_flag = queues['tr']['t8436']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t8436 FINISHED')
                    queues['tr']['t8436']['in'].put('end')
                    print('mother:\tsend to t8436 END')
                    break
                else:
                    print('mother:\tlistened from t8436 odd flag | {}'.format(listened_flag))
                    break

            # t8436 수집 내역 업데이트
            print('mother:\tUPDATE t8436 by TRU')
            TRU = tr_updater.TRUpdater()
            TRU.main('t8436')
            print('mother:\tFINISHED to update t8436')

            # t8436 mysql 종료
            # pid_mysql = processes_pid['mysql']['t8436']
            # os.kill(pid_mysql, signal.SIGTERM)

        # 신규상장종목조회 수집
        # t1403 으로 종목 data 수집
        if 't1403' in queues['tr']:
            queues['tr']['t1403']['in'].put('start')
            print('mother:\tsend to t1403 START')

            while 1:
                listened_flag = queues['tr']['t1403']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1403 FINISHED')
                    queues['tr']['t1403']['in'].put('end')
                    print('mother:\tsend to t1403 END')
                    break
                else:
                    print(f'mother:\tlistened from t1403 odd flag | {listened_flag}')
                    break

        # 관리/불성실/투자유의조회 수집
        # t1404 으로 종목 data 수집
        if 't1404' in queues['tr']:
            queues['tr']['t1404']['in'].put('start')
            print('mother:\tsend to t1404 START')

            while 1:
                listened_flag = queues['tr']['t1404']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1404 FINISHED')
                    queues['tr']['t1404']['in'].put('end')
                    print('mother:\tsend to t1404 END')
                    break
                else:
                    print(f'mother:\tlistened from t1404 odd flag | {listened_flag}')
                    break

        # 투자경고/매매정지/정리매매조회 수집
        # t1405 으로 종목 data 수집
        if 't1405' in queues['tr']:
            queues['tr']['t1405']['in'].put('start')
            print('mother:\tsend to t1405 START')

            while 1:
                listened_flag = queues['tr']['t1405']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1405 FINISHED')
                    queues['tr']['t1405']['in'].put('end')
                    print('mother:\tsend to t1405 END')
                    break
                else:
                    print(f'mother:\tlistened from t1405 odd flag | {listened_flag}')
                    break

        # 초저유동성조회 수집
        # t1410 으로 종목 data 수집
        if 't1410' in queues['tr']:
            queues['tr']['t1410']['in'].put('start')
            print('mother:\tsend to t1410 START')

            while 1:
                listened_flag = queues['tr']['t1410']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1410 FINISHED')
                    queues['tr']['t1410']['in'].put('end')
                    print('mother:\tsend to t1410 END')
                    break
                else:
                    print(f'mother:\tlistened from t1410 odd flag | {listened_flag}')
                    break

        # 종목별 증거금율조회 수집
        # t1411 으로 종목 data 수집
        if 't1411' in queues['tr']:
            queues['tr']['t1411']['in'].put('start')
            print('mother:\tsend to t1411 START')

            while 1:
                listened_flag = queues['tr']['t1411']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1411 FINISHED')
                    queues['tr']['t1411']['in'].put('end')
                    print('mother:\tsend to t1411 END')
                    break
                else:
                    print(f'mother:\tlistened from t1411 odd flag | {listened_flag}')
                    break

        # 종목별신용정보 수집
        # t1926 으로 종목 data 수집
        if 't1926' in queues['tr']:
            queues['tr']['t1926']['in'].put('start')
            print('mother:\tsend to t1926 START')

            while 1:
                listened_flag = queues['tr']['t1926']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1926 FINISHED')
                    queues['tr']['t1926']['in'].put('end')
                    print('mother:\tsend to t1926 END')
                    break
                else:
                    print(f'mother:\tlistened from t1926 odd flag | {listened_flag}')
                    break

        # 공매도일별추이 수집
        # t1927 으로 종목 data 수집
        if 't1927' in queues['tr']:
            queues['tr']['t1927']['in'].put('start')
            print('mother:\tsend to t1927 START')

            while 1:
                listened_flag = queues['tr']['t1927']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1927 FINISHED')
                    queues['tr']['t1927']['in'].put('end')
                    print('mother:\tsend to t1927 END')
                    break
                else:
                    print(f'mother:\tlistened from t1927 odd flag | {listened_flag}')
                    break

        # 종목별대차거래일간추이 수집
        # t1941 으로 종목 data 수집
        if 't1941' in queues['tr']:
            queues['tr']['t1941']['in'].put('start')
            print('mother:\tsend to t1941 START')

            while 1:
                listened_flag = queues['tr']['t1941']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1941 FINISHED')
                    queues['tr']['t1941']['in'].put('end')
                    print('mother:\tsend to t1941 END')
                    break
                else:
                    print(f'mother:\tlistened from t1941 odd flag | {listened_flag}')
                    break

        for pid in list(processes_pid['mysql'].values()):
            pass
            # os.kill(pid, signal.SIGTERM)

        print('kill mysqls')

        print('mother:\tall work done')

    elif xing_elements_type == 'update_t1302':

        # 주식분별주가조회 수집
        # t1302 으로 종목 data 수집
        if 't1302' in queues['tr']:
            queues['tr']['t1302']['in'].put('start')
            print('mother:\tsend to t1302 START')

            while 1:
                listened_flag = queues['tr']['t1302']['out'].get()
                if listened_flag == 'finished':
                    print('mother:\tlistened from t1302 FINISHED')
                    queues['tr']['t1302']['in'].put('end')
                    print('mother:\tsend to t1302 END')
                    break
                else:
                    print(f'mother:\tlistened from t1302 odd flag | {listened_flag}')
                    break

    elif xing_elements_type == 'update_t8411':
        # t8411 로 종목 data 수집
        queues['tr']['t8411']['in'].put('start')
        print('mother:\tsend to t8411 START')

    elif xing_elements_type == 'update_theme_sector':
        # # t1533 로 종목 data 수집
        # queues['tr']['t1533']['in'].put('start')
        # print('mother:\tsend to t1533 START')
        #
        # time.sleep(10)
        #
        # # t1537 로 종목 data 수집
        # queues['tr']['t1537']['in'].put('start')
        # print('mother:\tsend to t1537 START')

        # t8424 로 종목 data 수집
        # queues['tr']['t8424']['in'].put('start')
        # print('mother:\tsend to t8424 START')
        #
        # time.sleep(10)

        # t1516  로 종목 data 수집
        queues['tr']['t1516']['in'].put('start')
        print('mother:\tsend to t1516 START')

    return 1


def main():
    xing_elements = get_config_xing_elements()

    xing_elements = update_initial_xing_element(xing_elements)

    # child, mother 생성
    processes = {
        'mother': None,
        'children': {
            'real': {},
            'tr': {}
        },
        'mysql': {}
    }
    processes_pid = {
        'mother': None,
        'children': {
            'real': {},
            'tr': {}
        },
        'mysql': {}
    }
    queues = {
        'real': {},
        'tr': {}
    }
    functions = {
        'real': child_real,
        'tr': child_tr
    }

    # child 생성 및 dict 에 추가
    for key, values in xing_elements.items():
        xing_name = key
        xing_type = values['type']
        if (values['type'] == 'tr') or (values['type'] == 'real'):
            xing_element = values
            queue_out = Queue()
            queue_in = Queue()
            queue_mysql_out = Queue()
            queue_mysql_in = Queue()
            queues[xing_type][xing_name] = {}
            queues[xing_type][xing_name]['out'] = queue_out
            queues[xing_type][xing_name]['in'] = queue_in
            queues[xing_type][xing_name]['mysql_out'] = queue_mysql_out
            queues[xing_type][xing_name]['mysql_in'] = queue_mysql_in
            child_process = Process(target=functions[xing_type],
                                    args=(queue_out, queue_in, queue_mysql_out, queue_mysql_in, xing_element, mode))
            child_process.start()
            processes['children'][xing_type][xing_name] = child_process
            processes_pid['children'][xing_type][xing_name] = child_process.pid

        else:
            print('invalid type in xing_elements: {}'.format(values['type']))

        # mysql process 추가
        mysql_process = Process(target=child_mysql, args=(
        queues[xing_type][xing_name]['mysql_out'], queues[xing_type][xing_name]['mysql_in']))
        mysql_process.start()
        processes['mysql'][xing_name] = mysql_process
        processes_pid['mysql'][xing_name] = mysql_process.pid

    # child 모두 추가 및 start 후 mother 추가
    # child start 후에 parameter 로 input 해야 pid 추출 가능.
    mother_process = Process(target=mother, args=(queues, processes_pid, mode))
    mother_process.start()
    processes['mysql'] = mother_process
    processes_pid['mysql'] = mother_process.pid

    # processes join
    for key, values in processes.items():
        if isinstance(values, Process):
            values.join()
        elif isinstance(values, dict):
            for key_child, values_child in values.items():
                if isinstance(values_child, Process):
                    values_child.join()
                elif isinstance(values_child, dict):
                    if not len(values_child.keys()):
                        print('empty process dict {}: {}'.format(key_child, values_child))
                    else:
                        for key_grand_child, values_grand_child in values_child.items():
                            values_grand_child.join()


if __name__ == '__main__':
    mode = sys.argv[1]
    print(mode)
    main()
