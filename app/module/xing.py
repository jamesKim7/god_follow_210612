import sys
import os
import win32com.client
import pythoncom
import time
from datetime import datetime as dt
import pprint
import copy

import pandas as pd

from logger import Logger
import xacom

import threading
from queue import Queue

log = Logger(__name__)


class XQuery:
    """
    TR 조회를 위한 XAQuery 확장 클래스

        :param trcode: TR 번호
        :type trcode: str
        :param call_next: callNext 가 False 인 경우, 한번만 조회, True 일 경우, 다음(occur)이 있으면 계속 조회 (기본값은 True)
        :type call_next: bool

        ::

            query = Query("t8407")
            query = Query("t1101", False)
    """

    def __init__(self, xing_name, trcode, q, call_next=True):
        super().__init__()
        self.query = win32com.client.DispatchWithEvents("XA_DataSet.XAQuery", _XAQueryEvents)
        self.query.LoadFromResFile("C:\\eBEST\\xingAPI\\Res\\" + trcode + ".res")
        self.status = 0
        self.code = None
        self.msg = None
        self.q = q
        self.xing_element = None
        self.xing_name = xing_name
        self.trcode = trcode
        self.callNext = call_next
        self.input = {}
        self.output = {}
        self.compress = 0
        # self.is_data_received = False
        # self.finish_this_cycle = False
        # self.result_data = ''

    # parse inputBlock
    def parse_input(self, param):
        print("<<<<< [Query] 입력:%s" % param)
        # log.info("<<<<< [Query] 입력:%s" % param)
        for v in param.keys():
            if v != "Service":
                self.input_name = v
        self.input = param[self.input_name]
        self.compress = "comp_yn" in self.input['columns_names_and_values'].keys() and \
                        self.input['columns_names_and_values']["comp_yn"] == "Y"
        if "Service" in param:
            self.service = param["Service"]

    # parse outputBlock
    def parse_output(self, param):
        self.output = {}
        output_copy = copy.deepcopy(param)
        for k, v in output_copy.items():
            self.output[k] = v
            """
            if isinstance(v, list):
                # occur
                self.output[k] = v
            else:
                self.output[k] = v
                # self.output[k] = {}
                # for p in v:
                #     self.output[k][p] = None
            """

    # TR을 전송한다.
    def request(self, xing_element, is_next=False):
        """TR을 요청한다.
            :param input: TR의 input block 정보
            :type input: object { "InBlock" : { ... } }
            :param output: TR의 output block 정보. output block을 여러개가 존재할 수 있으며, DataFrame타입일 경우, occur 데이터를 반환한다.
            :type output: object { "OutBlock" : DataFrame or tuple, "OutBlock1" : DataFrame or tuple} }
            :param is_next: 연속 조회를 사용하기 위한 내부 파라미터로서 직접 사용하지 않는다.
            :return: output으로 지정한 형태로 값이 채워져서 반환된다.
            :rtype: object

            .. note::

                input 키값이 "Service"인 경우, RequestService 로 요청할 수 있다. 예) 종목검색(씽API용), ChartIndex(차트지표데이터 조회) TR

            .. warning:: 절대 개발자가 isNext값을 지정하지 않는다.


            ::

                    Query("t8407").request({
                        "InBlock" : {
                            "nrec" : 2,
                            "shcode" : "".join(["005930","035420"])
                        }
                    },{
                        "OutBlock1" : DataFrame(columns=("shcode","hname","price","open","high",
                                "low","sign","change","diff","volume"))
                    })
                    # 반환값
                    {
                        # output에서 지정한 DataFrame에 row값이 채워진 DataFrame이 반환된다.
                        "OutBlock1" : DataFrame
                    }

                    Query("t1101", False).request({
                        "InBlock" : {
                            "shcode" : "005930"
                        }
                    },{
                        "OutBlock" : ("hname","price", "sign", "change", "diff", "volume", "jnilclose",
                        "offerho1", "bidho1", "offerrem1", "bidrem1", "preoffercha1","prebidcha1",
                        "offerho2", "bidho2", "offerrem2", "bidrem2", "preoffercha2","prebidcha2",
                        "offerho3", "bidho3", "offerrem3", "bidrem3", "preoffercha3","prebidcha3",
                        "offerho4", "bidho4", "offerrem4", "bidrem4", "preoffercha4","prebidcha4",
                        "offerho5", "bidho5", "offerrem5", "bidrem5", "preoffercha5","prebidcha5",
                        "offerho6", "bidho6", "offerrem6", "bidrem6", "preoffercha6","prebidcha6",
                        "offerho7", "bidho7", "offerrem7", "bidrem7", "preoffercha7","prebidcha7",
                        "offerho8", "bidho8", "offerrem8", "bidrem8", "preoffercha8","prebidcha8",
                        "offerho9", "bidho9", "offerrem9", "bidrem9", "preoffercha9","prebidcha9",
                        "offerho10", "bidho10", "offerrem10", "bidrem10", "preoffercha10","prebidcha10",
                        "offer", "bid", "preoffercha", "prebidcha", "uplmtprice", "dnlmtprice",
                              "open", "high", "low", "ho_status", "hotime"
                        )
                    })
                    # 반환값
                    {
                        # output에서 지정한 DataFrame에 row값이 채워진 DataFrame이 반환된다.
                        "OutBlock" : DataFrame
                    }

                    Query("t1833").request({
                        "Service" : filepath
                    }, {
                        "OutBlock" : ("JongCnt",),
                        "OutBlock1" : DataFrame(columns=("shcode", "hname", "close", "change","diff"))
                    })
                    # 반환값
                    {
                        # output에서 지정한 tuple은 키와 값이 있는 direction으로 변경되어 반환된다.
                        "OutBlock" : { "JongCnt": ... },
                        # output에서 지정한 DataFrame에 row값이 채워진 DataFrame이 반환된다.
                        "OutBlock1" : DataFrame
                    }
        """

        # occur start
        xing_element['date_time']['occur_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')
        xing_element['occur']['size'] = 0

        self.xing_element = xing_element
        input = xing_element['InBlocks']
        output = xing_element['OutBlocks']

        if not input:
            input = {"InBlock": {}}
        if not is_next:
            pass
        self.query.reset()
        self.parse_input(input)
        self.parse_output(output)

        # SetFieldData setting
        for k, v in self.input['columns_names_and_values'].items():
            self.query.SetFieldData(self.trcode + self.input_name, k, 0, v)

        # Request (Service 고려하지 않음)
        requestCode = self.query.Request(is_next)
        """
            연속 주문일 경우만 isNext = True
        """

        if requestCode < 0:
            log.critical(xacom.parseErrorCode(requestCode))
            time.sleep(3)
            xing_element['phase'] = 'done'
            self.q.put(xing_element)

            return xing_element

        while self.query.status == 0:
            pythoncom.PumpWaitingMessages()

        time_start = time.time()
        while self.query.status_msg == 0:
            pythoncom.PumpWaitingMessages()

            # t1717의 경우 마지막 data는 수신이 안되기에 일정시간 지나면 break 하도록.
            if self.trcode == 't1717':
                if time.time() - time_start < 2:
                    time.sleep(1)
                else:
                    self.query.status_msg = '0000'

        if self.query.status_msg == '자료없음':
            log.critical('해당자료가 없습니다.')
            time.sleep(3)
            xing_element['phase'] = 'done'
            self.q.put(xing_element)

            # t1717 은 연속조회에 문제가 있어 occurs 시 date 직접 업데이트 해야함.
            if self.trcode == 't1717':
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['todt'] = ''

            return xing_element

        elif self.query.status_msg == 2:
            log.critical('자료가 없는 경우 이외의 오류가 발생했습니다.')
            time.sleep(3)
            xing_element['phase'] = 'done'
            self.q.put(xing_element)

            return xing_element

        # occur listened
        xing_element['date_time']['occur_listened'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

        self.listened_data = {}
        # output setting
        for k, v in self.output.items():
            self.output[k]['listened_data'] = []

        for k, v in self.output.items():
            if v['type'] == 'occurs':
                table_header = v['columns_names']
                if self.compress:
                    self.query.Decompress(self.trcode + k)
                self.listened_data[k] = []
                for p in range(0, self.query.GetBlockCount(self.trcode + k)):
                    block_list = []
                    for col in table_header:
                        block_list.append(self.query.GetFieldData(self.trcode + k, col, p))
                    self.output[k]['listened_data'].append(block_list)
                # print(self.output[k]['listened_data'][-1])
            elif v['type'] == 'non_occurs':
                table_header = v['columns_names']
                # occurs 일 경우를 위해 cts value update
                input_keys = self.input['columns_names_and_values'].keys()
                self.listened_data[k] = []
                block_list = []
                for col in table_header:
                    block_list.append(self.query.GetFieldData(self.trcode + k, col, 0))
                    if self.query.IsNext:
                        if col in input_keys:
                            self.input['columns_names_and_values'][col] = block_list[-1]
                self.output[k]['listened_data'].append(block_list)

        # xing_element update
        xing_element['phase'] = 'listened'
        xing_element['OutBlocks'] = self.output
        xing_element['occur']['size'] = sys.getsizeof(str(self.output))

        self.q.put(xing_element)

        second_need_to_sleep = max(0.0,
                                   3.0 - (dt.strptime(xing_element['date_time']['occur_listened'], '%Y%m%d %H%M%S.%f') -
                                          dt.strptime(xing_element['date_time']['occur_start'],
                                                      '%Y%m%d %H%M%S.%f')).total_seconds())
        time.sleep(second_need_to_sleep)

        # 초당 limit 고려
        """
        # 초당 전송 횟수를 고려하여 sleep
        tr_count_per_sec = self.query.GetTRCountPerSec(self.trcode)
        second_need_to_sleep = max(0.0,
                                   tr_count_per_sec -
                                   (dt.strptime(xing_element['date_time']['occur_listened'], '%Y%m%d %H%M%S.%f') -
                                    dt.strptime(xing_element['date_time']['occur_start'], '%Y%m%d %H%M%S.%f'))
                                   .total_seconds())
        time.sleep(second_need_to_sleep)

        # 기간(10분)당 전송 횟수를 고려
        # TODO : 10분 제한이 걸리면 blocking state 진입
        # tr_count_limit = self.query.GetTRCountLimit(self.trcode)
        # tr_count_10min = self.query.GetTRCountRequest(self.trcode)
        # if tr_count_10min >= tr_count_limit:
        #     second_need_to_sleep = max(0.0,
        #                                10 * 600 -
        #                                (dt.strptime(xing_element['date_time']['occur_listened'], '%Y%m%d %H%M%S.%f') -
        #                                 dt.strptime(xing_element['date_time']['occur_start'], '%Y%m%d %H%M%S.%f'))
        #                                .total_seconds())
        #     time.sleep(second_need_to_sleep)
        # 10분 마다 check 하는 논리가 추가되어야 함.
        """

        # t1305 은 연속조회에 문제가 있어 occurs 시 date 직접 업데이트 해야함.
        if self.trcode == 't1305' and xing_element['mode'] == 'day_by_day_candle_chart':
            # 당일 데이터 수집할 경우
            self.query.IsNext = 0

        # t1637 은 연속조회에 문제가 있어 occurs 시 date 직접 업데이트 해야함.
        if self.trcode == 't1637' and self.query.IsNext:
            # 일자 데이터 수집할 경우
            if xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun2'] == '1':
                date_last = xing_element['OutBlocks']['OutBlock1']['listened_data'][-1][0]
                xing_element['InBlocks']['InBlock']['columns_names_and_values']['date'] = date_last

        # t1665 은 연속조회에 문제가 있어 occurs 시 date 직접 업데이트 해야함.
        if self.trcode == 't1665':
            # 일자 데이터 수집할 경우
            if xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun3'] == '1':
                if xing_element['OutBlocks']['OutBlock1']['listened_data']:
                    date_last = xing_element['OutBlocks']['OutBlock1']['listened_data'][-1][0]

                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['to_date'] = date_last
                else:
                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['to_date'] = ''

        # t1717 은 연속조회에 문제가 있어 occurs 시 date 직접 업데이트 해야함.
        if self.trcode == 't1717':
            # 일자 데이터 수집할 경우
            if xing_element['InBlocks']['InBlock']['columns_names_and_values']['gubun'] == '0':
                if xing_element['OutBlocks']['OutBlock']['listened_data']:
                    date_last = xing_element['OutBlocks']['OutBlock']['listened_data'][-1][0]

                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['todt'] = date_last
                else:
                    xing_element['InBlocks']['InBlock']['columns_names_and_values']['todt'] = ''

        self.query.status = 0
        if self.query.IsNext:
            if self.callNext:
                return self.request(xing_element, True)
            else:
                # log.debug(">>>>> [Query] 결과(callNext=False):%s" % self.output)
                xing_element['phase'] = 'done'
                self.q.put(xing_element)

                return xing_element
        else:
            # log.debug(">>>>> [Query] 결과(callNext=True):%s" % self.output)
            xing_element['phase'] = 'done'
            self.q.put(xing_element)

            return xing_element


class Real(threading.Thread):
    """실시간 TR을 모니터링하는 작업 클래스
        :param trcode: 실시간 TR 코드
        :type trcode: str
        :param outputStyle: 실시간 TR 반환 데이터 컬럼
        :type outputStyle: tuple
        :param queue: 실시간 TR 반환 데이터를 저장할 큐 객체
        :type queue: Queue
        :return: self
        ::
            real = Real("SC1", ("eventid", "ordxctptncode", "ordmktcode", "ordptncode", "mgmtbrnno",
                "accno1","Isuno", "Isunm", "ordno", "orgordno", "execno",
                "ordqty", "ordprc", "execqty", "execprc", "ordtrxptncode",
                "secbalqty", "avrpchsprc", "pchsant"), Queue(100))
    """

    def __init__(self, trcode, outputStyle, queue):
        threading.Thread.__init__(self)
        self.real = win32com.client.DispatchWithEvents("XA_DataSet.XAReal", _XARealEvents)
        self.real.LoadFromResFile("C:\\eBEST\\xingAPI\\Res\\" + trcode + ".res")
        self.real.queue = queue
        self.real.outputStyle = outputStyle
        self.running = True

    def addTarget(self, value, xing_element, name="shcode"):
        """타겟을 등록한다. 여기서 타겟은 모니터링하는 대상이다.
            :param value: 모니터링할 대상. 기본값은 None
            :type value: None, str
            :param name: 모니터링할 대상의 속성. 기본값은 "shcode"
            :type name: str
            :return: self
            .. note::
                - 타겟에 정보를 전달할 필요가 없는 '주문 체결'과 같은 작업도 addTarget를 호출하여 모니터링을 시작해야한다.
                - '코스피 호가'는 타겟(종목코드(shcode))를 추가로 여러개 모니터링 할 수 있다.
            ::
                real.addTarget()
                real.addTarget("005930")
                real.addTarget(["005930","035420"])
        """

        self.real.xing_element = xing_element

        if value is None:
            self.removeAllTargets()
            self.real.AdviseRealData()
        else:
            if isinstance(value, str):
                value = [value]
            for v in value:
                self.real.SetFieldData("InBlock", name, v)
                self.real.AdviseRealData()
        return self

    def removeAllTargets(self):
        """모든 타겟의 모니터링을 제거한다.
            ::
                real.removeAllTargets()
        """
        self.real.UnadviseRealData()

    def removeTarget(self, key):
        """특정 타겟의 모니터링을 제거한다.
            :param key: 특정 타겟
            :type key: str
            ::
                real.removeAllTargets()
        """
        self.real.UnAdviseRealDataWithKey(key)

    def run(self):
        """실시간 TR을 모니터링 한다
            ::
                real.run()
        """
        while self.running:
            pythoncom.PumpWaitingMessages()
            # print("[%d] Thread is alive ? : %s" % (self.ident, self.is_alive()))


class RealManager:
    """실시간 TR을 모니터링하는 작업 클래스(Real)을 관리하는 클래스
    ::
        manager = RealManager()
    """

    def __init__(self):
        self.tasks = {}
        self.queues = {}
        self.queue_txt_writer = None

    def addTask(self, trcode, outputStyle, maxQueue, queue_txt_writer):
        """실시간 작업을 추가한다
            :param trcode: 실시간 TR 코드
            :type trcode: str
            :param outputStyle: 실시간 TR 반환 데이터 컬럼
            :type outputStyle: tuple
            :param maxQueue: 실시간 TR 반환 데이터를 저장할 큐의 개수
            :type maxQueue: int
            :return: Real
        ::
            # 주문 체결
            manager.addTask("SC1", ("Isuno", "Isunm", "ordno", "orgordno",
                     "eventid", "ordxctptncode", "ordmktcode",
                     "ordptncode", "mgmtbrnno",  "accno1",
                     "execno", "ordqty", "ordprc", "execqty",
                     "execprc", "ordtrxptncode", "secbalqty",
                     "avrpchsprc", "pchsant"), 50).addTarget()
            # 코스피 호가
            manager.addTask("H1_", ("shcode", "hottime","totofferrem", "totbidrem",
                        "offerho1", "bidho1", "offerrem1", "bidrem1",
                        "offerho2", "bidho2", "offerrem2", "bidrem2",
                        "offerho3", "bidho3", "offerrem3", "bidrem3",
                        "offerho4", "bidho4", "offerrem4", "bidrem4",
                        "offerho5", "bidho5", "offerrem5", "bidrem5",
                        "offerho6", "bidho6", "offerrem6", "bidrem6",
                        "offerho7", "bidho7", "offerrem7", "bidrem7",
                        "offerho8", "bidho8", "offerrem8", "bidrem8",
                        "offerho9", "bidho9", "offerrem9", "bidrem9",
                        "offerho10", "bidho10", "offerrem10", "bidrem10"
                    ), 100).addTarget(["005930","035420"])
            # 코스닥 호가
            manager.addTask("HA_", ("shcode", "hottime","totofferrem", "totbidrem",
                        "offerho1", "bidho1", "offerrem1", "bidrem1",
                        "offerho2", "bidho2", "offerrem2", "bidrem2",
                        "offerho3", "bidho3", "offerrem3", "bidrem3",
                        "offerho4", "bidho4", "offerrem4", "bidrem4",
                        "offerho5", "bidho5", "offerrem5", "bidrem5",
                        "offerho6", "bidho6", "offerrem6", "bidrem6",
                        "offerho7", "bidho7", "offerrem7", "bidrem7",
                        "offerho8", "bidho8", "offerrem8", "bidrem8",
                        "offerho9", "bidho9", "offerrem9", "bidrem9",
                        "offerho10", "bidho10", "offerrem10", "bidrem10"
                    ), 100).addTarget("168330")
        """
        queue = Queue(maxQueue)
        realTask = Real(trcode, outputStyle, queue)
        self.tasks[trcode] = realTask
        self.queues[trcode] = queue
        realTask.start()

        self.queue_txt_writer = queue_txt_writer
        realTask.real.queue = self.queue_txt_writer
        return realTask

    def removeTask(self, type):
        """실시간 작업을 제거한다.
            :param type: 실시간 TR 코드
            :type type: str
        ::
            manager.removeTask("SC1")
        """
        task = self.getTask(type)
        if task:
            task.removeAllTargets()
            # @todo check 큐에 있는걸 다 비우고 할것인가?
            task.running = False
            del self.queues[type]
            del self.tasks[type]

    def getTask(self, type):
        """실시간 작업을 얻는다.
            :param type: 실시간 TR 코드
            :type type: str
            :return: 실시간 작업 객체
            :rtype: Real
        ::
            manager.getTask("SC1")
        """
        return self.tasks[type] if type in self.tasks else None

    def getQueue(self, type):
        """실시간 작업의 큐를 얻는다.
            :param type: 실시간 TR 코드
            :type type: str
            :return: 실시간 작업 객체의 큐
            :rtype: Queue
        ::
            manager.getQueue("SC1")
        """
        return self.queues[type] if type in self.queues else None

    def run(self, cb=None):
        """실시간 TR별 큐의 정보를 추출하여 callback 함수로 전달한다.
            :param cb: 큐에서 추출된 정보를 받을 callback 함수
            :type type: def
            .. note::
                callback 함수는 type(실시간 TR코드)와 data(실시간 TR의 outputStyle의 데이터)를 파라미터로 갖는다.
        ::
            def callback(type, data):
                for i in range(len(data)):
                    if type == "SC1":
                        # ...
                    elif type == "JIF":
                        # ...
                    else:
                        # ...
            manager.run(callback)
        """
        for k, v in self.queues.items():
            data = []
            queue = self.getQueue(k)
            if queue and queue.qsize() > 0:
                for i in range(queue.qsize()):
                    data.append(queue.get())
            if cb:
                cb(k, data)


class Real_light():
    """실시간 TR을 모니터링하는 작업 클래스
        :param trcode: 실시간 TR 코드
        :type trcode: str
        :param outputStyle: 실시간 TR 반환 데이터 컬럼
        :type outputStyle: tuple
        :param queue: 실시간 TR 반환 데이터를 저장할 큐 객체
        :type queue: Queue
        :return: self
        ::
            real = Real("SC1", ("eventid", "ordxctptncode", "ordmktcode", "ordptncode", "mgmtbrnno",
                "accno1","Isuno", "Isunm", "ordno", "orgordno", "execno",
                "ordqty", "ordprc", "execqty", "execprc", "ordtrxptncode",
                "secbalqty", "avrpchsprc", "pchsant"), Queue(100))
    """

    def __init__(self, xing_name, trcode, outputStyle, queue):
        self.real = win32com.client.DispatchWithEvents("XA_DataSet.XAReal", _XARealEvents)
        self.real.LoadFromResFile("C:\\eBEST\\xingAPI\\Res\\" + trcode + ".res")
        self.real.xing_name = xing_name
        self.real.trcode = trcode
        self.real.queue = queue
        self.real.outputStyle = outputStyle
        self.running = True

    def addTarget(self, value, xing_element, name="shcode"):
        """타겟을 등록한다. 여기서 타겟은 모니터링하는 대상이다.
            :param value: 모니터링할 대상. 기본값은 None
            :type value: None, str
            :param name: 모니터링할 대상의 속성. 기본값은 "shcode"
            :type name: str
            :return: self
            .. note::
                - 타겟에 정보를 전달할 필요가 없는 '주문 체결'과 같은 작업도 addTarget를 호출하여 모니터링을 시작해야한다.
                - '코스피 호가'는 타겟(종목코드(shcode))를 추가로 여러개 모니터링 할 수 있다.
            ::
                real.addTarget()
                real.addTarget("005930")
                real.addTarget(["005930","035420"])
        """

        self.real.xing_element = xing_element

        if value is None:
            self.removeAllTargets()
            self.real.AdviseRealData()
        else:
            if isinstance(value, str):
                value = [value]
            for v in value:
                self.real.SetFieldData("InBlock", name, v)
                self.real.AdviseRealData()
        return self

    def removeAllTargets(self):
        """모든 타겟의 모니터링을 제거한다.
            ::
                real.removeAllTargets()
        """
        self.real.UnadviseRealData()

    def removeTarget(self, key):
        """특정 타겟의 모니터링을 제거한다.
            :param key: 특정 타겟
            :type key: str
            ::
                real.removeAllTargets()
        """
        self.real.UnAdviseRealDataWithKey(key)

    @staticmethod
    def run():
        """실시간 TR을 모니터링 한다
            ::
                real.run()
        """
        # while self.running:
        #     pythoncom.PumpWaitingMessages()
        #     print("[%d] Thread is alive ? : %s" % (self.ident, self.is_alive()))
        pythoncom.PumpMessages()


class _XAQueryEvents:
    def __init__(self):
        self.status = 0
        self.status_msg = 0
        self.code = None
        self.msg = None

    def reset(self):
        self.status = 0
        self.status_msg = 0
        self.code = None
        self.msg = None

    def OnReceiveData(self, szTrCode):
        # print(" - onReceiveData (%s:%s)" % (szTrCode, xacom.parseTR(szTrCode)))
        # log.debug(" - onReceiveData (%s:%s)" % (szTrCode, xacom.parseTR(szTrCode)))
        self.status = 1

    def OnReceiveMessage(self, systemError, messageCode, message):
        self.code = str(messageCode)
        self.msg = str(message)
        # print(" - OnReceiveMessage (%s:%s)" % (self.code, self.msg))
        # log.debug(" - OnReceiveMessage (%s:%s)" % (self.code, self.msg))
        if self.msg != '해당자료가 없습니다':
            self.status_msg = self.msg
        else:
            self.status_msg = '자료없음'


class _XARealEvents:
    def __init__(self):
        self.xing_element = None
        self.xing_name = ''
        self.trcode = ''
        self.queue = None
        self.outputStyle = None

    # put data in queue
    def _putData(self, trCode):
        output = {}
        for v in self.outputStyle:
            output[v] = self.GetFieldData('OutBlock', v)

        outblock = {
            'type': 'non_occurs',
            'columns_names': list(output.keys()),
            'listened_data': [list(output.values())]
        }
        return outblock

    def OnReceiveRealData(self, szTrCode):
        # print(' - OnReceiveRealData ({})'.format(szTrCode))
        # log.debug(" - OnReceiveRealData (%s)" % szTrCode )
        # log.debug(self._putData(szTrCode))
        xing_element = self.xing_element

        outblock = self._putData(szTrCode)

        xing_element['phase'] = 'listened'
        xing_element['OutBlocks']['OutBlock'] = outblock
        xing_element['occur']['size'] = sys.getsizeof(str(outblock))
        xing_element['date_time']['occur_start'] = dt.today().strftime('%Y%m%d %H%M%S.%f')
        xing_element['date_time']['occur_listened'] = dt.today().strftime('%Y%m%d %H%M%S.%f')

        if xing_element['occur']['size'] == '':
            xing_element['occur']['size'] = 0

        self.queue.put(xing_element)


# login 할 경우 사용
class XASessionEventHandler:

    def __init__(self):
        super().__init__()
        self.login_state = False
        self.is_data_received = False
        self.finish_this_cycle = False
        self.result_data = ''

    def OnLogin(self, code, msg):
        if code == "0000":
            self.login_state = True
            self.result_data = msg
            self.is_data_received = True
            self.finish_this_cycle = True

        else:
            self.login_state = False
            self.result_data = msg
            self.is_data_received = True
            self.finish_this_cycle = True


# real data receiver
class real_data_receiver:

    def __init__(self):
        super().__init__()

    def receive_OVC(self, result_data):
        print(result_data)
        if result_data[12] > 1 and result_data[12] < 6:
            print('make order')

    def receive_S3_(self, result_data):
        print(result_data)

    def order_ovs_future(self):
        RecCnt = 1
        OrdDt = '20200721'
        BrnCode = ' '
        AcntNo = '55555028307'
        Pwd = '0000'
        IsuCodeVal = 'EDQ20'
        # 'GCQ20' 바로채결
        # 'EDQ20' 시간 좀 걸림
        FutsOrdTpCode = '1'
        BnsTpCode = '2'
        AbrdFutsOrdPtnCode = '1'
        CrcyCode = ' '
        OvrsDrvtOrdPrc = ''
        CndiOrdPrc = ''
        OrdQty = '1'
        PrdtCode = ' '
        DueYymm = ''
        ExchCode = ' '

        # get_CIDBT00100(RecCnt, OrdDt, BrnCode, AcntNo, Pwd, IsuCodeVal, FutsOrdTpCode, BnsTpCode, AbrdFutsOrdPtnCode, CrcyCode, OvrsDrvtOrdPrc, CndiOrdPrc, OrdQty, PrdtCode, DueYymm, ExchCode)


class Xing:

    def __init__(self):
        super().__init__()
        self.S3_ = 0

    # 로그인하기
    @staticmethod
    def login(server):

        pythoncom.CoInitialize()

        id = "mura13"
        passwd = ''
        if server == 'demo':
            passwd = "kibum12"
        elif server == 'hts':
            passwd = 'kibum#12'
        cert_passwd = 'hip_lezzic12'

        instXASession = win32com.client.DispatchWithEvents("XA_Session.XASession", XASessionEventHandler)

        instXASession.ConnectServer('{}.ebestsec.co.kr'.format(server), 20001)
        instXASession.Login(id, passwd, cert_passwd, 0, 0)

        while not instXASession.is_data_received:
            pythoncom.PumpWaitingMessages()
        while not instXASession.finish_this_cycle:
            pythoncom.PumpWaitingMessages()

        result_dict = {
            'login_state': instXASession.login_state,
            'server': server,
            'server_msg': instXASession.result_data
        }

        # 계좌번호 가져오기

        accounts_cnt = instXASession.GetAccountListCount()
        accounts_num_list = []
        for i in range(accounts_cnt):
            accounts_num_list.append(instXASession.GetAccountList(i))
        print(accounts_num_list)

        return result_dict

        pythoncom.CoUninitialize()
