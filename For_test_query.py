from classes.classes_Get_Data_with_QUERY import conn
from Query.queries import QUERRY_causes_deviations
from func.func_date_interval import getDateInt
import pandas as pd
from pprint import pprint
import json
import requests
import time

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

class Get_data_rejected_reason:
    def __init__(self, year = None, week = None, query=QUERRY_causes_deviations):
        self.year = year
        self.week = week
        self.proxy = {"http":"http://b.bulatov:Hc92DwSB@192.168.2.94:3128",
                      "https":"https://b.bulatov:Hc92DwSB@192.168.2.94:3128"}

        # self.token = 'dep16_token'
        # self.host = 'http://data-receiver.spb-dev.k8s.dev.ias.iac.spb.ru/api'

        self.host = 'http://data-receiver.isiao.vpn/api'                     # ХОСТ для ПРОДА
        self.token = 'z7GzKeMQwB2LNcKB'                                      # ТОКЕН ДЛЯ ПРОДА

        self.path_handbook = 'resourses/Справочник Причины отклонения сообщений на портале «Наш Санкт-Петербург».xlsx'
        self.handbook = self.get_code_dict()
        self.date_start, self.date_end = getDateInt(self.year, self.week)
        self.handbook_dict, self.list_Problem = self.get_code_dict()
        self.conn = conn
        self.query = query
        self.id = None

    def get_code_dict(self):
        handbook = pd.read_excel(self.path_handbook)
        handbook['index'] = handbook['Короткое наименование'].apply(lambda x: x.split(' ')[6] if len(x)>20 else 'all')
        handbook.index = handbook['index']
        handbook = handbook[['Код', 'Короткое наименование', 'Полное наименование']]
        handbook_dict = handbook.T.to_dict()
        handbook_dict = {k: v['Код'] for k, v in handbook_dict.items()}  # получаем словарь для выставления верных кодов проблем (согласно справочника)
        list_Problem = [ind for ind in handbook.index if ind != 'all'] # получаем список пунктов проблем для формирования запросов в БД
        return handbook_dict, list_Problem

    def get_data(self,query_modify):
        with self.conn.cursor() as cur:
                cur.execute(query_modify)
                raw = cur.fetchall()
        return raw

    def get_data_dict_for_prep(self):
        dict_data = {}
        for x in self.list_Problem:
            if not isinstance(x, str):
                x = str(x)
            query_modify = self.query.replace('causes_number', str(x)).replace('date_start', self.date_start).\
                                                                       replace('date_end', self.date_end)
            data = self.get_data(query_modify) # указать функциюполучения данных по запросу!
            dict_data[x] = data[0][0]
        # pprint(dict_data)
        dict_data = {self.handbook_dict[k]: v for k, v in dict_data.items()}
        return dict_data

    def prep_data_for_load(self):
        datasets = []
        dict_data = self.get_data_dict_for_prep()
        for key in dict_data.keys():
            code= key
            val = dict_data[key]
            date = f'{self.year}-W{self.week}'
            payload = {'indicatorCode': '98.18.8',
                     'series': [{'attributes': [{'ref': 'unit', 'value': 'ед.'}],
                                 'dimensions': [{'ref' : 'territory','value': '40000000000'},
                                                {'ref': 'reject_reason', 'value': code}],
                                 'observation': {'time': date, 'value': val}}]}
            datasets.append(payload)
        data_for_load = {'token': self.token, 'body': {'action': 'LOAD_SERIES', 'datasets':datasets}}
        return data_for_load

    def loading(self):
        METHOD_GET_ID = 'requests/'
        self.METHOD_GET_ID = METHOD_GET_ID
        URL_ID_DOC = f'{self.host}/{METHOD_GET_ID}'
        HEADER = {'Content-Type': 'application/json',
                  'Content-Length': '832',
                  'Host': self.host}
        data = json.dumps(self.prep_data_for_load()).encode('utf-8')
        res = requests.post(url=URL_ID_DOC, data=data, headers=HEADER, proxies=self.proxy)
        self.id = json.loads(res.text)["requestId"]
        print('Данные отправдлены в ХД')
        print(f'ID: {self.id}')

    def check_status(self):
        assert (self.id != None), 'поле ID пустое. Загрузите данные, чтобы полчить ID'
        res_status = requests.get(url=f'{self.host}/{self.METHOD_GET_ID}{self.id}?token={self.token}',proxies=self.proxy)
        df =pd.DataFrame(json.loads(res_status.text))
        df['Status'] = df['statusHistory'].apply(lambda x: x['status'])
        df['Description'] = df['statusHistory'].apply(lambda x: x['description'])
        df['Date'] = df['statusHistory'].apply(lambda x: x['date'])
        df = df[['requestId', 'done', 'Status', 'Description', 'Date']]
        print(df)

for i in range(1,45,1):
    a = Get_data_rejected_reason(week = i, year=2021)
    a.loading()

# a = Get_data_rejected_reason()
# pprint(a.handbook)