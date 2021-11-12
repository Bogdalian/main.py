import pandas as pd
import json
import requests
from func.Func_prepare_data_for_loads import get_DataFrame_DB
import datetime
from func.func_date_interval import getDateInt
from pprint import pprint
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class File_loads:
    def __init__(self, year = None, week = None):
        # self.week = datetime.datetime.now().isocalendar().week
        # self.year = datetime.datetime.now().isocalendar().year
        self.week = week
        self.year = year
        self.proxy = {"http":"http://b.bulatov:Hc92DwSB@192.168.2.94:3128",
                      "https":"https://b.bulatov:Hc92DwSB@192.168.2.94:3128"}
        self.host = 'http://data-receiver.isiao.vpn/api'                     # ХОСТ для ПРОДА
        self.token = 'z7GzKeMQwB2LNcKB'                                      # ТОКЕН ДЛЯ ПРОДА
        # # -------------------------------------------------------------------------------------------------------------
        # self.host = 'http://data-receiver.spb-dev.k8s.dev.ias.iac.spb.ru/api' # ХОСТ ДЛЯ ТРЕНИРОВКИ
        # self.token = 'dep16_token'                                            # ТОКЕН ДЛЯ ТРЕНИРОВКИ

        self.METHOD_GET_ID = 'requests/'
        self.template = self.set_template() # шаблон для загрузки dataset'ов
        self.id = None

    '''
    Установить шаблон для обработки данных
    '''
    def set_template(self):
        return {'token': self.token, "body": {"action": "LOAD_SERIES", "datasets":[]}}

    '''
    Подтовка даных для загрузки
    '''
    def prepare_data(self):
        template = self.set_template()
        data_dict, num_week, year, date = get_DataFrame_DB(self.year, self.week)
        #print(data_dict)
        df = pd.read_excel('classes/test_template.xlsx')
        df['Значение'] = df['Код'].apply(lambda x: data_dict[x][0])
        df['Дата'] = f'{self.year}-W{self.week}'
        df['Территория'] = 40000000000

        list_code = df['Код'].unique()
        template = {'token': self.token, "body": {"action": "LOAD_SERIES", "datasets": []}}
        for code in list_code:  # пробег по всем DF
            df_prep = df[df['Код'] == code]
            # --------------------------- инициализация переменных: -----------------------------------
            indicator_code = str(code)
            territory_code = 40000000000
            ref = 'unit'
            unit_value = df_prep ['Ед. изм.'].values[0]
            point_date = df_prep ['Дата'].values[0]
            values_point = df_prep ['Значение'].values[0]  # для получения значений

            # --------------- Структура датасета в формате JSON --------------------------------------
            dataset = {"indicatorCode": str(indicator_code), "series":
                      [{"dimensions":
                      [{"ref": "territory",  # Примеры:
                      "value": str(territory_code)}],  # 40000000000
                      "attributes": [{"ref": str(ref),  # 'unit'
                      "value": str(unit_value)}],  # 'чел.'
                      "observation": {"time": str(point_date),  # '2021-10-19'/ '2018-H2'
                      "value": values_point}}]}  # значение
            # -------------------------- запись данных в датасет -------------------------------------
            template['body']['datasets'].append(dataset)
        return template
    '''
    Загрузка даных в ХД
    '''
    def loading(self):
        METHOD_GET_ID = 'requests/'
        URL_ID_DOC = f'{self.host}/{METHOD_GET_ID}'
        HEADER = {'Content-Type': 'application/json',
                  'Content-Length': '832',
                  'Host': self.host}
        data = json.dumps(self.prepare_data()).encode('utf-8')
        res = requests.post(url=URL_ID_DOC, data=data, headers=HEADER, proxies=self.proxy)
        #print(res.text)
        self.id = json.loads(res.text)["requestId"]
        print('Данные отправдлены в ХД')

    '''
    Проверка статуса запроса
    '''
    def check_status(self):
        assert (self.id != None), 'поле ID пустое. Загрузите данные, чтобы полчить ID'
        res_status = requests.get(url=f'{self.host}/{self.METHOD_GET_ID}{self.id}?token={self.token}',proxies=self.proxy)
        df =pd.DataFrame(json.loads(res_status.text))
        df['Status'] = df['statusHistory'].apply(lambda x: x['status'])
        df['Description'] = df['statusHistory'].apply(lambda x: x['description'])
        df['Date'] = df['statusHistory'].apply(lambda x: x['date'])
        df = df[['requestId', 'done', 'Status', 'Description', 'Date']]
        print(df)

    '''
    Получение интервала дат для выбраной недели
    '''
    def get_interval(self):
        start_date, end_date = getDateInt(self.year, self.week)
        print(f'Номер недели: {self.week}\nГод: {self.year}\nC {start_date} по {end_date}')



a =  File_loads(week=42, year=2021)
#a.loading()
#a.check_status()
# a.loading()
# print(a.check_status())