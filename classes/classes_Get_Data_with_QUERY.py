import pandas as pd
import datetime
from func.func_date_interval import getDateInt
import psycopg2
import datetime

DB = 'openspb'
USER = 'mmironov'
PASS = 'bxmu7jda7Hzm'
HOST = '10.241.0.197'
PORT = '5432'
conn = psycopg2.connect(database=DB,
                        user=USER,
                        password=PASS,
                        host=HOST,
                        port=PORT)

class QueryData_indicator_1:
    def __init__(self, query, conn, year, week_number):
        self.conn = conn
        self.date_start = getDateInt(year, week_number)[0]
        self.date_end = getDateInt(year, week_number)[1]
        self.query = query.replace('date_start', self.date_start).replace('date_end', self.date_end)

    def get_raw_indicator_1(self):
        with self.conn.cursor() as cur:
            cur.execute(self.query)
            raw = cur.fetchall()
            full = raw[0][0]
            part = raw[0][1]
            return full, part, f'с {self.date_start} по {self.date_end}'

class QueryData_indicator_2_and_3:
    def __init__(self, query1, query2, conn, year, week_number, number):
        self.conn = conn
        self.number = number
        self.date_start = getDateInt(year, week_number)[0]
        self.date_end = getDateInt(year, week_number)[1]
        self.query1 = query1.replace('date_start', self.date_start).replace('date_end', self.date_end).replace(
            'number', str(self.number))
        self.query2 = query2.replace('date_start', self.date_start).replace('date_end', self.date_end).replace(
            'number', str(self.number))

    def get_raw_indicator_2(self):
        with self.conn.cursor() as cur1:
            cur1.execute(self.query1)
            full = cur1.fetchall()
        with self.conn.cursor() as cur2:
            cur2.execute(self.query2)
            part = cur2.fetchall()
            # full = raw[0][0]
            # part = raw[0][1]
            return round(full[0][0], 1), round(part[0][0], 1), f'с {self.date_start} по {self.date_end}'

class QueryData_indicator_4:
    def __init__(self, query, conn, year, week_number):
        self.conn = conn
        self.date_start = getDateInt(year, week_number)[0]
        self.date_end = getDateInt(year, week_number)[1]
        self.range = pd.date_range(start=self.date_start, end=self.date_end, freq='D')[:-1]
        self.delta = datetime.timedelta(days=1)
        self.query = query

    def get_raw_indicator_4(self):
        list_df = []
        with self.conn.cursor() as cur:
            for dat in self.range:
                try:
                    dat = self.range[0]
                    date_add = str((dat + self.delta).date())
                    dat = str(dat.date())
                    query = self.query.replace('date_start', dat).replace('date_end', date_add)
                    cur.execute(query)
                    data = pd.DataFrame(cur.fetchall())
                    data['ESIA'] = data[2].apply(lambda x: 1 if isinstance(x, pd._libs.tslibs.nattype.NaTType) else 0)
                    list_df.append(data)
                except Exception:
                    print(dat, '---', date_add)
                    break
            df = pd.concat(list_df).reset_index(drop=True)
            df['ESIA'] = df[2].apply(lambda x: 1 if isinstance(x, pd._libs.tslibs.nattype.NaTType) else 0)
            df_esia = data[data['ESIA'] == 1]
            df_esia = df_esia.groupby(data[0]).nunique().sum()['ESIA']
            df = len(df.groupby(df[0]).nunique().index)
        return df, df_esia

class Get_data_addendum:
    def __init__(self,  query1_1=None,
                        query_1_2=None,
                        query_1_3=None,
                        query_5=None,
                        query_reason=None,
                        conn=conn, num_week=None,year=None,  number=None):

        self.query_1_1 = query1_1
        self.query_1_2 = query_1_2
        self.query_1_3 = query_1_3
        self.query_5 = query_5
        self.query_reson = query_reason
        self.number = number
        self.conn = conn
        self.num_week = num_week
        self.year = year
        self.start_date = self.get_interval()[0]
        self.end_date = self.get_interval()[1]

    # Расчёт первых показателей
    def get_data_query_1(self, start_date, end_date):
        assert (self.query_1_1 is not None), 'Укажите запрос Query_1_1'
        assert (self.number is not None), 'Укажите NUMBER'
        query = self.query_1_1.replace('date_start', str(start_date)).\
                replace('date_end', str(end_date)).replace ('number', str(self.number))
        conn = self.conn
        with conn.cursor() as cur:
            cur.execute(query)
            raw = cur.fetchall()
            full_data = raw[0][0]
            rejected = raw[0][1]
            accepted = full_data - rejected
            precent_reject = round((rejected/full_data)*100,1)
            return full_data, rejected, accepted, precent_reject

    # ПОлучение интервала дат
    def get_interval(self):
        week = self.num_week
        year = self.year
        start_week = datetime.datetime.strptime(f'{year}-W{int(week)-1}-1', '%Y-W%W-%w').date()
        end_week = start_week + datetime.timedelta(days=7)
        return start_week, end_week

    # среднее время до принятия/отклонения
    def get_mean_time_accept_and_reject(self):
        assert (self.query_1_2 is not None), 'Укажите запрос Query_1_2'
        query_accept = self.query_1_2.replace('date_start', str(self.start_date)).replace('date_end',str( self.end_date)).\
            replace('number', str(self.number))
        query_reject = self.query_1_3.replace('date_start', str(self.start_date)).replace('date_end', str(self.end_date)).\
            replace('number', str(self.number))
        conn = self.conn
        with conn.cursor() as cur:
            cur.execute(query_accept)
            raw_accept = cur.fetchall()
            cur.execute(query_reject)
            raw_reject = cur.fetchall()
        return round(raw_accept[0][0]), round(raw_reject[0][0])

    # среднее время до принятия-отклонения
    def get_mean_time_accept_and_reject_ALL(self):
        assert (self.query_5 is not None), 'Укажите запрос Query_5'
        query = self.query_5.replace('date_start', str(self.start_date)).replace('date_end',str( self.end_date)).replace('number', str(self.number))
        conn = self.conn
        with conn.cursor() as cur:
            cur.execute(query)
            raw_accept = cur.fetchall()
        return round(raw_accept[0][0])

    # сравнение с предыдущей неделей
    def get_data_by_previous_week(self):
        assert ((self.num_week>0) & (self.num_week<53)) | (self.num_week==52) | (self.num_week==1), 'Не прописаны граничные условия для номера недели'
       # прошлая неделя
        week = self.num_week -1
        year = self.year
        start_date = datetime.datetime.strptime(f'{year}-W{int(week)}-1', '%Y-W%W-%w').date() - datetime.timedelta(
            weeks=1)
        end_date= start_date + datetime.timedelta(days=7)
        past_full_data, rejected, accepted,  past_week_precent = self.get_data_query_1(start_date, end_date)
        # текущая неделя
        now_full_data, rejected, accepted,  now_week_precent = self.get_data_query_1(self.start_date, self.end_date)
        precent_pervios_now_week = now_week_precent - past_week_precent
        full_date_precent = round(((now_full_data/past_full_data)-1)*100, 1)
        return round(precent_pervios_now_week,1), full_date_precent


class Get_data_rejected_reason:
    def __init__(self):
        pass


# -- Обработка справочника
# Получение справочника
# handbook = pd.read_excel(r'C:\Users\b.bulatov\PycharmProjects\DataBase_Our_SPB_connect\handbook_reason.xlsx')
# handbook['index'] = handbook['Короткое наименование'].apply(lambda x: x.split(' ')[6] if len(x)>20 else 'all')
# handbook.index = handbook['index']
# handbook = handbook[['Код', 'Короткое наименование', 'Полное наименование']]
# handbook_dict = handbook.T.to_dict() # ?????????????????????????????????????????????????????????????????????????????????
# # получаемактальный список пунктов
# list_Problem = [ind for ind in handbook.index if ind != 'all']
# # - Получение интервала дат
# date_start, date_end = getDateInt(2021, 42)
# # ----- Получение сырых данных
# def get_data(query):
#     with conn.cursor() as cur:
#         cur.execute(query)
#         raw = cur.fetchall()
#     return raw
# #---Плучение словаря
# dict_data = {}
# for x in list_Problem:
#     if not isinstance(x, str):
#         x = str(x)
#     q = QUERRY_causes_deviations.replace('causes_number', str(x)).\
#         replace('date_start', date_start).replace('date_end', date_end)
#     data = get_data(q)
#     dict_data[x] = data[0][0]
# pprint(dict_data)