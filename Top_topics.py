import psycopg2
import pandas as pd
from dateutil.parser import parse
from collections import defaultdict
import numpy as np

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
query_problem_problem = '''
select
    pp.created_at,
    pp.reason_id
from
    problems_problem pp;
'''
query_class_reason = '''
select
    cr.category_id, -- для категорий 
    cr.classifier_id -- id_problem
from
    classifier_reason cr;
'''
query_classif = '''
select
    cc."id",
    cc."name"
from
    classifier_category cc;

'''

# соответствие номера id к названию
with conn.cursor() as cur:
    cur.execute(query_classif)
    raw = cur.fetchall()
df_classif= pd.DataFrame(raw)
df_classif.columns = ['id_category', 'id_problem']
dict_class = df_classif.to_dict()['id_problem']
dict_class = {int(key):val for key, val in dict_class.items()}

#-----------------------------------------------------------------------------------------------------------------------
with conn.cursor() as cur:
    cur.execute(query_class_reason)
    raw = cur.fetchall()
df_p= pd.DataFrame(raw)
df_p.columns = ['id_category', 'id_problem']
df_p = df_p.dropna()
df_p['id_category'] = df_p['id_category'] .astype(int)
df_p['id_category'] = df_p['id_category'].apply(lambda x: dict_class[x] if x in dict_class.keys() else 'нет данных')

dict_def= defaultdict(list)
uniq_problems = list(set(df_p['id_problem'].values))

# сопостваляем проблему к категории
for prob in uniq_problems:
    dict_def[prob].append(list(df_p[df_p['id_problem'] == prob].values)[0][0])


with conn.cursor() as cur:
    cur.execute(query_problem_problem)
    raw = cur.fetchall()

df= pd.DataFrame(raw)
df.columns = ['date', 'id_problem']
df['id_problem'] = df['id_problem'].apply(lambda x: dict_def[x])
df['date'] = df['date'].apply(lambda x: x.date())
df['id_problem'] = df['id_problem'].apply(lambda x: x[0] if len(x)>0 else np.nan)
df = df.dropna().reset_index(drop=True)
df.index = df['date']
df.index = pd.to_datetime(df.index, utc=True)
# count_cat = df.groupby(df['id_problem']).count().sort_values(['date'], ascending=False)
