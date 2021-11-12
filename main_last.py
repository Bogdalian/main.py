from func.Func_prepare_data_for_loads import (get_DataFrame_DB, get_data_for_load)
from classes.class_File_loads import *
import pandas as pd
import datetime
import requests
import json
import time
from pprint import pprint

year = 2021
week = 41
if __name__ == '__main__':
    for i in range(1,43,1):
        week = i
        res = File_loads(year = year, week = week)
        # res.loading()
        # print(' Загрузка даннных в ХД звершена')
        # ------- создание докумена excel ------------
        title = get_data_for_load(res.year, res.week)
        # print(title)
print('Создание файла excel завершено')