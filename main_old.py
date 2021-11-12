import datetime
from func.Func_prepare_data_for_loads import (get_data_for_load, get_DataFrame_DB)

if __name__ == '__main__':
    #date_correct = datetime.datetime.now()
    #year, week, weekday = date_correct.date().isocalendar()
    get_data_for_load(2021, 42)
    get_data_for_load(2021, 43)
    get_data_for_load(2021, 44)

