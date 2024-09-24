#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import pandasql as ps
import os
import sys
import glob
import re
import datetime

date = sys.argv[1]
dt = datetime.datetime.strptime(date, '%Y-%m-%d')

df = pd.DataFrame() 

for i in range(1, 8):
    current_dt = dt - datetime.timedelta(days = i)
    path = 'input' + '/' + datetime.datetime.strftime(current_dt, '%Y-%m-%d') + '.csv'
    df = df.append(pd.read_csv(path, header = None))
df.columns = ['email', 'action', 'dt']

df_new = ps.sqldf("""
SELECT email, 
SUM(CASE WHEN action ='CREATE' then 1 else 0 end) as create_count,
SUM(CASE WHEN action ='READ' then 1 else 0 end) as read_count,
SUM(CASE WHEN action ='UPDATE' then 1 else 0 end) as update_count,
SUM(CASE WHEN action ='DELETE' then 1 else 0 end) as delete_count
FROM df
GROUP BY email;""", locals())

path = 'output' + '/' + date + '.csv'
df_new.to_csv(path)




