# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.6.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

import FinanceDataReader as fdr
import pandas_datareader as pdr
import pymysql
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
import pytz
import requests
from bs4 import BeautifulSoup
import re
from dateutil.relativedelta import relativedelta
import sys
import pymysql
import time
from matplotlib import pyplot as plt
import seaborn
import math
fdr.__version__

# +
### 원유 가격과 주식 시장의 추이를 살펴본다.

# +
#데이터베이스 연결
connection = pymysql.connect(host='localhost',
                             user='investment',
                             password='1111',
                             db='investment',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


with connection.cursor() as cursor:
    sql = '''SELECT record_date, oil_dbi_price, kospi_price, nasdaq_price, usd_krw_price
                FROM dailyfinancedata D;'''
    cursor.execute(sql)
    data = cursor.fetchall()

connection.close()    
df = pd.DataFrame(data)

# +
df['oil_dbi_price'] = df['oil_dbi_price'].astype(float)
df['kospi_price'] = df['kospi_price'].astype(float)
df['nasdaq_price'] = df['nasdaq_price'].astype(float)
df['usd_krw_price']= df['usd_krw_price'].astype(float)
df['oil_dbi_price_'] = df['oil_dbi_price'] * 30
df_krw = pd.DataFrame(df.loc[:,'usd_krw_price'])

df = df.set_index('record_date')
df.index = pd.to_datetime(df.index)
df_kospi = pd.DataFrame(df.loc[:,'kospi_price'])
df_nasdaq = pd.DataFrame(df.loc[:,'nasdaq_price'])
df_oil = pd.DataFrame(df.loc[:,'oil_dbi_price'])

df_kospi = df_kospi[np.isfinite(df_kospi['kospi_price'])]
df_oil = df_oil[np.isfinite(df_oil['oil_dbi_price'])]
df_nasdaq = df_nasdaq[np.isfinite(df_nasdaq['nasdaq_price'])]
df_krw = df_krw[np.isfinite(df_krw['usd_krw_price'])]

df.head()


# +
def plot(df, column_names=[], year=[], fig_size = (40,10), lines = [], normalize = False, minmax = False):
    
    
    df_plot = pd.DataFrame(df.loc[:, column_names])
    df_plot = df_plot[df_plot.notna()]
    df_plot = df_plot[df_plot.index.year.isin(year)]
    
    if(normalize == True):
        df_plot=(df_plot-df_plot.mean())/df_plot.std()
    if(minmax == True):    
        df_plot=(df_plot-df_plot.min())/(df_plot.max()-df_plot.min())
    
    df_plot.plot()
    plt.rcParams["figure.figsize"] = fig_size
    
    for line in lines:
        plt.axhline(y=line, color='r', linestyle='-')
    plt.show()
    #print(df_plot.head())
    #return df_plot
    
    
# -

plot(df, ['usd_krw_price'], list(range(1980, 2020)), lines = [800, 900, 1000, 1100, 1200])
plot(df, ['usd_krw_price'], [1996, 1997, 1998])
plot(df, ['usd_krw_price'], [2007, 2008, 2009])
plot(df, ['usd_krw_price'], [2018, 2019, 2020])


# +
plot(df, ['kospi_price', 'nasdaq_price', 'usd_krw_price'], list(range(1980, 2021)), normalize = True)
plot(df, ['kospi_price', 'nasdaq_price', 'usd_krw_price'], list(range(1980, 2021)), minmax = True)


#plot(df, ['kospi_price', 'nasdaq_price', 'usd_krw_price'], list(range(1997, 1999)), normalize = True)
#plot(df, ['kospi_price', 'nasdaq_price', 'usd_krw_price'], list(range(2008, 2010)), normalize = True)
#plot(df, ['kospi_price', 'nasdaq_price', 'usd_krw_price'], list(range(2019, 2021)), normalize = True)
# -

# ### 첫번째 궁금증) 유가와 KOSPI 주가는 어떤 관계를 갖는가?
#
# →전체 기간으로 보았을 때는 거의 관계가 없다.(매우 약한 양의 상관관계) 어느정도 비슷한 추이를 보이지만, 영향을 받지 않을 때도 있다. corr : 0.023
# 하지만! 구간을 나누어 보았을때는 확실히 차이가 보인다! 월별로 봤을 때는 차이 없음, 년도별로 보기!

df.plot()
plt.show()
print(df.corr())

# +

df_fall_97 = df[df.index.year.isin([1997, 1998])]
df_fall_08 = df[df.index.year.isin([2008, 2009])]
df_fall_20 = df[df.index.year.isin([2020])]
df_fall_97.plot()
plt.show()
df_fall_08.plot()
plt.show()
df_fall_20.plot()
plt.show()

# -

# ### 한국 주식 시장(KOSPI)는 마치 틀에 갇혀있는 것처럼 일정 수준의 주가를 유지하기 때문에 박스피라고 불린다. 그렇다면 박스권 안에서 어떤 주식은 성장하고 어떤 주식은 떨어졌을까?

# +
df_company = []
df_kospi = []
df_finance = []

connection = pymysql.connect(host='localhost',
                             user='investment',
                             password='1111',
                             db='investment',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

with connection.cursor() as cursor:
    sql = '''SELECT *
                FROM company c;'''
    cursor.execute(sql)
    df_company = pd.DataFrame(cursor.fetchall())
    
    sql = '''SELECT record_date, company_id, closing_price
                FROM companydailystockdata;'''
    cursor.execute(sql)
    df_kospi = pd.DataFrame(cursor.fetchall())
    
    sql = '''SELECT record_date, kospi_price
                FROM dailyfinancedata d
                WHERE d.kospi_price is not null;'''
    cursor.execute(sql)
    df_finance = pd.DataFrame(cursor.fetchall())
    
connection.close()
# -

df_kospi = df_kospi.set_index('record_date')
df_finance = df_finance.set_index('record_date')

# +

df_kospi['company_id'] = df_kospi['company_id'].astype(int)
df_finance = df_finance.astype(float)
print(df_company.head())
print(df_kospi.head())
print(df_finance.head())
# -

plt.plot(df_finance)
plt.axhline(y=2000, color='r', linestyle='-')
plt.rcParams["figure.figsize"] = (60,10)
plt.show()


# ## 우량기업 찾아내기

# +
## 우량기업 기준 : 자본 1000억 이상, 부채비율 100% 미만, 유동비율 150% 이상, 유보율 500% 이상
## 성장기업 : 매출이 평균 5% 이상 상승

# +
sql = '''SELECT * from financial_statement fs
            JOIN company c
            ON fs.company_id = c.id;'''
connection = pymysql.connect(host='localhost',
                             user='investment',
                             password='1111',
                             db='investment',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
fn_st = []
with connection.cursor() as cursor:
    cursor.execute(sql)
    fn_st = pd.DataFrame(cursor.fetchall())

fn_st.head()
    
# -

# %store -r name_list

# +
import itertools
id_set = (set(fn_st['company_id']))
count = 0
for id in id_set:
    
    fn_st_comp = fn_st[fn_st['company_id'] == id]
    year_list = fn_st_comp['year']
    fn_st_comp = fn_st_comp[fn_st_comp.columns[:2].tolist() + fn_st_comp.columns[3:-1].tolist()]
    print(fn_st_comp.to_string())
    
    break
        
    
# -


