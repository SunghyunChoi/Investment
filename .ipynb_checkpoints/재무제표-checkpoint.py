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
import urllib.request
from selenium.webdriver import Chrome
import json
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys

# + jupyter={"outputs_hidden": true}
## companydailystockdata - organ, foreign, foreign ratio update
    def financial_statement(record_year):
    connection = pymysql.connect(host='localhost',
                                 user='investment',
                                 password='1111',
                                 db='investment',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    read_company_data = f"SELECT id,code FROM company;"
    answer = {}
    browser = Chrome()

    with connection.cursor() as cursor:

        cursor.execute(read_company_data)
        results = cursor.fetchall()
        comp_count = 0

        for result in results: 

            company_code, company_id = result['code'], result['id']
            comp_count += 1
            if(comp_count < 359):
                continue
            #Getting Data from Naver Finance using Selenium
            try:
                browser.get(f'https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={company_code}&target=finsum_more')
                browser.find_elements_by_xpath('''//*[@id="cns_Tab21"]''')[0].click()
            except Exception as e:
                print(e)
                continue
            html = browser.page_source
            soup = BeautifulSoup(html, 'html.parser')
            financial_data = soup.find('table', {'class' : 'gHead01 all-width', 'summary' : '주요재무정보를 제공합니다.'})

            #Getting year data : year_dict
            data_list =financial_data.select('thead>tr')[1]
            year_list = data_list.find_all('th')
            this_year = str(datetime.now().year-1)
            year_dict = {}
            for date in year_list:
                year = date.text.strip()[:4]
                year_dict[year] = []
                if(year == this_year):
                    break

            #Getting Data        
            fd_data = financial_data.select('tbody>tr')
            fd_name = []
            fd_num = []
            fd_name_eng = ['sales', 'operating_income', '-', '-', '-', 'net_income', '-',  'asset', 'liability', '-', 'capital', '-', 'capital_stock', 'operating_cashflow', 'investment_cashflow', 'financial_cashflow', 'capex', 'fcf', 'Interest_liability', 'operating_margin', 'net_margin', 'roe', 'roa', 'debt_ratio', 'capital_reserve_rate', 'eps', 'per', 'bps', 'pbr', 'dps', 'dividend_yield', 'dividend_payout', 'issued_stocks']
            for fd in fd_data:
                fd_name.append(fd.find('th').text.strip())


            for fd, name in zip(fd_data, fd_name):
                fd_num_list = fd.find_all('td')

                for idx,year in enumerate(year_dict.keys()):

                    #print(year, name, fd_num_list[idx].text)
                    num = (fd_num_list[idx].text.strip()).replace(',', '')
                    year_dict[year].append(num)



            for key in year_dict.keys():
                insert = []
                dic = year_dict[key]
                
                if(key != record_year):
                    continue
                
                for idx, name,eng_name, num in zip(range(len(fd_name)),fd_name,fd_name_eng, dic):
                    if(eng_name == '-'):
                        continue
                    elif(num == ''):
                        num = '0'
                    insert.append([num])
                    #print(idx+1, name, eng_name,num, type(num))


                try:
                    sql = '''INSERT IGNORE INTO `financial_statement` 
                                        (`company_id`, `year`, `sales`, `operating_income`, `net_income`, `asset`, `liability`, `capital`, `capital_stock`, `operating_cashflow`, `investment_cashflow`, `financial_cashflow`, `capex`, `fcf`, `Interest_liability`, `operating_margin`, `net_margin`, `roe`, `roa`, `debt_ratio`, `capital_reserve_rate`, `eps`, `per`, `bps`, `pbr`, `dps`, `dividend_yield`, `dividend_payout`, `issued_stocks`) 
                                      VALUES
                                        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
                    cursor.execute(sql, (company_id, key, insert[0], insert[1], insert[2], insert[3], insert[4], insert[5], insert[6], insert[7], insert[8], insert[9], insert[10], insert[11], insert[12], insert[13], insert[14], insert[15], insert[16], insert[17], insert[18], insert[19], insert[20], insert[21], insert[22], insert[23], insert[24], insert[25], insert[26]))


                except Exception as e:
                    print(e)
                    connection.close()
            print(company_id, 'completed', comp_count)

            connection.commit()

    connection.close()
# -

company_code

# + jupyter={"outputs_hidden": true}
browser = Chrome()
browser.get(f'https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd=015760&target=finsum_more')
browser.find_elements_by_xpath('''//*[@id="cns_Tab21"]''')[0].click()

html = browser.page_source
soup = BeautifulSoup(html, 'html.parser')
financial_data = soup.find('table', {'class' : 'gHead01 all-width', 'summary' : '주요재무정보를 제공합니다.'})
print(financial_data)
# -

data_list =financial_data.select('thead>tr')[1]
year_list = data_list.find_all('th')
this_year = str(datetime.now().year-1)
year_dict = {}
for date in year_list:
    year = date.text.strip()[:4]
    year_dict[year] = []
    if(year == this_year):
        break
print(year_dict)

# + jupyter={"outputs_hidden": true}
#fd = financial data
fd_data = financial_data.select('tbody>tr')
fd_name = []
fd_num = []
fd_name_eng = ['sales', 'operating_income', '-', '-', '-', 'net_income', '-',  'asset', 'liability', '-', 'capital', '-', 'capital_stock', 'operating_cashflow', 'investment_cashflow', 'financial_cashflow', 'capex', 'fcf', 'Interest_liability', 'operating_margin', 'net_margin', 'roe', 'roa', 'debt_ratio', 'capital_reserve_rate', 'eps', 'per', 'bps', 'pbr', 'dps', 'dividend_yield', 'dividend_payout', 'issued_stocks']
for fd in fd_data:
    fd_name.append(fd.find('th').text.strip())
    
    
for fd, name in zip(fd_data, fd_name):
    fd_num_list = fd.find_all('td')
    
    for idx,year in enumerate(year_dict.keys()):
        
        #print(year, name, fd_num_list[idx].text)
        num = (fd_num_list[idx].text.strip()).replace(',', '')
        year_dict[year].append(num)
        #fd_num.append(fd_n)
    


for key in year_dict.keys():
    
    dic = year_dict[key]
    print(key)    
    for idx, name, num in zip(range(len(fd_name)),fd_name, dic):
        print(idx+1, name, num)

# +
import pandas as pd

name_list = {}
for name, eng_name in zip(fd_name, fd_name_eng):
    if(eng_name == '-'):
        continue
    else:
        name_list[name] = eng_name

# -

# %store name_list


import winsound
winsound.Beep(500, 200)


