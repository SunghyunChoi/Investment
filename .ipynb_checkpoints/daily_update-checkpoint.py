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

# +
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
import time
import urllib.request #
from selenium.webdriver import Chrome
import json

pymysql.converters.encoders[np.float64] = pymysql.converters.escape_float
pymysql.converters.conversions = pymysql.converters.encoders.copy()
pymysql.converters.conversions.update(pymysql.converters.decoders)

fdr.__version__


# -

# ## 회사 정보 업데이트

# +
def connect():
    connection = pymysql.connect(host='localhost',
                             user='investment',
                             password='1111',
                             db='investment',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
    return connection
        
#####################################
######## Update Company Data ########
#####################################
def daily_company_update():

    #회사정보 조회
    df_kospi = fdr.StockListing('KOSPI')
    updated_companies = []
    update_count = 0
    
    #데이터베이스 연결
    connection = connect()

    # Company Data Update
    try:
        with connection.cursor() as cursor:
            
            for idx, row in df_kospi.iterrows():
                
                name = row[1]
                code = str(row[0])
                sector = str(row[2])
                industry = str(row[3])
                if('\'' in industry):
                    industry = industry.replace('\'', '\"')
 
                test_sql = f"SELECT code FROM `company` WHERE code = '{code}'"
                cursor.execute(test_sql)
                exists_ = cursor.fetchone()
                
                if(exists_):
                    continue
                else:
                    sql = f'''INSERT INTO `company` (`name`, `code`, `sector`, `industry`) 
                            VALUES ('{name}', '{code}', '{sector}', '{industry}');'''
                    update_count += 1
                    updated_companies.append(sql)
                
                cursor.execute(sql)
                #print(cursor.fetchone())

        connection.commit()

    finally:
        connection.close()
        print(f"{update_count} companies updated, total : {df_kospi.shape[0]}")
        print(updated_companies)
        


# -

# ## 한국 주식 가격 업데이트

# +
#####################################
## Update Daily Company Price Data ## 
#####################################
#업데이트 시간 체크 여부도 확인해야 할 듯

def daily_price_update(start_date = None, end_date = None):
    
    #record_date
    timezone = pytz.timezone('Asia/Seoul')
    today = datetime.now(tz = timezone).date()
    comp_count = 0
    
    #Check if input format is valid
    if(start_date == None):
        print(f"Missing start_date : Loading Data From {today}")
        start_date = today
    else : 
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
        except :
            print(f"Not Valid Input Type : start_date {start_date}")
            return 0

    if(end_date == None):
        print(f"Missing end_date : Loading All Data till {today}")
        end_date = today
    else : 
        try:
            datetime.strptime(end_date, "%Y-%m-%d")
        except : 
            print(f"Not Valid Input Type : end_date {end_date}")
            return 0
    
    #데이터베이스 연결
    connection = connect()
    
    try:
        with connection.cursor() as cursor:

            read_company_data = f"SELECT company_id,code FROM company;"
            cursor.execute(read_company_data)

            results = cursor.fetchall()
            error_sql = []
            
            for result in results: 

                comp_code, company_id = result['code'], result['company_id']
                comp_count += 1
                if(comp_count < 360):
                    continue
                df_daily = fdr.DataReader(comp_code, start_date, end_date)
                daily_sales = get_daily_sales(comp_code,company_id, start_date, end_date)
                if(len(daily_sales.keys()) == 0):
                    continue
                #print(f"comp_code : {comp_code}, comp_count = {comp_count}, daily_sales : {daily_sales.keys()}")
                for idx, row in df_daily.iterrows():
                #for idx in df_daily.index :
                    
                    #print(comp_count)
                    date = idx.date()
                    record_date = datetime.strftime(date, "%Y-%m-%d")
                    starting_price = row['Open']
                    closing_price = row['Close']
                    exchange_rate = round(row['Change'],5) if not np.isnan(row['Change']) else 0
                    volume = row['Volume']
                    try:
                        organ = daily_sales[date][0]
                        foreign = daily_sales[date][1]
                        foreign_ratio = daily_sales[date][2]
                        
                    except Exception as E:
                        print(f"No ratio date for {date}")
                        print(E)
                    try :
                        sql = f'''INSERT INTO `companydailystockdata` 
                                (`company_id`, `record_date`, `starting_price`, `closing_price`, `exchange_rate`, `volume`, `organ`, `foreign`, `foreign_ratio`) 
                              VALUES 
                                (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                              ON DUPLICATE KEY UPDATE 
                                 starting_price = %s, closing_price = %s, exchange_rate = %s, volume = %s, organ = %s, foreign_ratio = %s;'''
                        
                        ################FOREIGN DUPLICATE KEY UPDATE가 안됨!!!!!!!!################
                        #print((company_id, record_date, starting_price, closing_price, exchange_rate, volume, organ, foreign, foreign_ratio, starting_price, closing_price, exchange_rate, volume, organ, foreign, foreign_ratio))
                        #print(sql, (company_id, record_date, starting_price, closing_price, exchange_rate, volume, organ, foreign, foreign_ratio, starting_price, closing_price, exchange_rate, volume, organ, foreign, foreign_ratio))
                        cursor.execute(sql,(company_id, record_date, starting_price, closing_price, exchange_rate, volume, organ, foreign, foreign_ratio, starting_price, closing_price, exchange_rate, volume, organ, foreign_ratio))
                        connection.commit()
                    except Exception as e:
                        print(e)
                        error_sql.append(sql)
                        continue

    finally :
        print(f"{comp_count} numbers of company price data from {start_date} - {end_date} updated")
    
    connection.close()


## companydailystockdata - organ, foreign, foreign ratio update
def get_daily_sales(company_code, company_id, start_date, end_date):
    
    answer = {}
    response = requests.get(f"https://finance.naver.com/item/frgn.nhn?code={company_code}")
    html = response.content
    soup = BeautifulSoup(html, 'html.parser')
    data_list = soup.find_all(name="tr", attrs = {'onmouseover' : 'mouseOver(this)'})
    list_start = 1
    list_end = soup.find(name = 'td', attrs = {'class':'pgRR'})
    list_end = list_end.find(name = 'a', href = True)
    list_end = int(list_end['href'].split('=')[-1])
    
    #print(f"https://finance.naver.com/item/frgn.nhn?code={company_code}")
    
    #No data before this date(on Naver)
    date_end = datetime.strptime('2005-01-03', '%Y-%m-%d').date()
    
    if(type(start_date) == str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    if(type(end_date) == str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    if((start_date - date_end).days < 0):
        start_date = date_end

    for page in range(list_start, list_end + 1):

        response = requests.get(f"https://finance.naver.com/item/frgn.nhn?code={company_code}&page={page}")
        html = response.content
        soup = BeautifulSoup(html, 'html.parser')
        data_list = soup.find_all(name="tr", attrs = {'onmouseover' : 'mouseOver(this)'})
        
        for data in data_list : 
            try:
                record_date = datetime.strptime(data.find(name = 'td', attrs = {'class' : 'tc'}).text,'%Y.%m.%d').date()
                organ_foreign = data.find_all(name = 'td', attrs = {'class' : 'num'})
                organ = int(organ_foreign[4].text.replace(',', ''))
                foreign = int(organ_foreign[5].text.replace(',', ''))
                foreign_ratio = round(float(organ_foreign[7].text[:-1]), 5)
                
            except Exception as e: 
                print(e)
                try:
                    keys = answer.keys()
                    #print(f"finance_naver data from {min(keys)} to {max(keys)} is loaded")
                except:
                    print(1)
                    #print(f"No finance_naver For the Period : {start_date} - {end_date}")

                return answer

            if(((record_date - start_date).days>0) and (record_date - end_date).days<=0 ):
                    answer[record_date] = [organ, foreign, foreign_ratio]
                    continue
            elif((record_date - start_date).days <= 0) :
                answer[record_date] = [organ, foreign, foreign_ratio]
                return answer
            else: 
                print(record_date, start_date, end_date)
                print((record_date - start_date).days)
                print((record_date - end_date).days)
                print('what happend?')
                continue

        if(page == list_end):
            return answer


# -

# ## 시장지표 업데이트

# +
###########################################
####### Update Daily Finance Data #########
###########################################
## Get Oil Data ##
def get_oil_data(start_date = None, end_date = None):
    
    oil_dbi_data = {}
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    end_limit = datetime.strptime('2006-04-18', "%Y-%m-%d").date()
        
    if((end_date - end_limit).days < 0):
        print("No Oil Data Found for the period")
        return []
    if((start_date - end_limit).days < 0):
        start_date = end_limit
    
    for page in range(0,1000):   
        
        response = requests.get(f"https://finance.naver.com/marketindex/worldDailyQuote.nhn?marketindexCd=OIL_CL&fdtc=2&page={page}")
        html = response.content
        soup = BeautifulSoup(html, 'html.parser')
        data_list = soup.find_all(name="tr")
        time.sleep(0.5)
        if(response.status_code == 404):
            print("Oil Data Page Not Working")
        
        num = 0
        for data in data_list:
            
            date_tag = data.find(name = 'td', attrs = {'class' : 'date'})
            if(not date_tag):
                continue

            tags = data.find_all(name = 'td', attrs = {'class' : 'num'})
            price_tag = tags[0]
            exchange_rate_tag = tags[2]

            date = datetime.strptime(date_tag.text.strip().replace('.', '-'), "%Y-%m-%d").date()
            if(((date - start_date).days>0) and (date - end_date).days<=0 ): 
                price = float(price_tag.text.strip())
                exchange_rate = round((float(exchange_rate_tag.text.strip()[:-1]))/100, 5)
                oil_dbi_data[date] = [price, exchange_rate]
                #print(date, start_date, end_date, 'input data')
                
            elif((date - start_date).days == 0) :
                #print(date, start_date, end_date, 'return data')
                price = float(price_tag.text.strip())
                exchange_rate = round((float(exchange_rate_tag.text.strip()[:-1]))/100, 5)
                oil_dbi_data[date] = [price, exchange_rate]
                keys = oil_dbi_data.keys()
                print(f"OIL_DBI data from {min(keys)} to {max(keys)} is loaded")
                return oil_dbi_data

                
            elif ((date - start_date).days < 0) : 
                #print(date, start_date, end_date, 'skip')
                try:
                    keys = oil_dbi_data.keys()
                    print(f"OIL_DBI data from {min(keys)} to {max(keys)} is loaded")
                except:
                    print(f"No OIL_DBI Data For the Period : {start_date} - {end_date}")
                        
                return oil_dbi_data
                continue
                
            else :
                continue

    

## GET DATA FROM FinanceDataReader
def get_data(name, start_date = None, end_date = None):
    
    fdr_data = ['KS11', 'IXIC', 'CSI300', 'USD/KRW', 'BTC/KRW']
    pdr_data = ['GOLDAMGBD228NLBM']
    crawling_data = ['OIL_DBI']
    data = {}
    price = 0
    exchange_rate = 0
    
    if(name in fdr_data):
    
        
        try:
            #fdr.DataReader('Index', start_date, end_date = default today)
            df = fdr.DataReader(name, start_date, end_date)
        except :
            print(f"DataReading Error : No {name} Data Found for the period")
            return data
        
        for idx, row in df.iterrows():

            date = idx.date()
            price = row['Close']
            exchange_rate = round(row['Change'],5)

            data[date] = [price, exchange_rate]
        
        keys = data.keys()
        print(f"{name} data from {min(keys)} to {max(keys)} is loaded")
        return data

    elif(name in pdr_data):
        try:
            #fdr.DataReader('Index', start_date, end_date = default today)
            yesterday = datetime.strptime(start_date, '%Y-%m-%d').date() - timedelta(days=1)
            yesterday_str = datetime.strftime(yesterday, '%Y-%m-%d')
            df = pdr.DataReader(name, 'fred', yesterday_str, end_date)
        except :
            print(f"DataReadingError : No {name} Data Found for the period")
            return data
        
        try:
            yesterday_data = df.loc[yesterday_str][name]
            df = df.drop(yesterday, 0)
        except : 
            yesterday_data = 0
        
        for idx, row in df.iterrows():
            
            date = idx.date()
            price = row[name] if not np.isnan(row[name]) else 0
            exchange_rate = round((price - yesterday_data) / price, 4) if not (np.isnan(price) == True or price == 0) else 0

            data[date] = [price, exchange_rate]
            yesterday_data = price
        try:
            keys = data.keys()
            print(f"{name} data from {min(keys)} to {max(keys)} is loaded")
        except:
            print(f"No {name} Data For the Period : {start_date} - {end_date}")
        return data
    
    elif(name in crawling_data):
        #try:
        data = get_oil_data(start_date, end_date)
        return data
        #except : 
        #    print(f"DataReadingError : No {name} Data Found for the period")
        #   return data
    else :
        print("No Such Data")
        return 0
    
def return_data(df, date):
    
    try:
        price = df[date][0]
        exchange_rate = df[date][1]
    except:
        price = None
        exchange_rate = None
    
    return price, exchange_rate

def daily_finance_data(start_date = None, end_date = None):
    
    #record_date
    timezone = pytz.timezone('Asia/Seoul')
    today = datetime.strftime(datetime.now(tz = timezone).date(), "%Y-%m-%d")
    
    #Check if input format is valid
    if(start_date == None):
        print(f"Missing start_date : Loading Data From {today}")
        start_date = today
    else : 
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
        except :
            print(f"Not Valid Input Type : start_date {start_date}")
            return 0

    if(end_date == None):
        print(f"Missing end_date : Loading All Data till {today}")
        end_date = today
    else : 
        try:
            datetime.strptime(end_date, "%Y-%m-%d")
        except : 
            print(f"Not Valid Input Type : end_date {end_date}")
            return 0

    #KOSPI
    kospi_data = get_data('KS11', start_date, end_date)    
    #NASDAQ
    nasdaq_data = get_data('IXIC', start_date, end_date)
    #CSI 300
    csi300_data = get_data('CSI300', start_date, end_date)
    #USD_KRW
    usd_krw_data = get_data('USD/KRW', start_date, end_date)
    #GOLD
    gold_data = get_data('GOLDAMGBD228NLBM', start_date, end_date)
    #OIL_WTI
    oil_dbi_data = get_data('OIL_DBI', start_date, end_date)
    #BTC/KRW
    bitcoin_data = get_data('BTC/KRW', start_date, end_date)
    df_total = [kospi_data, nasdaq_data, csi300_data, usd_krw_data, gold_data, oil_dbi_data, bitcoin_data]
    
    #Making Data Range to iterate

    date_set = []
    min_range = []
    max_range = []
    
    for df in df_total:

        date_range = df.keys()
        try:
            start_date = min(date_range)
            end_date = max(date_range)
        except :
            print("there is some missing data")
            continue
        min_range.append(start_date)
        max_range.append(end_date)
        
    min_start = min(min_range)
    max_end = max(max_range)
    
    while min_start <= max_end:
        date_set.append(min_start)
        min_start += relativedelta(days=1)
    
    kospi_price, kospi_exchange_rate = 0,0
    nasdaq_price, nasdaq_exchange_rate = 0,0
    csi300_price,csi300_exchange_rate = 0,0
    usd_krw_price, usd_krw_exchange_rate = 0,0
    gold_price, gold_exchange_rate = 0,0
    oil_dbi_price, oil_dbi_exchange_rate = 0,0
    bitcoin_price, bitcoin_exchange_rate = 0,0
    
    #데이터베이스 연결
    connection = connect()
    for date in date_set:
    
        record_date = datetime.strftime(date, "%Y-%m-%d")

        kospi_price, kospi_exchange_rate = return_data(kospi_data, date)
        nasdaq_price, nasdaq_exchange_rate = return_data(nasdaq_data, date)
        csi300_price,csi300_exchange_rate = return_data(csi300_data, date)
        usd_krw_price, usd_krw_exchange_rate = return_data(usd_krw_data, date)
        gold_price, gold_exchange_rate = return_data(gold_data, date)
        oil_dbi_price, oil_dbi_exchange_rate = return_data(oil_dbi_data, date)
        bitcoin_price, bitcoin_exchange_rate = return_data(bitcoin_data, date)
        #try:
        with connection.cursor() as cursor:

            query='''INSERT INTO `dailyfinancedata` (`record_date`, `kospi_price`, `kospi_exchange_rate`, `nasdaq_price`, `nasdaq_exchange_rate`, `csi300_price`, `csi300_exchange_rate`, `usd_krw_price`, `usd_krw_exchange_rate`, `gold_price`, `gold_exchange_rate`, `oil_dbi_price`, `oil_dbi_exchange_rate`, `bitcoin_price`, `bitcoin_exchange_rate`) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE 
                         kospi_price =%s, kospi_exchange_rate = %s, nasdaq_price = %s, nasdaq_exchange_rate = %s, csi300_price = %s, csi300_exchange_rate = %s, usd_krw_price = %s, usd_krw_exchange_rate = %s, gold_price = %s, gold_exchange_rate = %s, oil_dbi_price = %s, oil_dbi_exchange_rate = %s, bitcoin_price = %s, bitcoin_exchange_rate = %s;
                    '''
            #print(query, (record_date, kospi_price, kospi_exchange_rate, nasdaq_price, nasdaq_exchange_rate, csi300_price, csi300_exchange_rate, usd_krw_price, usd_krw_exchange_rate, gold_price, gold_exchange_rate, oil_dbi_price, oil_dbi_exchange_rate, bitcoin_price, bitcoin_exchange_rate, kospi_price, kospi_exchange_rate, nasdaq_price, nasdaq_exchange_rate, csi300_price, csi300_exchange_rate, usd_krw_price, usd_krw_exchange_rate, gold_price, gold_exchange_rate, oil_dbi_price, oil_dbi_exchange_rate, bitcoin_price, bitcoin_exchange_rate))
            cursor.execute(query, (record_date, kospi_price, kospi_exchange_rate, nasdaq_price, nasdaq_exchange_rate, csi300_price, csi300_exchange_rate, usd_krw_price, usd_krw_exchange_rate, gold_price, gold_exchange_rate, oil_dbi_price, oil_dbi_exchange_rate, bitcoin_price, bitcoin_exchange_rate, kospi_price, kospi_exchange_rate, nasdaq_price, nasdaq_exchange_rate, csi300_price, csi300_exchange_rate, usd_krw_price, usd_krw_exchange_rate, gold_price, gold_exchange_rate, oil_dbi_price, oil_dbi_exchange_rate, bitcoin_price, bitcoin_exchange_rate))

        
    connection.commit()
    connection.close()
    print(f"Data From {min(min_range)} - {max(max_range)} updated")


    
    
#####################################    
#####Update Financial_Statement######
#####################################
def financial_statement(record_year):
    connection = connect()
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

# ### 매일 실행

# + jupyter={"outputs_hidden": true}
# 한국거래소 상장종목 전체
# StockListing : 'KRX', 'KOSPI', 'KOSDAQ', 'KONEX', 'NASDAQ', 'NYSE', 'AMEX', 'SP500'

#daily_company_update()
daily_price_update('2020-05-28')
#daily_finance_data('2020-05-20')
# -

soup

a = get_data('OIL_DBI', '2020-04-05', '2020-04-08')
a
