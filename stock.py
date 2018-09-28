#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 19 23:18:03 2018

@author: litian
"""

from jqdatasdk import *
import pandas as pd
import datetime
import requests
from bs4 import BeautifulSoup
import numpy as np
import matplotlib.pyplot as plt
import pickle
import random


def acc_hold_update(start_date, end_date, account_holding_df, Transaction_df = None, price_offline_df = None, t_days = None, bonus_dict = None):
    """
        Task:
            Update the account holding table
        Parameters:
            start_date:
                define the start date of account info to be updated
            end_date:
                define the end date of account info to be updated
            Transaction_df:
                the dataframe including all transaction records
                columns = ["Account_ID", "Date", "Type", "Stock_Code",  "Number", "Price"]
            account_holding_df:
                the origin account holding dataframe
                columns = ["Date", "Account_ID",  "Stock_Code", "Number", "Cost", "Value", "Position", "MaxProfit", "InDate"]
        Return:
            a new dataframe of the updated account holdings info
    """
    
   
    ## transform the startdate, endate to the datetime type
    datestart = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    early_date = datestart - datetime.timedelta(days=20)
    early_date_str = early_date.strftime('%Y-%m-%d')
    

    ## create a new account holding dataframe for funtion returning
    account_holding_new = account_holding_df.copy()
    
    if t_days is None:
        ## get trade days with jqdatasdk
        trade_days = get_trade_days(early_date_str, end_date)
    else:
        start_index = np.argwhere(np.array(t_days) == datestart.date())[0][0]
        end_index = np.argwhere(np.array(t_days) == datetime.datetime.strptime(end_date, "%Y-%m-%d").date())[0][0]
        trade_days = t_days[start_index:(end_index+1)]
    
    ## if the trasaction dataframe is none, create an empty dataframe
    if Transaction_df is None:
        Transaction_df = pd.DataFrame(columns = ["Account_ID", "Date", "Type", "Stock_Code",  "Number", "Price"])

    ## collect all stock_code from account_holding_df and Transaction_df
    set1 = set(account_holding_df['Stock_Code'].value_counts().index)
    set2 = set(Transaction_df['Stock_Code'].value_counts().index)
    code_set = set1 | set2
    code_l = list(code_set)


    
    if len(account_holding_new)>0:
        ## get the last date in the account holding dataframe
        acc_last_date = datetime.datetime.strptime(account_holding_new.iloc[-1, 0], "%Y-%m-%d").date()
    else:
        acc_last_date = datetime.datetime.strptime("1900-01-01", "%Y-%m-%d").date()
    
    ## for loop, every trade day's holdings info will be updated
    for j,datecurrent in enumerate(trade_days):
        
        ## data type transformtion, get the rows of the day before datecurrent 
        today_str = datecurrent.strftime('%Y-%m-%d')
        
        if datecurrent > acc_last_date:
            ## at datestart to update every trade day
            if datecurrent >= datestart.date():
                
                if j > 0:
                    ## get the day before current day
                    yesterday = trade_days[j-1]
                    yesterday_str = yesterday.strftime('%Y-%m-%d')
                    
                    ## get the data of yesterday
                    yesterday_df = account_holding_new[account_holding_new['Date'] == yesterday_str]
                else:
                    yesterday_df = account_holding_new[account_holding_new['Date'] == acc_last_date.strftime('%Y-%m-%d')]
    
                ## initialize a variable 'bonus_cash'
                bonus_cash = 0        
    
                ## initialize money_record_index
                money_record_index = None       
                
                
                ## copy the yesterday's rows to datecurrent
                for i in yesterday_df.index:
                    
                    account_holding_new = account_holding_new.append(yesterday_df.loc[i], ignore_index = True)
                    
                    # update the date 
                    account_holding_new.iloc[-1, 0] = today_str                
                    
                    ## update the value of stock with the updated close price
                    code_str = account_holding_new.iloc[-1, 2]
                    
    
                    ## get the bonus dataframe from the dictionary
                    if code_str in bonus_dict.keys():
                        bonus_df = bonus_dict[code_str]    
                    else:
                        bonus_df = pd.DataFrame()
                   
                  
                    ## update the value with the close price
                    if code_str != '000000.RMB':
                        if price_offline_df is None:
                            stock_price = get_price(code_str, start_date = today_str, end_date = today_str, fq = None)
                        else:
                            stock_price = get_price_local(code_str, today_str, today_str, price_offline_df)
    
                        if not np.isnan(stock_price.loc[today_str, 'close']):
                            new_value = stock_price.loc[today_str, 'close']
                            ## update "Value"
                            account_holding_new.iloc[-1, 5] = new_value
                            ## update MaxProfit 
                            if ((new_value - account_holding_new.iloc[-1, 4])/account_holding_new.iloc[-1, 4]) > account_holding_new.iloc[-1, 7]:
                                account_holding_new.iloc[-1, 7] = (new_value - account_holding_new.iloc[-1, 4])/account_holding_new.iloc[-1, 4]
                         
                    ## if this record is money record, remember the index of this record
                    else:
                        money_record_index = account_holding_new.index[-1]
                
                ## update the money record's cost, value with bonus_cash
                if money_record_index is not None:
                    account_holding_new.loc[money_record_index, 'Cost'] += bonus_cash
                    account_holding_new.loc[money_record_index, 'Value'] += bonus_cash
                    
        ## select the transaction info rows of datecurrent
        t_df = Transaction_df[Transaction_df['Date'] == today_str]
        
       
        ## for each transaction on current day
        for i in t_df.index:
            
           
            ## get the information of transaction: ID, code, number, price, operation type
            ID = t_df.loc[i, 'Account_ID']
            code = t_df.loc[i, 'Stock_Code']
            num = t_df.loc[i, 'Number']
            price = t_df.loc[i, 'Price']
            op = t_df.loc[i, 'Type']
            
            ## the criterion to find the stock record in accounting holdings table
            criterion = ((account_holding_new['Date'] == today_str) &
                        (account_holding_new['Account_ID'] == ID) &
                        (account_holding_new['Stock_Code'] == code))
            
            ## the criterion to find the money record in accounting holdings table
            criterion_m = ((account_holding_new['Date'] == today_str) &
                           (account_holding_new['Account_ID'] == ID) &
                        (account_holding_new['Stock_Code'] == '000000.RMB'))
            
            ## index the sub-dataframe for the stock record
            result = account_holding_new[criterion]
            
            ## index the sub-dataframe for the money record
            result_m = account_holding_new[criterion_m]


            
            ## if the stock_code of the trasaction exist in the account holdings table        
            if len(result) > 0:
    
                ## get the number, cost of the stock in the account 
                old_num = account_holding_new.loc[criterion, 'Number'].iloc[0]
                old_cost = account_holding_new.loc[criterion, 'Cost'].iloc[0]
                old_value = account_holding_new.loc[criterion, 'Value'].iloc[0]
                
                ## get the number of money in the account
                old_cost_m = account_holding_new.loc[criterion_m, 'Cost'].iloc[0]
                old_value_m = account_holding_new.loc[criterion_m, 'Value'].iloc[0]
                
                ## if the operation is on money
                if code == '000000.RMB':
                    
                    ## the criterion to find all record in accounting holdings table
                    criterion_acc = ((account_holding_new['Date'] == today_str) & \
                                     (account_holding_new['Account_ID'] == ID))
                    
                    result_acc = account_holding_new[criterion_acc]
                    
                    account_cost = sum(result_acc['Number'] * result_acc['Cost'])
                    account_value = sum(result_acc['Number'] * result_acc['Value'])
                    
                    stock_cost = account_cost - old_cost
                    stock_value = account_value - old_value
                    return_ratio = account_value/account_cost

                    ## if the transaction is money in
                    if op == 'Buy':
                        ## increase the number of money in the account
                        account_holding_new.loc[criterion, 'Cost'] = (((old_value + price)+stock_value)/return_ratio) - stock_cost
                        account_holding_new.loc[criterion, 'Value'] = old_value + price
                        account_holding_new.loc[criterion, 'InDate'] = today_str
                        
                    ## if the transacion is money out
                    else:
                        ## decrease the number of money in the account, can be less than zero
                        account_holding_new.loc[criterion, 'Cost'] = ((old_value - price)+stock_value)/return_ratio - stock_cost
                        account_holding_new.loc[criterion, 'Value'] = old_value - price

    
                ## if the operation is on stock
                else:
                    ## if buy stock
                    if op == 'Buy':

                        
                        ## increase the number of stock in the account
                        account_holding_new.loc[criterion, 'Number'] = old_num + num
                        ## update the cost of stock in the account
                        new_cost = (old_num * old_cost + num * price) / (old_num + num)
                        account_holding_new.loc[criterion, 'Cost'] = new_cost
                        ## update the number of position of stock in the account
                        account_holding_new.loc[criterion, 'Position'] += 1
                        
                        ## get maxprofit
                        maxprofit = account_holding_new.loc[criterion, 'MaxProfit'].iloc[0]
                        
                        
                        ## update maxprofit
                        old_maxvalue = old_cost * (1 + maxprofit)                        
                        new_maxprofit = (old_maxvalue - new_cost) / new_cost                        
                        account_holding_new.loc[criterion, 'MaxProfit'] = new_maxprofit
                        
                        ## update InDate
                        account_holding_new.loc[criterion, 'InDate'] = today_str
                        
                        ## update the money in the account
                        account_holding_new.loc[criterion_m, 'Cost'] = old_cost_m - num * price
                        account_holding_new.loc[criterion_m, 'Value'] = old_value_m - num * price
 
                        ## debug
##                        print(op + code)

                       
    
                    ## if sell stock
                    else:

                        ## if the number of stock sold greater than the stock in the account, sell all of the stock
                        if num >= old_num:
                            ## update the money in the account
                            account_holding_new.loc[criterion_m, 'Cost'] = old_cost_m + old_num * old_cost
                            account_holding_new.loc[criterion_m, 'Value'] = old_value_m + old_num * price
                            
                            ## drop the stock record in the account 
                            account_holding_new = account_holding_new.drop(account_holding_new.loc[criterion].index)
                            account_holding_new = account_holding_new.reset_index(drop=True)
                        
                        ## if the number of stock sold less than the stock in the account
                        else:
                            ## decrease the number of stock in the account
                            account_holding_new.loc[criterion, 'Number'] = old_num - num                            
                            
                            ## update the cost of stock in the account
                            new_cost = (old_num * old_cost - num * price) / (old_num - num)
                            account_holding_new.loc[criterion, 'Cost'] = new_cost
                            ## update the number of position of stock in the account
                            account_holding_new.loc[criterion, 'Position'] -= 1
                            
                            ## update maxprofit
                            old_maxvalue = old_cost * (1 + maxprofit)                        
                            new_maxprofit = (old_maxvalue - new_cost) / new_cost                        
                            account_holding_new.loc[criterion, 'MaxProfit'] = new_maxprofit
                           
                            ## increase the money in the account
                            account_holding_new.loc[criterion_m, 'Cost'] = old_cost_m + old_num * price
                            account_holding_new.loc[criterion_m, 'Value'] = old_value_m + old_num * price

                        ## debug
##                        print(op + code)


            
            ## if the stock_code of the trasaction is a new code   
            else:

                
                ## if the operation is transaction on money
                if code == '000000.RMB':


                    ## if money in 
                    if op == 'Buy':
                        ## create a new money record in the account
                        new_row = pd.DataFrame([[today_str, ID, code, 1, price, price, 1, 0, today_str]], 
                                               columns = ["Date", "Account_ID",  "Stock_Code", "Number", "Cost", "Value", "Position", "MaxProfit", "InDate"])
                        account_holding_new = account_holding_new.append(new_row, ignore_index = True)
                        
                    ## if money out, do nothing
                        


                ## if the operation is on stock and money record exist in the account
                elif len(result_m) > 0:


                    
                    ## get the number of money in the account
                    old_cost_m = account_holding_new.loc[criterion_m, 'Cost'].iloc[0]
                    old_value_m = account_holding_new.loc[criterion_m, 'Value'].iloc[0]
                    ## if buy stock
                    if op == 'Buy':

                        ## get the close price of the stock
                        if price_offline_df is None:
                            stock_price = get_price(code, start_date = today_str, end_date = today_str, fq = None)
                        else:
                            stock_price = get_price_local(code, today_str, today_str, price_offline_df)

                        ## decrease the money in the account
                        account_holding_new.loc[criterion_m, 'Cost'] = old_cost_m - num * price
                        account_holding_new.loc[criterion_m, 'Value'] = old_value_m - num * price
                        
                        ## create a new stock record in the account
                        new_row = pd.DataFrame([[today_str, ID, code, num, price, stock_price.loc[today_str, 'close'], 1, ((stock_price.loc[today_str, 'close'] - price)/price), today_str]],
                                               columns = ["Date", "Account_ID",  "Stock_Code", "Number", "Cost", "Value", "Position", "MaxProfit", "InDate"])
                        account_holding_new = account_holding_new.append(new_row, ignore_index = True)

                    ## debug
##                    print(op + code)

    
    ## make sure that Number, Cost and Value are numeric
    account_holding_new['Number'] = pd.to_numeric(account_holding_new['Number'])
    account_holding_new['Cost'] = pd.to_numeric(account_holding_new['Cost'])
    account_holding_new['Value'] = pd.to_numeric(account_holding_new['Value'])
                    
    return account_holding_new


def net_value_cal(account_holding_df, start_date = None, account_value_df = None, t_days = None):
    """
        Task:
            Update the account net value table
        Parameters:
            account_holding_df:
                the origin account holding dataframe
                columns = ["Date", "Account_ID",  "Stock_Code", "Number", "Cost", "Value", "Position", "MaxProfit", "InDate"]
            start_date:
                define the start date of account info to be updated
            account_value_df:
                the dataframe including all account net value
                columns = ["Date", "Account_ID", "Cost", "Value"]
        Return:
            a new dataframe of the updated account net value table
    """    
    ## if start_date is None, set start_date to an impossible date
    if start_date is None:
        start_date = '2081-10-25'
        
    ## transform the startdate to the datetime type
    datestart = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    
    ## get the date of the first record in the account_holding_df, if fails throw an error
    try:
        
        daterecord = datetime.datetime.strptime(account_holding_df.iloc[0, 0], "%Y-%m-%d")
        
    except TypeError:
        print('account_holding_df should be a dataframe with columns ["Date", "Account_ID",  "Stock_Code", "Number", "Cost", "Value", "Position", "MaxProfit", "InDate"]')
    
    ## if no account_value_df or start_date is too early
    if (account_value_df is None) | (datestart < daterecord):
        
        ## create a new account_value_df
        account_value_updated_df = pd.DataFrame(columns = ["Date", "Account_ID", "Cost", "Value"])
        
        ## update the new df with all records in the account_holding_df
        ## from the first record
        if start_date == '2081-10-25':
            datestart = daterecord

      
    ## if the start_date is None
    elif start_date == '2081-10-25':
        ## copy account_value_df to a new dataframe
        account_value_updated_df = account_value_df.copy()
        
        ## get the date of the last record in the account_value_df
        try:
            daterecord = datetime.datetime.strptime(account_value_df.iloc[-1, 0], "%Y-%m-%d")
        except TypeError:
            print('account_value_df should be a dataframe with columns ["Date", "Account_ID", "Cost", "Value"]')
        
        ## set the datestart to the daterecord + 1
        datestart = daterecord + datetime.timedelta(days=1)

    
    else:
         ## copy account_value_df to a new dataframe
        account_value_updated_df = account_value_df.copy() ### need to be modified

    ## set the dateend to the last record in the account_holding_df
    dateend = datetime.datetime.strptime(account_holding_df.iloc[-1, 0], "%Y-%m-%d")

    if t_days is None:    
        ## get trade days with jqdatasdk
        trade_days = get_trade_days(datestart, dateend)
    else:
        start_index = np.argwhere(np.array(t_days) == datestart.date())[0][0]
        end_index = np.argwhere(np.array(t_days) == dateend.date())[0][0]
        trade_days = t_days[start_index:(end_index+1)]
    
     ## for loop, every trade day's holdings info will be updated
    for j,datecurrent in enumerate(trade_days):
        
        ## data type transformtion 
        today_str = datecurrent.strftime('%Y-%m-%d')
                   
        ## get the current day's record from account_holding_df
        current_holding_df = account_holding_df[account_holding_df['Date'] == today_str]
        
        ## for every record
        for i in current_holding_df.index:
            
            ## get the information in the record
            account_id = current_holding_df.loc[i, 'Account_ID']
            number = current_holding_df.loc[i, 'Number']
            cost = current_holding_df.loc[i, 'Cost']
            value = current_holding_df.loc[i, 'Value']

            ## the criterion to find the stock record in account value table
            criterion = ((account_value_updated_df['Date'] == today_str) &
                        (account_value_updated_df['Account_ID'] == account_id))
            
            ## index the sub-dataframe for the stock record
            result = account_value_updated_df[criterion]

            ## if find the account & currentdate in the table
            if len(result) > 0:
                
                ## update the record in the account value table
                old_cost = account_value_updated_df.loc[criterion, 'Cost'].iloc[0]
                old_value = account_value_updated_df.loc[criterion, 'Value'].iloc[0]
                account_value_updated_df.loc[criterion, 'Cost'] = old_cost + number * cost
                account_value_updated_df.loc[criterion, 'Value'] = old_value + number * value
            
            ## if not, create a new row
            else:
                
                new_cost = number * cost
                new_value = number * value
                
                ## create a new money record in the account
                new_row = pd.DataFrame([[today_str, account_id, new_cost, new_value]], 
                                             columns = ["Date", "Account_ID", "Cost", "Value"])
                
                account_value_updated_df = account_value_updated_df.append(new_row, ignore_index = True)
    
    ## make sure that cost and value is numeric
    account_value_updated_df['Cost'] = pd.to_numeric(account_value_updated_df['Cost'] )
    account_value_updated_df['Value'] = pd.to_numeric(account_value_updated_df['Value'] )
    
    return account_value_updated_df


def comp_growth(account_value_df, account_id_l, start_date, end_date, stock_id_l = None):
    """
        Task:
            compare the growth rate between account and stock
        Parameters:
            account_value_df:
                a dataframe of account value information
                columns = ["Date", "Account_ID", "Cost", "Value"]
            account_id_l:
                a list of account ids
                example: ['001.GPZH', '002.GPZH']
            start_date:
                start date
            end_date:
                end date
            stock_id_l:
                a list of stock code, according to jqdata
                example: ['600487.XSHG', '6002222.XSHG']
        Return:
            a dataframe of compared growth rate, very easy to plot
            columns' type:
                index(Date): datetime.date
                'growth1': start from 1
                'growth2': start from 1
                .
                .
                .
    """    
    
    ## get trade days with jqdatasdk
    trade_days = get_trade_days(start_date, end_date)
    
    ## create an empty dataframe with column 'Date'
    comp_df = pd.DataFrame(index = trade_days)
    
    ## create a timeseries dataframe from account_value_df
    ts_av_df = account_value_df.set_index(pd.to_datetime(account_value_df['Date']).dt.date)

    for account_id in account_id_l:
        
        ## filter the account value by account_id
        id_condition = ts_av_df['Account_ID'] == account_id
        id_df = ts_av_df.loc[id_condition, ['Cost','Value']]
        
        ## initialize yesterday's cost, value
        yesterday_cost = -1
        yesterday_value = -1
        yesterday_growth = 1
                
        for index_day in trade_days:
            
            ## if index-day has no record, growth not change
            if index_day not in id_df.index:
                today_growth = yesterday_growth
                
            ## if find a record, update the growth
            else:
                ## get today's cost, value
                today_cost = id_df.loc[index_day,'Cost']
                today_value = id_df.loc[index_day, 'Value']
                                
                ## if the first record, set growth to 1
                if yesterday_cost == -1:
                    today_growth = 1
                    
                ## else, update the growth
                else:
                                        
                    ## calculate today's growth
                    today_increase = (today_value/today_cost)/(yesterday_value/yesterday_cost) - 1
                    today_growth = yesterday_growth * (1 + today_increase)
                
                ## set yesterday's cost, value, growth
                yesterday_cost = today_cost
                yesterday_value = today_value
                yesterday_growth = today_growth                
            
            comp_df.loc[index_day, account_id] = today_growth

    if stock_id_l is not None:
        
        for stock_id in stock_id_l:
            
            ## get the price from jqdata
            stock_df = get_price(stock_id, start_date, end_date, fq = 'post')
            
            ## get the origin value
            origin_value = stock_df.iloc[0, 1]
            
            for index_day in trade_days:
                current_value = stock_df.loc[index_day, 'close'] 
                comp_df.loc[index_day, stock_id] = current_value/origin_value
            
    return comp_df
    

def get_bonus_info(stock_code, site = 'SINA'):
    """
        Task:
            Fetch the bonus information from the specified site
        Parameters:
            stock_code:
                the stock code you want to fetch the bonus information
                example: '600487'
            site:
                now only support data from SINA, will support more sites in the future
        Return:
            a dataframe of bonus information, time series indexed,
            columns' type:
                index(Date): datetime.date
                'B_Shares': int64, 10 shares to bonus shares
                'I_Shares': int64, 10 shares to into shares
                'Cash': int64, 10 shares to cash
                'ED_Date': datetime.date, Ex-Dividend Date
                'RR_Date': datetime.date, equity rights registration date
    """       
    
    bonus_df = pd.DataFrame(columns = ["Date", "B_Shares", "I_Shares",  "Cash", "ED_Date", "RR_Date"])
    
    if site == 'SINA':
        
        ## get the url from stock_code
        url_a = 'http://money.finance.sina.com.cn/corp/go.php/vISSUE_ShareBonus/stockid/'
        url_z = '.phtml'
        url = url_a + stock_code + url_z
        
        ## get the soup
        r = requests.get(url)        
        html_doc = r.text        
        soup = BeautifulSoup(html_doc, "lxml")

        ## get the data tags from the soup    
        bonus = soup.select("#sharebonus_1 > tbody > tr > td")

        ## the number of record        
        record_num = len(bonus)//9
        
        ## parse the data record
        for i in range(0,record_num):
                       
            ## filter the valid record
            if (bonus[i*9].text != '--') & \
                (bonus[i*9 + 1].text != '--') & \
                (bonus[i*9 + 2].text != '--') & \
                (bonus[i*9 + 3].text != '--') & \
                (bonus[i*9 + 5].text != '--') & \
                (bonus[i*9 + 6].text != '--') :
                
                ## update the columns' value
                bonus_df.loc[i, 'Date'] = bonus[i*9].text
                bonus_df.loc[i, 'B_Shares'] = bonus[i*9+1].text
                bonus_df.loc[i, 'I_Shares'] = bonus[i*9+2].text
                bonus_df.loc[i, 'Cash'] = bonus[i*9+3].text
                bonus_df.loc[i, 'ED_Date'] = bonus[i*9+5].text
                bonus_df.loc[i, 'RR_Date'] = bonus[i*9+6].text
        
        if len(bonus_df) > 0:
            

            ## transform the columns to desired type
            bonus_df['Date'] = pd.to_datetime(bonus_df['Date']).dt.date
            bonus_df['ED_Date'] = pd.to_datetime(bonus_df['ED_Date']).dt.date
            bonus_df['RR_Date'] = pd.to_datetime(bonus_df['RR_Date']).dt.date
            bonus_df['B_Shares'] = pd.to_numeric(bonus_df['B_Shares'])
            bonus_df['I_Shares'] = pd.to_numeric(bonus_df['I_Shares'])
            bonus_df['Cash'] = pd.to_numeric(bonus_df['Cash'])
            
    ## time series indexed
    bonus_df = bonus_df.set_index('Date')
    
    ## time ascending sort
    bonus_df = bonus_df.sort_index()
        
    return bonus_df
    

def get_weekly_price(price_df):
    """
        Task:
            transform the daily price dataframe to a weekly price dataframe
        Parameters:
            price_df:
                daily price dataframe, from JQDATA method 'get_price()'
       Return:
           weekly_price_df: 
           a dataframe of weekly price, time series indexed,
            columns' type:
                index(Date): datetime.date
                open      119 non-null float64
                close     119 non-null float64
                high      119 non-null float64
                low       119 non-null float64
                volume    119 non-null float64
                money     119 non-null float64
    """       
    ## create an empty dataframe of weekly price
    weekly_price_df = pd.DataFrame(columns = ['open', 'close', 'high', 'low', 'volume', 'money'])
    
    ## for loop to read the daily price dataframe
    for lab,row in price_df.iterrows():
        
        ## get the date of friday in the same week
        w_day = lab.weekday()
        fri_date = lab.to_pydatetime() - datetime.timedelta(days=w_day) + datetime.timedelta(days=4)
        
        ## if it is the first record in the daily price dataframe
        if len(weekly_price_df) == 0:
            
            ## set the first record
            weekly_price_df.loc[fri_date, 'open'] = row['open']
            weekly_price_df.loc[fri_date, 'close'] = row['close']
            weekly_price_df.loc[fri_date, 'high'] = row['high']
            weekly_price_df.loc[fri_date, 'low'] = row['low']
            weekly_price_df.loc[fri_date, 'volume'] = row['volume']
            weekly_price_df.loc[fri_date, 'money'] = row['money']
            
        else:           
            ## if the friday record already exists
            if weekly_price_df.index[-1] != fri_date:
                weekly_price_df.loc[fri_date, 'open'] = row['open']
                weekly_price_df.loc[fri_date, 'close'] = row['close']
                weekly_price_df.loc[fri_date, 'high'] = row['high']
                weekly_price_df.loc[fri_date, 'low'] = row['low']
                weekly_price_df.loc[fri_date, 'volume'] = row['volume']
                weekly_price_df.loc[fri_date, 'money'] = row['money']
            
            else:   
                weekly_price_df.loc[fri_date, 'close'] = row['close']
            
                if weekly_price_df.loc[fri_date, 'high'] < row['high']:
                    weekly_price_df.loc[fri_date, 'high'] = row['high']
                
                if weekly_price_df.loc[fri_date, 'low'] > row['low']:
                    weekly_price_df.loc[fri_date, 'low'] = row['low']
                
                weekly_price_df.loc[fri_date, 'volume'] += row['volume']
            
                weekly_price_df.loc[fri_date, 'money'] += row['money']
    
    return weekly_price_df
          

def get_weekly_mean(weekly_price_df):
    """
        Task:
            add '5W' '10W' '20W' mean to the weekly price dataframe
        Parameters:
            weekly_price_df:
                weekly price dataframe
       Return:
           weekly_price_mean_df: 
           a dataframe of weekly price, time series indexed, with 5W,10W,20W mean
            columns' type:
                index(Date): datetime.date
                open      119 non-null float64
                close     119 non-null float64
                high      119 non-null float64
                low       119 non-null float64
                volume    119 non-null float64
                money     119 non-null float64
                5W        119 non-null float64
                10W        119 non-null float64
                20W        119 non-null float64
                UP        179 non-null integer
                DOWN      179 non-null integer
    """    

    ## create a copy dataframe of the weekly price dataframe    
    weekly_price_mean_df = weekly_price_df.copy()
    
    ## calculate mean value
    weekly_price_mean_df['5W'] = weekly_price_mean_df['close'].rolling(window=5).mean()
    weekly_price_mean_df['10W'] = weekly_price_mean_df['close'].rolling(window=10).mean()
    weekly_price_mean_df['20W'] = weekly_price_mean_df['close'].rolling(window=20).mean()
        
    ## up trend or down trend?
    weekly_price_mean_df['UP'] = ((weekly_price_mean_df['5W'] > weekly_price_mean_df['10W']) & (weekly_price_mean_df['10W'] > weekly_price_mean_df['20W'])).apply(int)
    weekly_price_mean_df['DOWN'] = ((weekly_price_mean_df['5W'] < weekly_price_mean_df['10W']) & (weekly_price_mean_df['10W'] < weekly_price_mean_df['20W'])).apply(int)
    
    
    return weekly_price_mean_df
                
def get_price_KDJ(price_df, N=9, M1=3, M2=3, t=25):
    """
        Task:
            add 'K' 'D' 'J' to the price dataframe
        Parameters:
            price_df:  price dataframe
            N:  KDJ parameter
            M1: KDJ parameter
            M2: KDJ parameter
            t:  threshold with KDJ
       Return:
           price_KDJ_df: 
           a dataframe of price, time series indexed, with KDJ
            columns' type:
                index(Date): datetime.date
                open      119 non-null float64
                close     119 non-null float64
                high      119 non-null float64
                low       119 non-null float64
                volume    119 non-null float64
                money     119 non-null float64
                K         119 non-null float64
                D         119 non-null float64
                J         119 non-null float64
                UP        179 non-null integer
                DOWN      179 non-null integer
    """    
    ## create a copy dataframe of the price dataframe    
    price_KDJ_df = price_df.copy()
    
    ## get the length of the price dataframe
    datalen = len(price_df)
    
    ## for loop to get the KDJ value for every record
    for i in range(datalen):
        ## get the begin point
        if i-N<0:  
            b=0  
        else:  
            b=i-N+1 
        
        ## get the records for analysis
        rsvdata = price_KDJ_df.iloc[b:i+1,]
        if (max(rsvdata['high']) - min(rsvdata['low'])) != 0:
            rsv = ((rsvdata.iloc[-1,1] - min(rsvdata['low']))/(max(rsvdata['high']) - min(rsvdata['low'])))*100
        else:
            rsv = 100
        if i==0:  
            k=rsv  
            d=rsv
        else:  
            k=1/float(M1)*rsv+(float(M1)-1)/M1*float(last_k)  
            d=1/float(M2)*k+(float(M2)-1)/M2*float(last_d)  
            
        j=3*k-2*d  
        
        ## up trend or down trend
        if i == 0:
            up = 0
            down = 0
        else:
            ## gold cross of KDJ
            if (((last_j < last_k) & (last_k < last_d)) & ((j > k) & (k > d)) & (last_d<t)):
                up = 1
            else:
                up = 0
            ## dead cross of KDJ
            if (((j < k) & (k < d)) & ((last_j > last_k) & (last_k > last_d)) & (last_d>(100-t))):
                down = 1
            else:
                down = 0
                
        ## get the current row    
        lab = price_KDJ_df.index[i]
        
        ## update the columns
        price_KDJ_df.loc[lab,'K'] = k
        price_KDJ_df.loc[lab,'D'] = d
        price_KDJ_df.loc[lab,'J'] = j
        price_KDJ_df.loc[lab,'UP'] = up
        price_KDJ_df.loc[lab,'DOWN'] = down

        ## update yesterday's value
        last_k = k
        last_d = d
        last_j = j
    
    return price_KDJ_df



def get_BSP_mean_KDJ(weekly_mean_df, daily_KDJ_df, stockid):
    """
        Task:
            get the buy & sell point dataframe
        Parameters:
            weekly_mean_df:  weekly price dataframe with mean indicators
            daily_KDJ_df: daily price dataframe with KDJ
            stockid: stock code (like "600519.XSHG")
       Return:
           BSP_df: 
           a dataframe of buy & sell advices
            columns' type:
                Date       15 non-null datetime64[ns]
                Oper       15 non-null object
                StockID    15 non-null object
    """     
    ## create an empty dataframe for BSP_df
    BSP_df = pd.DataFrame(columns = ["Date", "Oper",  "StockID"])
    
    ## for loop to read the record in daily_KDJ_df
    for lab, row in daily_KDJ_df.iterrows():
        
        ## get today's date
        date = lab.to_pydatetime();
        
        ## set oper to the default value
        oper = 0 ;
        
        ## get the date of last friday
        w_day = lab.weekday()
        fri_date = lab.to_pydatetime() - datetime.timedelta(days=(w_day+7)) + datetime.timedelta(days=4)
        
        if fri_date in weekly_mean_df.index:            
            if ((weekly_mean_df.loc[fri_date,'UP'] == 1) & (row['UP'] == 1)):
                if row['close'] >= weekly_mean_df.loc[fri_date,'5W']:
                    oper = "Buy"
            elif ((weekly_mean_df.loc[fri_date,'DOWN'] == 1) & (row['DOWN'] == 1)):
                oper = "Sell"
            
            if oper != 0 :
                BSP_df = BSP_df.append({"Date":date, "Oper":oper, "StockID":stockid}, ignore_index = True)
    
    return BSP_df
            
def get_T_df(account_holding_df, BSP_df, account_id, scale, op_date, price_offline_df = None, account_money = -1):
    """
        Task:
            get the transaction dataframe for one day based on the BSP_df 
            used for loopback testing
        Parameters:
            account_holding_df:  dataframe of account holding information
            BSP_df: buy&sell point dataframe
            account_id: account id for loopback testing 
            scale: scale of singe buy money
            op_date: date of the operation
       Return:
           T_df: the dataframe including all transaction records
                columns = ["Account_ID", "Date", "Type", "Stock_Code",  "Number", "Price"]
    """        
        
    T_df = pd.DataFrame(columns = ["Account_ID", "Date", "Type", "Stock_Code",  "Number", "Price"])
    
    ## the criterion to find the record in the operation date
    criterion1 = (BSP_df['Date'] == datetime.datetime.combine(op_date,datetime.time(0)))
    today_bsp = BSP_df.loc[criterion1,]


    ## transform op_date to string
    op_date_str = op_date.strftime('%Y-%m-%d')
    
    ## the criterion to find the stock record in accounting holdings table
    criterion2 = ((account_holding_df['Date'] == op_date_str) &
                        (account_holding_df['Account_ID'] == account_id))
    today_account = account_holding_df.loc[criterion2,]
    
        
    ## get account money
    if account_money == -1:
        account_money = today_account.loc[(today_account['Stock_Code'] == '000000.RMB'), 'Value'].iloc[0]
    
    ## through the account records, make the deciesion to buy or sell stocks in the account
    if len(today_account) > 0:        
        for lab, row in today_account.iterrows():
            stock_id = row['Stock_Code']
            if (stock_id != '000000.RMB') & (stock_id not in list(today_bsp['StockID'])):
                maxprofit = row['MaxProfit']
                cost = row['Cost']
                value = row['Value']
                number = row['Number']
                position = row['Position']


                ## sell the stock if loss money
                if maxprofit <= 0:
                    if (value/cost) < 0.95:
                        T_df = T_df.append({"Account_ID":account_id, "Date":op_date_str, "Type":'Sell', "Stock_Code":stock_id,  "Number":number, "Price":value}, ignore_index = True)                        
                        account_money += (number * value)
                elif (maxprofit > 0) & (maxprofit <= 0.15):
                    if (value/cost) < 0.95:
                        T_df = T_df.append({"Account_ID":account_id, "Date":op_date_str, "Type":'Sell', "Stock_Code":stock_id,  "Number":number, "Price":value}, ignore_index = True)
                        account_money += (number * value)


    if (len(today_bsp) > 0) & (len(today_account) > 0):
        for lab, row in today_bsp.iterrows():
            
            stock_id = row['StockID']
            
            ## get the close price of the stock
            if price_offline_df is None:
                stock_price = get_price(stock_id, start_date = op_date_str, end_date = op_date_str, fq = None)
            else:
                stock_price = get_price_local(stock_id, op_date_str, op_date_str, price_offline_df)
            
            close_price = stock_price.loc[op_date_str, 'close']
            
            stock_record = today_account.loc[(today_account['Stock_Code'] == stock_id), ]
            
            
            if row['Oper'] == 'Buy':
                number = int(scale/(close_price*100)) * 100
                money = number * close_price
                
                
                if money < account_money:                    
                    if len(stock_record) == 0:
                        T_df = T_df.append({"Account_ID":account_id, "Date":op_date_str, "Type":'Buy', "Stock_Code":stock_id,  "Number":number, "Price":close_price}, ignore_index = True)
                        account_money -= money
                    else:
                        position = stock_record.iloc[0,6]
                        if position < 4:
                            T_df = T_df.append({"Account_ID":account_id, "Date":op_date_str, "Type":'Buy', "Stock_Code":stock_id,  "Number":number, "Price":close_price}, ignore_index = True)
                            account_money -= money
            elif row['Oper'] == 'Sell':
                if len(stock_record) > 0:
                    number = stock_record['Number'].iloc[0]
                    T_df = T_df.append({"Account_ID":account_id, "Date":op_date_str, "Type":'Sell', "Stock_Code":stock_id,  "Number":number, "Price":close_price}, ignore_index = True)
                    account_money += number * close_price


    return T_df


def single_back_test_001(bsp_df, t_days, price_df = None, bonus_dict = None):
    """
        Task:
            based on bsp_df, run a single back test, No_001
        Parameters:
            bsp_df: buy&sell point dataframe
            t_days: trade days list
            price_df: price offline dataframe
            bonus_dict: bonus offline information 
       Return:
           df: dataframe of test account holdings information
           v_df: dataframe of test account'value information
    """     
    
    ## initialize start, end date
    start = t_days[0].strftime('%Y-%m-%d')


    ## Trasnsaction information
    t_df = pd.DataFrame({"Account_ID":["001.GPZH"], "Date":[start], "Type":["Buy"], 
                                   "Stock_Code":["000000.RMB"],  
                                   "Number":[1], 
                                   "Price":[100000000]}, 
                                    columns = ["Account_ID", "Date", "Type", "Stock_Code",  "Number", "Price"])
    
    ## Account Holdings information
    a_df = pd.DataFrame(columns = ["Date", "Account_ID",  "Stock_Code", "Number", "Cost", "Value", "Position", "MaxProfit", "InDate"])
    
    ## initialize df
    df = acc_hold_update(start, start, a_df, t_df, price_offline_df=price_df, t_days = t_days)
    
    ## default scale, account_value/40    
    in_scale = 2500000

    ## for loop t_days to buy&sell stocks and update df
    for date in t_days:
        
        ## udpate stocks' price
        df = acc_hold_update(date.strftime('%Y-%m-%d'), date.strftime('%Y-%m-%d'), df, price_offline_df = price_df, t_days=t_days, bonus_dict = bonus_dict)
        
        ## buy & sell stocks
        today_t_df = get_T_df(df,bsp_df,"001.GPZH", in_scale,date, price_offline_df = price_df) 
        df = acc_hold_update(date.strftime('%Y-%m-%d'), date.strftime('%Y-%m-%d'), df, today_t_df, price_offline_df = price_df, t_days=t_days, bonus_dict = bonus_dict)
    
    ## calculate v_df    
    v_df = net_value_cal(df, t_days = t_days)
     
    return df, v_df
    
def random_sample_test(bsp_df, start, end, n, price_df = None, bonus_dict = None):  
    """
        Task:
            select n random date to run single back test
        Parameters:
            bsp_df: buy&sell point dataframe
            start: start date, string
            end: end date, string
            n: number of random sample
            price_df: price offline dataframe
            bonus_dict: bonus offline information 
       Return:
           test_result_df: test result dataframe,including n rows
           sum_result_dict: result summary
           test_df_list: list of dataframes of test account holdings information
           test_v_df_list: list of dataframes of test account'value information
    """     

    ## get trade_days with jqdata
    trade_days = get_trade_days(start, end)
    
    ## get n random date'index
    sample_index = random.sample(range(len(trade_days)), n)
    
    
    ## initialize the variables
    test_df_list = []
    test_v_df_list = []    
    test_t_df_list = []
    j = 0
    
    ## for loop the sample index
    for i in sample_index:

        j += 1    
        print("the "+str(j)+"th sample, "+trade_days[i].strftime('%Y-%m-%d')+":start computing...")        
        
        df, v_df, t_df = single_back_test_001(bsp_df, trade_days[i:], price_df = price_df, bonus_dict = bonus_dict)
        
       
        ## update test_df_list, test_v_df_list
        test_df_list.append(df)
        test_v_df_list.append(v_df)
        test_t_df_list.append(t_df)
        
  
    return test_df_list, test_v_df_list, test_t_df_list 
    

