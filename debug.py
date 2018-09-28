

from stock import *

## input parameters 
start = "2018-01-01"
end = "2018-06-29" 

## Trasnsaction information
t_df = pd.DataFrame({"Account_ID":["001.GPZH", "001.GPZH","001.GPZH","001.GPZH", "001.GPZH"], 
                               "Date":["2018-01-25", "2018-03-02", "2018-04-26", "2018-05-10", "2018-05-17"], 
                               "Type":["Buy", "Buy", "Buy", "Sell", "Buy"], 
                               "Stock_Code":["000000.RMB", "600487.XSHG", "603799.XSHG", "600487.XSHG", "002050.XSHE"],  
                               "Number":[1, 2000, 1000, 2000, 3000], 
                               "Price":[1000000, 39.00, 105.35, 34.30, 17.63]}, 
                                columns = ["Account_ID", "Date", "Type", "Stock_Code",  "Number", "Price"])

## Trasnsaction information
t_df = pd.DataFrame({"Account_ID":["001.GPZH", "001.GPZH"], 
                               "Date":["2018-01-25", "2018-03-02"], 
                               "Type":["Buy", "Buy"], 
                               "Stock_Code":["000000.RMB", "600487.XSHG"],  
                               "Number":[1, 100000], 
                               "Price":[1000000, 9.00]}, 
                                columns = ["Account_ID", "Date", "Type", "Stock_Code",  "Number", "Price"])



## Account Holdings information
a_df = pd.DataFrame(columns = ["Date", "Account_ID",  "Stock_Code", "Number", "Cost", "Value", "Position", "MaxProfit", "InDate"])

## Trasnsaction information
t_df = pd.DataFrame({"Account_ID":["001.GPZH", "001.GPZH"], 
                               "Date":["2018-03-19","2018-03-20"], 
                               "Type":["Buy","Sell"], 
                               "Stock_Code":["000000.RMB","000000.RMB"],  
                               "Number":[1,1], 
                               "Price":[1000000,500000]}, 
                                columns = ["Account_ID", "Date", "Type", "Stock_Code",  "Number", "Price"])

        
df = acc_hold_update('2018-01-03', '2018-03-18', a_df, t_df)

df = acc_hold_update('2018-03-19', '2018-03-20', df, t_df)


v_df = net_value_cal(df)

c_df = comp_growth(v_df, ['001.GPZH'], '2018-01-03', '2018-03-22')

c_df = comp_growth(v_df, ['001.GPZH'], '2014-01-09', '2018-06-20', ['600519.XSHG'])
                 
c_df = comp_growth(v_df, ['001.GPZH'], '2006-01-05', '2018-08-25', ['000001.XSHG'])

c_df.plot()
plt.show()
   

get_price(code_str, start_date = today_str, end_date = today_str)


bonus_df = get_bonus_info('600487','SINA')

## Trasnsaction information
t_df1 = pd.DataFrame({"Account_ID":["001.GPZH", "001.GPZH"], 
                               "Date":["2014-01-03", "2014-01-09"], 
                               "Type":["Buy", "Buy"], 
                               "Stock_Code":["000000.RMB", "600519.XSHG"],  
                               "Number":[1, 1000], 
                               "Price":[119340, 119.34]}, 
                                columns = ["Account_ID", "Date", "Type", "Stock_Code",  "Number", "Price"])


## get shanghai index price 

## input parameters 
start = "2015-01-01"
end = "2018-06-29" 

shindex_df = get_price("000001.XSHG", start_date = start, end_date = end)

df = shindex_df[['close']].copy()

mean_index = df['close'].rolling(window=120).mean()

df['120mean'] = mean_index

df['growth'] = df['close'] - df['120mean']

clean_df = df.dropna().copy()

sum = 0
for i,row in clean_df.iterrows():
    sum = sum + row['growth']
    clean_df.loc[i,'sum'] = sum
    
clean_df['sumNormal'] = clean_df['sum'] / clean_df['120mean']



clean_df['sumNormal'].describe()

clean_df.boxplot(column ='sumNormal')

print(clean_df['sumNormal'].quantile([0.25, 0.75]))

sumnormal = clean_df.loc['2007-01-08':, 'sumNormal']

buy_point = sumnormal.quantile([0.25, 0.75])[0.25]

sell_point = sumnormal.quantile([0.25, 0.75])[0.75]

def get_oper(x,buy,sell):
    if x < buy:
        return 1
    elif x > sell:
        return -1
    else:
        return 0

clean_df['operation'] = clean_df['sumNormal'].apply(get_oper,buy=buy_point,sell =sell_point)

clean_df['operation'].plot()







trade_days = stock_df.index.date

trade_days = get_trade_days("2005-01-05", "2018-08-24")

trade_days = get_trade_days("2006-01-04", "2007-06-15")





c_df = comp_growth(v_df, ['001.GPZH'], start, end)
   
c_df = comp_growth(v_df, ['001.GPZH'], "2006-01-19","2014-04-01", ['000300.XSHG'])


c_df = comp_growth(v_df, ['001.GPZH'], "2015-01-04",end, ['000300.XSHG'])

c_df = comp_growth(v_df, ['001.GPZH'], "2005-05-01",end, ['000300.XSHG'])


fig = c_df.plot().get_figure()
fig.savefig("jpg/one_month.jpg")



#
#
#
#
test_df_list, test_v_df_list, test_t_df_list = random_sample_test(total_bsp_df_nosell, "2005-01-04", "2018-08-24", 100, price_df = price_database_all_origin, bonus_dict = bonus_dict_local)
    


tmp_df = pd.DataFrame(data = {"Type":"Buy", "Number":1, "Cost":1}, index=["000000.RMB"],columns = ["Type", "Number", "Cost", "Sell_Price", "Return", "Return_Ratio"])        
        

tmp_df1 = pd.DataFrame(columns = ["Type", "Stock_Code",  "Number", "Cost", "Sell_Price", "Return", "Return_Ratio"])
tmp_df1 = tmp_df1.set_index("Stock_Code")

from stock import *

trade_days = get_trade_days("2013-09-03", "2018-08-24")


df, v_df, t_df = single_back_test_001(total_bsp_df_nosell, trade_days, price_df = price_database_all_origin, bonus_dict = bonus_dict_local)

c_df = comp_growth(v_df, ['001.GPZH'], "2013-09-03","2018-08-24", ['000300.XSHG'])
c_df.plot()


c_df = comp_growth(v_df, ['001.GPZH'], "2018-01-01","2018-08-24", ['000300.XSHG'])
c_df.plot()
