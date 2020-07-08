import pandas as pd
from pandas.core.frame import  DataFrame
import requests
import json
import re
import csv
import calendar
import datetime
import time,math

# Set base parameters
start_time="2017-04-01 00:00:00" 
end_time="2020-04-01 00:00:00" 
start_timeStamp = int(calendar.timegm(time.strptime(start_time, "%Y-%m-%d %H:%M:%S")))
end_timeStamp = int(calendar.timegm(time.strptime(end_time, "%Y-%m-%d %H:%M:%S")))


# Get all url
k=(end_timeStamp-start_timeStamp)//(3600*2000)
m=int(((end_timeStamp-start_timeStamp)%(3600*2000))/3600)

url =[]
t=0
for i in range(k):
    t=t+1
    end=end_timeStamp-2000*3600*i
    base_url = 'https://min-api.cryptocompare.com/data/v2/histohour?fsym=BTC&tsym=USDT&limit='+str(2000)+'&toTs='+str(end)+'&e=binance'
    url.append(base_url)
    if t==k:
        end1=end_timeStamp-2000*3600*t
        base_url1= 'https://min-api.cryptocompare.com/data/v2/histohour?fsym=BTC&tsym=USDT&limit='+str(m)+'&toTs='+str(end1)+'&e=binance'
        url.append(base_url1)


# Download datasets
def parse_page(url):

    r=requests.get(url[k])
    re_json=r.json()
    histhour=re_json['Data']['Data']

    for i in range(k):
        r=requests.get(url[k-i-1])
        re_json=r.json()
        histhour1=re_json['Data']['Data']
        histhour.extend(histhour1)
    
    histdata=[]
    for i in range(len(histhour)):
        j=histhour[i]
        temp={}
        temp["close"]=j["close"]
        temp["high"]=j["high"]
        temp["low"]=j["low"]
        temp["open"]=j["open"]
        temp["volume"]=j["volumeto"]
        temp["baseVolume"]=j["volumefrom"]
        temp["datatime"]=j["time"]
        histdata.append(temp)
    result=pd.DataFrame(histdata)
    return result  


def transfer_time(df):
    for i in range(df.shape[0]):
        df.loc[i,'datatime']=datetime.datetime.utcfromtimestamp(int(df.loc[i,'datatime'])).strftime('%Y-%m-%d %H:%M:%S')
    return df


def save_file(data_df):
    newDF = data_df.drop_duplicates()
    newDF.to_csv("/Users/rainie/Downloads/QuantIntern_Project1/W1/task1/BTU_USDT_per1h.csv",index=False)
    print("Save Successfully!")

    
#main function    
if __name__ == '__main__':
    pdata=parse_page(url)
    df=transfer_time(pdata)
    save_file(df)