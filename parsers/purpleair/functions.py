
from webbrowser import get
from numpy import average
import requests, os, numpy as np
import pandas as pd
from datetime import datetime
import time
import json
from io import StringIO


def get_historicaldata(url,sensors_list,fields,bdate,edate,average_time,key_read,dir):
    # Historical API URL
    # root_api_url = 'https://api.purpleair.com/v1/sensors/'
    root_api_url = url
    
    # Average time: The desired average in minutes, one of the following:0 (real-time),10 (default if not specified),30,60
    average_api = f'&average={average_time}'

    # Creating fields api url from fields list to download the data: Note: Sensor ID/Index will not be downloaded as default
    # fields_list = ['pm2.5_atm_a', 'pm2.5_atm_b', 'pm2.5_cf_1_a', 'pm2.5_cf_1_b', 'humidity_a', 'humidity_b', 
    #            'temperature_a', 'temperature_b', 'pressure_a', 'pressure_b']
    fields_list = fields
    for i,f in enumerate(fields_list):
        if (i == 0):
            fields_api_url = f'&fields={f}'
        else:
            fields_api_url += f'%2C{f}'

    # Dates of Historical Data period
    begindate = datetime.strptime(bdate, '%m-%d-%Y')
    enddate   = datetime.strptime(edate, '%m-%d-%Y')
    
    # Downlaod days based on average
    if (average_time == 60):
        date_list = pd.date_range(begindate,enddate,freq='14d') # for 14 days of data
    else:
        date_list = pd.date_range(begindate,enddate,freq='2d') # for 2 days of data

    if date_list[len(date_list)-1].date != enddate.date:
        date_list = np.append(date_list.date, enddate.date())

    print(date_list)
        
    # Converting to UNIX timestamp
    date_list_unix=[]
    for dt in date_list:
        date_list_unix.append(int(time.mktime(dt.timetuple())))

    # Reversing to get data from end date to start date
    date_list_unix.reverse()
    len_datelist = len(date_list_unix) - 1
        
    # Getting 2-data for one sensor at a time
    for s in sensors_list:
        # Adding sensor_index & API Key
        hist_api_url = root_api_url + f'{s}/history/csv?api_key={key_read}'

        # Creating start and end date api url
        for i,d in enumerate(date_list_unix):
            if (i < len_datelist):
                print('Downloading for PA: %s for Dates: %s and %s.' 
                      %(s,datetime.fromtimestamp(date_list_unix[i+1]),datetime.fromtimestamp(d)))
                dates_api_url = f'&start_timestamp={date_list_unix[i+1]}&end_timestamp={d}'
            
                # Final API URL
                api_url = hist_api_url + dates_api_url + average_api + fields_api_url
                            
                #
                try:
                    response = requests.get(api_url)
                except:
                    print(api_url)
                #
                try:
                    assert response.status_code == requests.codes.ok
                
                    # Creating a Pandas DataFrame
                    df = pd.read_csv(StringIO(response.text), sep=",", header=0)
                
                except AssertionError:
                    df = pd.DataFrame()
                    print('Bad URL!')
            
                if df.empty:
                    print('------------- No Data Available -------------')
                else:
                    # Adding Sensor Index/ID
                    df['id'] = s
                
                    #
                    date_time_utc=[]
                    for index, row in df.iterrows():
                        date_time_utc.append(datetime.fromtimestamp(row['time_stamp']))
                    df['date_time_utc'] = date_time_utc
                
                    # Dropping duplicate rows
                    df = df.drop_duplicates(subset=None, keep='first', inplace=False)
                    
                    # writing to csv file
                    filename = dir + '/sensorsID_%s_%s_%s.csv' % (s,datetime.fromtimestamp(date_list_unix[i+1]).strftime('%m-%d-%Y'),datetime.fromtimestamp(d).strftime('%m-%d-%Y'))
                    df.to_csv(filename, index=False, header=True)


# sensor_list = [26695]
# bdate = '10-22-2019'
# edate = '11-20-2019'
# average_time = 60
# key = 'C7D7011A-68C9-11EC-B9BF-42010A800003'
# dir = 'data'

cfd = os.path.dirname(os.path.abspath(__file__))
os.chdir(cfd)
# os.mkdir(dir)
# get_historicaldata(sensor_list, bdate, edate, average_time, key, dir)


def gen_range(start, stop, step):
    current = stop
    while current > start:
        next_current = current - step
        if next_current > start:
            yield [current, next_current]
        else:
            yield [current, start]
        current = next_current



# import os
# with open("checkpoint.txt", "rb") as file:
#     try:
#         file.seek(-2, os.SEEK_END)
#         while file.read(1) != b'\n':
#             file.seek(-2, os.SEEK_CUR)
#     except OSError:
#         file.seek(0)
#     last_line = file.readline().decode()

# print(last_line[:-1])