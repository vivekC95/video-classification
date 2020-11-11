# -*- coding: utf-8 -*-
"""
Created on Sun Nov  8 06:28:47 2020

@author: vivek
"""
import requests as req 
import pandas as pd
import sys 
import time

#Add Vars 
sys.path.append('E:\ContentClassification')

#External DBOPS Lib

from main.DbOps import DatabaseOperations as DO

# Globals
API_KEY = 'e8ddef0c243341e37891'
API_SECRET = '0779e8e508b750dafe56b57a2a36d0d3ca5eddc7'

data = req.get('https://api.dailymotion.com/channels?fields=name%2Cid&limit=100').json()

print(data['list'])

data_df = pd.DataFrame().from_records(data['list'])

for i in range(0,data_df.shape[0]):
    print(data_df['id'][i])

print(req.get('https://api.dailymotion.com/channel/auto/videos?fields=duration%2Ctitle%2Cdescription&page=1&limit=100').json())

db = DO(host='localhost',username='vivek',password='password',port=3306,database='content')



final_vid_data = pd.DataFrame()
for category in data_df['id'].unique():
    page_num = 1
    has_more = True
    index = 0
    limit = 100
    print('Ingesting Data For Category: ' + category)
    while has_more == True:    
        vid_data = req.get('https://api.dailymotion.com/channel/{cat}/videos?fields=duration%2Ctitle%2Cdescription&page={num}&limit=100'.format(num = page_num, cat = category)).json()
        print(limit)
        print(vid_data['total'])
        print('Progress: ' + str((limit/vid_data['total'])*100) + '%')
        has_more = vid_data['has_more']
        dynamic_df = pd.DataFrame().from_records(vid_data['list'])
        dynamic_df['category'] = category
        final_vid_data = pd.concat([dynamic_df,final_vid_data])
        db.data = final_vid_data 
        db.write_data_from_frame('videodetails',chunks = 100)
        page_num += 1
        time.sleep(3)
        limit += 100
    
db = DO(final_vid_data,host='localhost',username='vivek',password='password',port=3306,database='content')


    

