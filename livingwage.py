import sys
import requests
from bs4 import BeautifulSoup as bs
from time import sleep
import time
from random import randint
from warnings import warn
import json
import pandas as pd

def get_states(url):
    r = requests.get(url, auth=('user', 'pass'))
    soup = bs(r.content, 'html.parser')
    a_tags = soup.find_all('a')[4:-3]
    states = [i['href'][-12:-10] for i in a_tags]
    return states

def append_values(year, state, url, df):
    r = requests.get(url, auth=('user', 'pass'))
    soup = bs(r.content, 'html.parser')
    tr_tag = soup.find_all("tr", class_="even results")
    ann_inc_bt = [i.text.split('\n') for i in tr_tag][0][2:-1]
    ann_inc_bt.insert(0,state)
    ann_inc_bt.insert(0,year)
    df.loc[len(df)] = ann_inc_bt




if __name__=='__main__':
    df = pd.DataFrame(columns=['Year','State_FIPS','1Adult','1Adult_1Child','1Adult_2Child','1Adult_3Child','2Adult','2Adult_1Child','2Adult_2Child','2Adult_3Child'])

    #2012: missing data for state 24 and 26
    # url = 'https://web.archive.org/web/20120622033154/https://livingwage.mit.edu/'

    #2013:works!
    url = 'https://web.archive.org/web/20130801141421/http://livingwage.mit.edu/'

    #2014: Needs adjustment
    # url = 'https://web.archive.org/web/20140921030413/http://livingwage.mit.edu/'
    
    states = get_states(url)
    year = url[28:32]

    tic = time.perf_counter()

    for i in range(len(states)):
        sleep(randint(1,3))
        # if states[i] in ['states/24','states/26']:
        #     continue
        new_url = url+'states/'+states[i]
        append_values(year, states[i],new_url, df)

    toc = time.perf_counter()
    print(f"Parsing took {toc - tic:0.1f} seconds") 

    print(df.head())