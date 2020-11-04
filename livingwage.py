import sys
import requests
from bs4 import BeautifulSoup as bs
from time import sleep
import time
from random import randint
from warnings import warn
import json, re
import pandas as pd

# def get_states(url):
#     r = requests.get(url, auth=('user', 'pass'))
#     soup = bs(r.content, 'html.parser')
#     # a_tags = soup.find_all('a')[4:-3]
#     ul_tags = soup.find_all('ul',{'class':'location_list'})
#     states = [i['href'][-12:-10] for sublist in [tag.find_all('a') for tag in ul_tags] for i in sublist]
#     return states

def append_state_values(year, state, url, df):
    new_url = url+'states/'+state
    r = requests.get(new_url, auth=('user', 'pass'))
    soup = bs(r.content, 'html.parser')
    updated_year = soup.find_all('a')[-1]['href'][5:9]
    #Handle year 2020 when scraping straight from website (not wayback machine)
    if year=='':
        year = '2020'
        updated_year = '2020'
    if year==updated_year:
        tr_tag = soup.find_all("tr", class_="odd results")
        ann_inc_bt = [re.sub('\s+','',i.text)[1:].replace(',','') for i in tr_tag[1].find_all('td')][1:]
        hourly_wages = [re.sub('\s+','',i.text)[1:].replace(',','') for i in tr_tag[0].find_all('td')][1:]
        if (int(year) > 2016) and (int(year) < 2020):    
            ann_inc_bt.pop(8)
            hourly_wages.pop(8)
        ann_inc_bt.extend(hourly_wages)
        ann_inc_bt.insert(0,state)
        ann_inc_bt.insert(0,year)
        df.loc[len(df)] = ann_inc_bt

def append_metro_values(year, metro, url, df):
    r = requests.get(url, auth=('user', 'pass'))
    soup = bs(r.content, 'html.parser')
    updated_year = soup.find_all('a')[-1]['href'][5:9]
    #Handle year 2020 when scraping straight from website (not wayback machine)
    if year=='':
        year = '2020'
        updated_year = '2020'
    if year==updated_year:
        tr_tag = soup.find_all("tr", class_="odd results")
        ann_inc_bt = [re.sub('\s+','',i.text)[1:].replace(',','') for i in tr_tag[1].find_all('td')][1:]
        hourly_wages = [re.sub('\s+','',i.text)[1:].replace(',','') for i in tr_tag[0].find_all('td')][1:]
        if (int(year) > 2016) and (int(year) < 2020):    
            ann_inc_bt.pop(8)
            hourly_wages.pop(8)
        ann_inc_bt.extend(hourly_wages)
        ann_inc_bt.insert(0,metro)
        ann_inc_bt.insert(0,year)
        df.loc[len(df)] = ann_inc_bt

def final_adjustments_state(df):
    df[['1Adult-Y','1Adult_1Child-Y','1Adult_2Child-Y','1Adult_3Child-Y','2Adult1W-Y','2Adult1W_1Child-Y','2Adult1W_2Child-Y','2Adult1W_3Child-Y', '2Adult-Y','2Adult_1Child-Y','2Adult_2Child-Y','2Adult_3Child-Y','1Adult-W', '1Adult_1Child-W', '1Adult_2Child-W','1Adult_3Child-W', '2Adult1W-W', '2Adult1W_1Child-W', '2Adult1W_2Child-W','2Adult1W_3Child-W', '2Adult-W', '2Adult_1Child-W', '2Adult_2Child-W','2Adult_3Child-W']] = df[['1Adult-Y','1Adult_1Child-Y','1Adult_2Child-Y','1Adult_3Child-Y','2Adult1W-Y','2Adult1W_1Child-Y','2Adult1W_2Child-Y','2Adult1W_3Child-Y', '2Adult-Y','2Adult_1Child-Y','2Adult_2Child-Y','2Adult_3Child-Y','1Adult-W', '1Adult_1Child-W', '1Adult_2Child-W','1Adult_3Child-W', '2Adult1W-W', '2Adult1W_1Child-W', '2Adult1W_2Child-W','2Adult1W_3Child-W', '2Adult-W', '2Adult_1Child-W', '2Adult_2Child-W','2Adult_3Child-W']].apply(pd.to_numeric)

    #Set 2017 values for Louisiana as mean of 2016 and 2018 values
    lou17 = list((df[(df['State_FIPS']=='22')&(df['Year']=='2018')].values[0][2:]+df[(df['State_FIPS']=='22')&(df['Year']=='2016')].values[0][2:])/2)
    lou17.insert(0,'22')
    lou17.insert(0,'2017')
    df.loc[len(df)] = lou17

    #Set 2019 values for Nebraska as mean of 2018 and 2020 values
    neb19 = list((df[(df['State_FIPS']=='31')&(df['Year']=='2018')].values[0][2:]+df[(df['State_FIPS']=='31')&(df['Year']=='2020')].values[0][2:])/2)
    neb19.insert(0,'31')
    neb19.insert(0,'2019')
    df.loc[len(df)] = neb19

    #Set 2019 values for Vermont as mean of 2018 and 2020 values
    ver19 = list((df[(df['State_FIPS']=='50')&(df['Year']=='2018')].values[0][2:]+df[(df['State_FIPS']=='50')&(df['Year']=='2020')].values[0][2:])/2)
    ver19.insert(0,'50')
    ver19.insert(0,'2019')
    df.loc[len(df)] = ver19

    return df


if __name__=='__main__':
    export_path = '/home/dhense/PublicData/Economic_analysis/intermediate_files/'

    df_map = pd.read_excel("metro_mapper.xlsx")

    metro_codes = list(df_map['CBSA_2003_Code'].astype(str).unique())

    df = pd.DataFrame(columns=['Year','State_FIPS','1Adult-Y','1Adult_1Child-Y','1Adult_2Child-Y','1Adult_3Child-Y','2Adult1W-Y','2Adult1W_1Child-Y','2Adult1W_2Child-Y','2Adult1W_3Child-Y', '2Adult-Y','2Adult_1Child-Y','2Adult_2Child-Y','2Adult_3Child-Y','1Adult-W', '1Adult_1Child-W', '1Adult_2Child-W','1Adult_3Child-W', '2Adult1W-W', '2Adult1W_1Child-W', '2Adult1W_2Child-W','2Adult1W_3Child-W', '2Adult-W', '2Adult_1Child-W', '2Adult_2Child-W','2Adult_3Child-W'])

    df_metro = pd.DataFrame(columns=['Year','Metro_Code','1Adult-Y','1Adult_1Child-Y','1Adult_2Child-Y','1Adult_3Child-Y','2Adult1W-Y','2Adult1W_1Child-Y','2Adult1W_2Child-Y','2Adult1W_3Child-Y', '2Adult-Y','2Adult_1Child-Y','2Adult_2Child-Y','2Adult_3Child-Y','1Adult-W', '1Adult_1Child-W', '1Adult_2Child-W','1Adult_3Child-W', '2Adult1W-W', '2Adult1W_1Child-W', '2Adult1W_2Child-W','2Adult1W_3Child-W', '2Adult-W', '2Adult_1Child-W', '2Adult_2Child-W','2Adult_3Child-W'])

    #2012: missing data for state 24 and 26
    # url = 'https://web.archive.org/web/20120622033154/https://livingwage.mit.edu/'

    #2013:works!
    # url = 'https://web.archive.org/web/20130801141421/http://livingwage.mit.edu/'

    #2014: Needs adjustment
    # url = 'https://web.archive.org/web/20140921030413/http://livingwage.mit.edu/'

    #2016: Works - 51/51!
    # url16 = 'https://web.archive.org/web/20160601202248/http://livingwage.mit.edu/'
    url16 = 'https://web.archive.org/web/20160816142444/http://livingwage.mit.edu/'

    #2017: Works for 50/51 Missing Louisiana ('22'): Average 2016 and 2018 data
    url17 = 'https://web.archive.org/web/20170607031844/http://livingwage.mit.edu/'
    

    #2018: Works - 51/51!
    url18 = 'https://web.archive.org/web/20180602075733/http://livingwage.mit.edu/'

    #2019: Works - 49/51 Missing Vermont ('50') and Nebraska ('31')
    url19 = 'https://web.archive.org/web/20190603125215/http://livingwage.mit.edu/'

    #2020:
    url20 = 'https://livingwage.mit.edu/'

    
    states = ['01','02','04','05','06','08','09','10','11','12','13','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','44','45','46','47','48','49','50','51','53','54','55','56']

    tic = time.perf_counter()

    url_list = [url16,url17,url18,url19,url20]
    # url_list = [url16]

    
    for url in url_list:

        year = url[28:32]

        # for i in range(len(states)):
        #     sleep(randint(1,5))
        #     append_state_values(year, states[i], url, df)

        for i in range(len(metro_codes)):
            sleep(randint(1,5))    
            new_url = url+'metros/'+metro_codes[i]
            try:
                append_metro_values(year, metro_codes[i], new_url, df_metro)
            except:
                continue

    
    # df = final_adjustments_state(df)

    toc = time.perf_counter()
    print(f"Parsing took {toc - tic:0.1f} seconds") 

    print(len(df_metro))
    # print(set(states) - set(df['State_FIPS'].value_counts().index))

    # df.to_csv(export_path+'livingwage.csv',index=False)
    df_metro.to_csv(export_path+'livingwagemetro.csv',index=False)


    #Next steps
    #1. Calculate CAGR for each state, calc results going back to 1999 (or use cpi to adjust back in time? or get cost item specific inflation rates?)
    #2. Map Metro codes using mapping document
    