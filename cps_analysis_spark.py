import os, pickle, time
import pandas as pd
import numpy as np
from itertools import combinations, product, chain
import pyspark.sql.functions as func
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext
import warnings

warnings.filterwarnings('ignore')
sc = SparkContext("local", "CPS Parser")
sqlContext = SQLContext(sc)



def calc_urate(df_cps,recode_counts,type):
    if type =='U3':
        urate = (recode_counts['Unemployed - on layoff']+recode_counts['Unemployed - looking'])/(recode_counts['Employed - at work']+recode_counts['Employed - absent']+recode_counts['Unemployed - on layoff']+recode_counts['Unemployed - looking'])
    else:
        df_cps['Unemployed_U6'] = np.where(((df_cps['LF_recode'].isin(['Unemployed - on layoff','Unemployed - looking']))|(df_cps['Look_last_year']=='Yes')|(df_cps['Look_last_month']=='Yes')|(df_cps['Job_offered_week']=='Yes')|(df_cps['Last_work']=='Within Last 12 Months')|(df_cps['Reason_not_looking'].isin(['Believes No Work Available In Area Of Expertise','Couldnt Find Any Work','Lacks Necessary Schooling/Training']))|(df_cps['Discouraged']=='Yes')|(df_cps['FT_PT_status'].isin(['PT Hrs, Usually Pt For Economic Reasons','PT for Economic Reasons, Usually Ft']))),'Yes','No')

        num_unemployed = len(df_cps[df_cps['Unemployed_U6']=='Yes'])

        num_employed = len(df_cps[(df_cps['LF_recode'].isin(['Employed - at work','Employed - absent']))&(df_cps['Unemployed_U6']=='No')])

        urate = num_unemployed/(num_employed+num_unemployed)

    return urate


def calc_urate_weighted(df_cps,type):
    # df_cps = weight(df_cps)
    
    if type=='U3':
        recode_weights = df_cps.groupby('LF_recode')['Weight'].sum()
        recode_weighted = dict(zip(recodes,[0,0,0,0,0,0,0]))
        for r in recode_weighted.keys():
            if r in df_cps['LF_recode'].value_counts().index:
                recode_weighted[r] = df_cps['LF_recode'].value_counts()[r]

        urate_weighted = (recode_weighted['Unemployed - on layoff']+recode_weighted['Unemployed - looking'])/((recode_weighted['Employed - at work']+recode_weighted['Employed - absent'])+recode_weighted['Unemployed - on layoff']+recode_weighted['Unemployed - looking'])
    else:
        unemployed_weight = df_cps[df_cps['Unemployed_U6']=='Yes']['Weight'].sum()
        employed_weight = df_cps[(df_cps['LF_recode'].isin(['Employed - at work','Employed - absent']))&(df_cps['Unemployed_U6']=='No')]['Weight'].sum()

        urate_weighted = unemployed_weight/(unemployed_weight+employed_weight)

    return urate_weighted








if __name__=='__main__':
    pickle_path = '/home/dhense/PublicData/Economic_analysis/intermediate_files/cps/'




    print("...loading pickle")
    tmp = open('/home/dhense/PublicData/Economic_analysis/intermediate_files/incarcerated.pickle','rb')
    df_incarcerated = pickle.load(tmp)
    tmp.close()

    print("...loading pickle")
    tmp = open('/home/dhense/PublicData/Economic_analysis/intermediate_files/state_pop.pickle','rb')
    df_state = pickle.load(tmp)
    tmp.close()

    incarceration_dict = {'male':'TOTRACEM','female':'TOTRACEF','white':'TOTWHITE','black':'TOTBLACK','hispanic':'TOTHISP','white_male':'WHITEM','white_female':'WHITEF','black_male':'BLACKM','black_female':'BLACKF','hispanic_male':'HISPM','hispanic_female':'HISPF','total':'INCARCERATED'}

    #year=2000
    # print(df.head())

    var_list = ['Sex','Race','Hispanic','Age_group','Marital_Status','School_completed','State_FIPS','Disabled','Ever_active_duty']
    sex_list = [{'Sex': 'Female'}, {'Sex': 'Male'}]
    #remove other from race?
    race_list = [{'Race': 'White Only'}, {'Race': 'Black Only'}]
    hispanic_list = [{'Hispanic': 'Hispanic'}, {'Hispanic': 'Not Hispanic'}]
    #change age grouping from number to string range (e.g. 1 = '16-19'); limit ages 20-65 for subpops
    # age_list = [{'Age_group': (2,2)},{'Age_group': (3,3)},{'Age_group': (4,4)},{'Age_group': (5,5)},{'Age_group': (6,6)},{'Age_group': (7,7)},{'Age_group': (8,8)},{'Age_group': (9,9)},{'Age_group': (10,10)},{'Age_group': (2,3)},{'Age_group': (3,4)},{'Age_group': (4,5)},{'Age_group': (5,6)},{'Age_group': (6,7)},{'Age_group': (7,8)},{'Age_group': (8,9)},{'Age_group': (9,10)}]
    age_list = [{'Age_group': (2,3)},{'Age_group': (3,4)},{'Age_group': (4,5)},{'Age_group': (5,6)},{'Age_group': (6,7)},{'Age_group': (7,8)},{'Age_group': (8,9)},{'Age_group': (9,10)}]
    #Combine both married items into one? Add absent to separated?
    marital_list = [{'Marital_status': 'Married'},{'Marital_status': 'Never married'},{'Marital_status': 'Divorced'},{'Marital_status': 'Widowed'},{'Marital_status': 'Separated'}]
    #simplify school completed to fewer categories
    school_list = [{'School_completed': 'High school degree'},{'School_completed': 'No high school degree'},{'School_completed': 'Some college but no degree'},{'School_completed': 'Bachelors degree'},{'School_completed': 'Masters degree'},{'School_completed': 'Associate degree'},{'School_completed': 'Professional school degree'},{'School_completed': 'Doctorate degree'}]
    # division_list = [{'Division': 'South Atlantic'},{'Division': 'East North Central'},{'Division': 'Mid-Atlantic'},{'Division': 'Pacific'},{'Division': 'Mountain'},{'Division': 'West South Central'},{'Division': 'West North Central'},{'Division': 'New England'},{'Division': 'East South Central'}]
    state_list = [{'State_FIPS': '06'},{'State_FIPS': '36'},{'State_FIPS': '12'},{'State_FIPS': '48'},{'State_FIPS': '42'},{'State_FIPS': '17'},{'State_FIPS': '39'},{'State_FIPS': '26'},{'State_FIPS': '34'},{'State_FIPS': '37'},{'State_FIPS': '25'},{'State_FIPS': '13'},{'State_FIPS': '04'},{'State_FIPS': '51'},{'State_FIPS': '40'},{'State_FIPS': '01'},{'State_FIPS': '54'},{'State_FIPS': '16'},{'State_FIPS': '32'},{'State_FIPS': '27'},{'State_FIPS': '22'},{'State_FIPS': '08'},{'State_FIPS': '55'},{'State_FIPS': '47'},{'State_FIPS': '30'},{'State_FIPS': '05'},{'State_FIPS': '35'},{'State_FIPS': '31'},{'State_FIPS': '21'},{'State_FIPS': '20'},{'State_FIPS': '49'},{'State_FIPS': '46'},{'State_FIPS': '18'},{'State_FIPS': '19'},{'State_FIPS': '53'},{'State_FIPS': '38'},{'State_FIPS': '56'},{'State_FIPS': '41'},{'State_FIPS': '29'},{'State_FIPS': '45'},{'State_FIPS': '02'},{'State_FIPS': '28'},{'State_FIPS': '24'},{'State_FIPS': '23'},{'State_FIPS': '33'},{'State_FIPS': '44'},{'State_FIPS': '09'},{'State_FIPS': '50'},{'State_FIPS': '10'},{'State_FIPS': '11'},{'State_FIPS': '15'}]
    #disabled_list = [{'Disabled':i} for i in list(df['Disabled'].value_counts().index)]
    #duty_list = list(df['Ever_active_duty'].value_counts().index)

    all_list = [sex_list,race_list,hispanic_list,age_list,state_list]
    # all_list_str = ['sex_list','race_list','hispanic_list','age_list','state_list']

    #print(len(list(product(*all_list))))

    #Timer start
    tic = time.perf_counter()

    lst = [] 
    for i in range(1,4): 
        lst.append(list(combinations(all_list,i)))

    lst_flat = list(chain.from_iterable(lst))
    #print(len(lst_flat))

    count = 0
    final_list = []
    for l in lst_flat:
        final_list.append(list(product(*l)))
    final_flat = list(chain.from_iterable(final_list))
    print(len(final_flat))

    final_flat_sub = final_flat[300:302]

    months = ['jan']
    # months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    
    #Turn this to spark df
    #df_data = pd.DataFrame()

    for year in range(2000,2001):
        for m in months:
            df = sqlContext.read.format('csv').options(header='true', inferSchema='true').load('/home/dhense/PublicData/Economic_analysis/intermediate_files/cps_csv/cps_'+m+str(year)+'.csv')

            for f in final_flat_sub:
                #create copy of df named df_sub
                df_sub = sqlContext.read.format('csv').options(header='true', inferSchema='true').load('/home/dhense/PublicData/Economic_analysis/intermediate_files/cps_csv/cps_'+m+str(year)+'.csv')
                #filter according to f, separate out age_group filter
                for i in f:
                    if type(list(i.values())[0]) is tuple:
                        df_sub=df_sub.filter((func.col(str(list(i.keys())[0]))>=list(i.values())[0][0])&(func.col(str(list(i.keys())[0]))<=list(i.values())[0][1]))
                    else:
                        #print(str(list(i.keys())[0]))
                        df_sub = df_sub.filter(func.col(str(list(i.keys())[0]))==list(i.values())[0])
                        print(df_sub.count())
                

                #continue (skip f) if length of dataframe is less than 100
                if df_sub.count()<100:
                    continue

                #create recode counts
                #Make sure recode counts contains all items, and adds 0 to ones without any
                recodes = ['Employed - at work','Not in labor force - retired','Not in labor force - other','Not in labor force - disabled','Unemployed - looking','Employed - absent','Unemployed - on layoff']
                #df_sub['LF_recode'].value_counts().keys().tolist()
                recode_counts = dict(zip(recodes,[0,0,0,0,0,0,0]))
                for r in recode_counts.keys():
                    # print(df_sub.select(func.collect_list('LF_recode')).first()[0])
                    # print(df_sub.groupby('LF_recode').count().select(func.collect_list('LF_recode')).first()[0])
                    if r in df_sub.groupby('LF_recode').count().select(func.collect_list('LF_recode')).first()[0]:
                        recode_counts[r] = df_sub.groupby('LF_recode').count().where(func.col('LF_recode')==r)
                        #print(df_sub.groupby('LF_recode').count().where(func.col('LF_recode')==r).show())
                        # print(r)

                #calculate U3 rate
                #calculate U3 weighted
                #calculate U6 rate
                #calculate U6 weighted

                #create dataframe titled data w all relevant data
                #append data to df_data

    # toc = time.perf_counter()
    # print(f"Calculations took {toc - tic:0.1f} seconds") 

    #Save df_data as csv/pickle
