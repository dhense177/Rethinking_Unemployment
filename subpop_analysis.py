import os, pickle, math
import pandas as pd
import numpy as np
import pyspark.sql.functions as f
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from functools import reduce
from itertools import combinations, product, chain
import warnings
warnings.filterwarnings('ignore')

# sc = SparkContext("local", "CPS Parser")
# sqlContext = SQLContext(sc)

# spark = SparkSession.builder.appName('pandasToSparkDF').getOrCreate()


# def growth(df,start_year,end_year,period,subpop=None,start_month='jan',end_month='dec',roll_avg=1,stat='UR'):
#     '''
#         Calculate growth rate for given statistic 'stat' on a frequency (e.g. yearly) specified by 
#         'period' between 'start_month' of 'start_year' and 'end_month' of 'end_year' for all subpopulations

#         roll_avg takes an average of months around start_month and end_month. For example, roll_avg of 3 for 
#         start_month = 2 and end month = 9 will calculate the mean of months 1,2 and 3 for start_month and months
#         8,9 and 10 for end month. Defaults to 1 where the formula only takes into account the values of the
#         months provided with no average of months around them

#         Must separate out cases where period does not divide evenly into time frame (end_year-start_year)
#     '''
#     yr_growth = pd.DataFrame()
#     for s in df['SubPop'].unique():
#         df_sub = df[df['SubPop']==s]

#         df_sub = df_sub[(df_sub['Year']>=start_year) & (df_sub['Year']<=end_year)]

#         if subpop:
#             df_sub = df_sub[df_sub['SubPop'].str.contains(subpop)]

#         ratio = int(math.ceil(len(df_sub['Year'].unique())/period))
#         for yr in range(ratio):
#             if len(df_sub['Year'].unique()) % period > 0:
#                 ur = ((df_sub[(df_sub['Year']==end_year-period+ratio+yr-1) & (df_sub['Month']>=(end_month-((roll_avg-1)/2))) & (df_sub['Month']<=(end_month+(roll_avg-1)/2))][stat].mean())-(df_sub[(df_sub['Year']==start_year+yr) & (df_sub['Month']>=(start_month-(roll_avg-1)/2)) & (df_sub['Month']<=(start_month+(roll_avg-1)/2))][stat].mean()))/(df_sub[(df_sub['Year']==start_year+yr) & (df_sub['Month']>=(start_month-(roll_avg-1)/2)) & (df_sub['Month']<=(start_month+(roll_avg-1)/2))][stat].mean())
#                 yr_growth = yr_growth.append(pd.DataFrame([{'SubPop':s,'Start_month':start_month,'Start_year':start_year,'End_month':end_month,'End_year':end_year,'Period':str(str(start_year+yr)+'-'+str(end_year-period+ratio+yr-1)),str(stat)+' Growth %'+' ('+str(period)+'YR)':ur*100}])).reset_index(drop=True)
#             else:
#                 ur = ((df_sub[(df_sub['Year']==end_year-ratio+yr+1) & (df_sub['Month']>=(end_month-(roll_avg-1)/2)) & (df_sub['Month']<=(end_month+(roll_avg-1)/2))][stat].mean())-(df_sub[(df_sub['Year']==start_year+yr) & (df_sub['Month']>=(start_month-(roll_avg-1)/2)) & (df_sub['Month']<=(start_month+(roll_avg-1)/2))][stat].mean()))/(df_sub[(df_sub['Year']==start_year+yr) & (df_sub['Month']>=(start_month-(roll_avg-1)/2)) & (df_sub['Month']<=(start_month+(roll_avg-1)/2))][stat].mean())
#                 yr_growth = yr_growth.append(pd.DataFrame([{'SubPop':s,'Start_month':start_month,'Start_year':start_year,'End_month':end_month,'End_year':end_year,'Period':str(str(start_year+yr)+'-'+str(end_year-ratio+yr+1)),str(stat)+' Growth %'+' ('+str(period)+'YR)':ur*100}])).reset_index(drop=True)

#     #Get rid of infinite and nan values
#     yr_growth = yr_growth.replace([np.inf, -np.inf], np.nan)
#     yr_growth = yr_growth[yr_growth[str(stat)+' Growth %'+' ('+str(period)+'YR)'].isnull()==False]
#     return yr_growth.reset_index(drop=True)


def growth(df,start_year,end_year,subpop=None,start_month=1,end_month=12,roll_avg=1,stat='U3_Rate'):
    '''
        Calculate growth rate for given statistic 'stat' on a frequency (e.g. yearly) specified by 
        'period' between 'start_month' of 'start_year' and 'end_month' of 'end_year' for all subpopulations

        roll_avg takes an average of months around start_month and end_month. For example, roll_avg of 3 for 
        start_month = 2 and end month = 9 will calculate the mean of months 1,2 and 3 for start_month and months
        8,9 and 10 for end month. Defaults to 1 where the formula only takes into account the values of the
        months provided with no average of months around them

        Must separate out cases where period does not divide evenly into time frame (end_year-start_year)
    '''
    yr_growth = pd.DataFrame()
    for s in df['SubPop'].unique():
        df_sub = df[df['SubPop']==s]

        df_sub = df_sub[(df_sub['Year']>=start_year) & (df_sub['Year']<=end_year)]

        if subpop:
            df_sub = df_sub[df_sub['SubPop'].str.contains(subpop)]


        ur = ((df_sub[(df_sub['Year']==end_year) & (df_sub['Month']>=(end_month-((roll_avg-1)/2))) & (df_sub['Month']<=(end_month+(roll_avg-1)/2))][stat].mean())-(df_sub[(df_sub['Year']==start_year) & (df_sub['Month']>=(start_month-(roll_avg-1)/2)) & (df_sub['Month']<=(start_month+(roll_avg-1)/2))][stat].mean()))

        yr_growth = yr_growth.append(pd.DataFrame([{'SubPop':s,'Period':str(str(start_month)+'/'+str(start_year)+'-'+str(end_month)+'/'+str(end_year)),str(stat)+' Growth %':ur*100}])).reset_index(drop=True)


    #Get rid of infinite and nan values
    yr_growth = yr_growth.replace([np.inf, -np.inf], np.nan)
    yr_growth = yr_growth[yr_growth[str(stat)+' Growth %'].isnull()==False]
    return yr_growth.reset_index(drop=True)





if __name__=='__main__':
    pickle_path = '/home/dhense/PublicData/Economic_analysis/intermediate_files/cps/'
    import_path = '/home/dhense/PublicData/Economic_analysis/'

    # print("...loading pickle")
    # tmp = open('/home/dhense/PublicData/Economic_analysis/intermediate_files/subpop_data.pickle','rb')
    # df = pickle.load(tmp)
    # tmp.close()

    df = pd.read_csv(import_path+'subpop_data.csv')

    #Remove brackets and curly braces from SubPop column
    df['SubPop'] = df['SubPop'].str.replace(r'[','').str.replace(r']','').str.replace(r'{','').str.replace(r'}','')

    #Change age group from category number (e.g. (2, 3),(3, 4), etc...) into actual range (e.g. 20-29,25-34, etc...)
    age_groups = {'(2, 3)':'20-29','(3, 4)':'25-34','(4, 5)':'30-39','(5, 6)':'35-44','(6, 7)':'40-49','(7, 8)':'45-54','(8, 9)':'50-59','(9, 10)':'55-64'}
    for k,v in age_groups.items():
        df.loc[df['SubPop'].str.contains(k),'SubPop'] = df[df['SubPop'].str.contains(k)]['SubPop'].str.replace(k,v)

    #Turn month field into int
    months_dict = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}
    df['Month'] = df['Month'].map(lambda x: months_dict[x])

    #Only keep pop percentage greater than 1%
    df = df[df['Pop_percentage']>0.01]

    period = 20
    start_year = 2000
    end_year = 2019

    u3_growth = growth(df,2000,2019,start_month=1,end_month=12,roll_avg=3,stat='U3_Rate')
    u6_growth = growth(df,2000,2019,start_month=1,end_month=12,roll_avg=3,stat='U6_Rate')
    u3_lfpr_growth = growth(df,2000,2019,start_month=1,end_month=12,roll_avg=3,stat='U3_LFPR')
    u6_lfpr_growth = growth(df,2000,2019,start_month=1,end_month=12,roll_avg=3,stat='U6_LFPR')

    growth20 = reduce(lambda  left,right: pd.merge(left,right,on=['SubPop','Period'], how='outer'), [u3_growth,u6_growth,u3_lfpr_growth,u6_lfpr_growth])


    u3_growth = growth(df,2007,2009,start_month=12,end_month=6,roll_avg=3,stat='U3_Rate')
    u6_growth = growth(df,2007,2009,start_month=12,end_month=6,roll_avg=3,stat='U6_Rate')
    u3_lfpr_growth = growth(df,2007,2009,start_month=12,end_month=6,roll_avg=3,stat='U3_LFPR')
    u6_lfpr_growth = growth(df,2007,2009,start_month=12,end_month=6,roll_avg=3,stat='U6_LFPR')

    growth_gr = reduce(lambda  left,right: pd.merge(left,right,on=['SubPop','Period'], how='outer'), [u3_growth,u6_growth,u3_lfpr_growth,u6_lfpr_growth])

    u3_growth = growth(df,2007,2020,start_month=12,end_month=6,roll_avg=3,stat='U3_Rate')
    u6_growth = growth(df,2007,2020,start_month=12,end_month=6,roll_avg=3,stat='U6_Rate')
    u3_lfpr_growth = growth(df,2007,2020,start_month=12,end_month=6,roll_avg=3,stat='U3_LFPR')
    u6_lfpr_growth = growth(df,2007,2020,start_month=12,end_month=6,roll_avg=3,stat='U6_LFPR')

    growth_gr_now = reduce(lambda  left,right: pd.merge(left,right,on=['SubPop','Period'], how='outer'), [u3_growth,u6_growth,u3_lfpr_growth,u6_lfpr_growth])


    #Periods to cover:
    #   -Full 20 years
    #   -begining of great recession until now
    #   -great recession
    #   -covid recession
    #   -dot com bubble

    #To add rolling avrage for months - turn months into ints, use between in mask?

    #Different periods of time (roughly): 2000-2007,2008-2012,2013-2020


    #Filter for specific subpops?
    #subpop = 'Sex'

    '''
    U3_growth = growth(start_year,end_year,period,stat='U3_weighted')
    U6_growth = growth(start_year,end_year,period,stat='U6')
    #num_total is not population - change this
    #Pop_growth = growth(start_year,end_year,period,subpop=subpop,stat='Num_total')
    LFPR_growth = growth(start_year,end_year,period,stat='LFPR')

    #Weighted Average - create separate function
    #Calculate CAGR

    growth_list = [UR_growth,U6_growth,LFPR_growth]
    growth = reduce(lambda  left,right: pd.merge(left,right,on=['SubPop','Start_year','End_year','Period'],how='outer'), growth_list)
    '''

    #Once identify interesting UR trend:
    #Look at pop percentages
    #Example: Black 20-29 No HS
    #Look at Percentage Schooling for black (and 20-29)

    #This gets you all rows with school and black in SubPop column; use Pop_percentage column!
    #df[df['SubPop'].str.contains('School')][df[df['SubPop'].str.contains('School')]['SubPop'].str.contains('Black')]
    
    '''
    #Black School Completed
    df_black_school = df[df['SubPop'].str.contains('School')][df[df['SubPop'].str.contains('School')]['SubPop'].str.contains('Black')]
    df_black_school = df_black_school[df_black_school['SubPop'].str.count(':')==2]

    plt.plot(df_black_school[(df_black_school['SubPop']=="'Race': 'Black Only', 'School_completed': 'Bachelors degree'")]['Pop_percentage'],label='Bachelors')

    plt.plot(df_black_school[(df_black_school['SubPop']=="'Race': 'Black Only', 'School_completed': 'High school degree'")]['Pop_percentage'],label='HS')

    plt.plot(df_black_school[(df_black_school['SubPop']=="'Race': 'Black Only', 'School_completed': 'No high school degree'")]['Pop_percentage'],label='No HS')

    plt.legend()

    plt.show()
    '''