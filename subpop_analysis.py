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


def growth(start_year,end_year,period,subpop=None,stat='UR'):
    '''
        Calculate growth rate for given statistic 'stat' on a frequency (e.g. yearly) 
        specified by 'period' between 'start_year' and 'end_year' for all subpopulations

        Must separate out cases where period does not divide evenly into time frame (end_year-start_year)
    '''
    yr_growth = pd.DataFrame()
    for s in df['SubPop'].unique():
        df_sub = df[df['SubPop']==s]

        df_sub = df_sub[(df_sub['Year']>=start_year) & (df_sub['Year']<=end_year)]

        if subpop:
            df_sub = df_sub[df_sub['SubPop'].str.contains(subpop)]

        ratio = int(math.ceil(len(df_sub['Year'].unique())/period))
        for yr in range(ratio):
            if len(df_sub['Year'].unique()) % period > 0:
                ur = ((df_sub[(df_sub['Year']==end_year-period+ratio+yr-1) & (df_sub['Month']=='dec')][stat].values[0])-(df_sub[(df_sub['Year']==start_year+yr) & (df_sub['Month']=='jan')][stat].values[0]))/(df_sub[(df_sub['Year']==start_year+yr) & (df_sub['Month']=='jan')][stat].values[0])
                yr_growth = yr_growth.append(pd.DataFrame([{'SubPop':s,'Start_year':start_year,'End_year':end_year,'Period':str(str(start_year+yr)+'-'+str(end_year-period+ratio+yr-1)),str(stat)+' Growth %'+' ('+str(period)+'YR)':ur*100}])).reset_index(drop=True)
            else:
                ur = ((df_sub[(df_sub['Year']==end_year-ratio+yr+1) & (df_sub['Month']=='dec')][stat].values[0])-(df_sub[(df_sub['Year']==start_year+yr) & (df_sub['Month']=='jan')][stat].values[0]))/(df_sub[(df_sub['Year']==start_year+yr) & (df_sub['Month']=='jan')][stat].values[0])
                yr_growth = yr_growth.append(pd.DataFrame([{'SubPop':s,'Start_year':start_year,'End_year':end_year,'Period':str(str(start_year+yr)+'-'+str(end_year-ratio+yr+1)),str(stat)+' Growth %'+' ('+str(period)+'YR)':ur*100}])).reset_index(drop=True)
    return yr_growth








if __name__=='__main__':
    pickle_path = '/home/dhense/PublicData/Economic_analysis/intermediate_files/cps/'

    print("...loading pickle")
    tmp = open('/home/dhense/PublicData/Economic_analysis/intermediate_files/subpop_data.pickle','rb')
    df = pickle.load(tmp)
    tmp.close()

    schema = StructType([
            StructField('Year', IntegerType(), True),StructField('Month', StringType(), True),StructField('SubPop', StringType(), True),StructField('Num_employed', IntegerType(), True),StructField('Num_unemployed', IntegerType(), True),StructField('Num_LF', IntegerType(), True),StructField('Num_total', IntegerType(), True),StructField('LFPR', FloatType(), True),StructField('UR', FloatType(), True),StructField('UR_weighted', FloatType(), True),StructField('U6', FloatType(), True),StructField('U6_weighted', FloatType(), True)
    ])


    period = 3
    start_year = 2000
    end_year = 2002

    # df_spark = sqlContext.read.format('csv').options(header='true', inferSchema='true').load('/home/dhense/PublicData/Economic_analysis/intermediate_files/dec2000.csv')
    # print(df_spark.show(5))

    #Different periods of time (roughly): 2000-2007,2008-2012,2013-2020


    #Filter for specific subpops?
    #subpop = 'Sex'

    '''
    UR_growth = growth(start_year,end_year,period,stat='UR',)
    U6_growth = growth(start_year,end_year,period,stat='U6')
    #num_total is not population - change this
    #Pop_growth = growth(start_year,end_year,period,subpop=subpop,stat='Num_total')
    LFPR_growth = growth(start_year,end_year,period,stat='LFPR')

    #Weighted Average - create separate function
    #Calculate CAGR

    growth_list = [UR_growth,U6_growth,LFPR_growth]
    growth = reduce(lambda  left,right: pd.merge(left,right,on=['SubPop','Start_year','End_year','Period'],how='outer'), growth_list)
    '''
        
    