import os, pickle
import pandas as pd
import numpy as np




def process_icpsr(df_icpsr):
    df_icpsr = df_icpsr[(df_icpsr.STATEID==70)&(df_icpsr.YEAR>1994)][['YEAR','WHITEM','WHITEF','BLACKM','BLACKF','HISPM','HISPF','TOTRACEM','TOTRACEF']]

    df_icpsr['TOTWHITE'] = df_icpsr['WHITEM'] + df_icpsr['WHITEF']
    df_icpsr['TOTBLACK'] = df_icpsr['BLACKM'] + df_icpsr['BLACKF']
    df_icpsr['TOTHISP'] = df_icpsr['HISPM'] + df_icpsr['HISPF']

    df_icpsr['TOTRACE'] = df_icpsr['TOTRACEM'] + df_icpsr['TOTRACEF']

    df_icpsr = df_icpsr.rename(columns={'YEAR':'Year'})

    return df_icpsr


def process_corr(df_corr):
    df = pd.DataFrame()
    df['INCARCERATED'] = df_corr['Total/c']
    df['Year'] = np.arange(1980,2023)
    df = df[df.Year<2017][['Year','INCARCERATED']]
    return df








if __name__=='__main__':
    filepath = '/home/dhense/PublicData/Economic_analysis/Data/Unemployment_Analysis/'
    incarcerated_pickle = 'incarcerated.pickle'

    df_corr = pd.read_excel(filepath+'Total_correctional_population_counts_by_status.xlsx',skiprows=10)
    df_icpsr = pd.read_csv(filepath+'ICPSR_37003/DS0001/37003-0001-Data.tsv',delimiter='\t')

    df_icpsr = process_icpsr(df_icpsr)
    df_corr = process_corr(df_corr)

    df_incarcerated = df_icpsr.merge(df_corr,on='Year',how='left')


    print("...saving pickle")
    tmp = open('/home/dhense/PublicData/Economic_analysis/intermediate_files/'+incarcerated_pickle,'wb')
    pickle.dump(df_incarcerated,tmp)
    tmp.close()

    #total incarcerated = state prisons +local jails + federal prisons
    #non-violent drug related: 21.5% of total incarcerated
    #total non violent: 58% of total incarcerated
