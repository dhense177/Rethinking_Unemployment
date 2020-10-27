import os, pickle
import pandas as pd
import numpy as np


def process_ces(df_ces):
    df_ces.columns = df_ces.columns.map(str.strip)

    string_cols = ['series_id','year','period','value']
    for col in string_cols:
        df_ces[col] = df_ces[col].astype(str).str.strip()

    df_ces = df_ces.astype({'year':int,'value':int})

    #Filter for non-seasonally adjusted data
    df_ces = df_ces[df_ces['series_id'].isin(['CEU0000000001','CEU0000000010'])]
    df_ces = df_ces[df_ces['year']>1989]
    # df_ces = pd.DataFrame(df_ces.groupby(by=['series_id','year'])['value'].mean()).reset_index()

    # df_ces = df_ces.astype({'year':int,'value':int})

    return df_ces










if __name__=='__main__':
    url_path = 'https://download.bls.gov/pub/time.series/ce/'
    pickle_path = '/home/dhense/PublicData/Economic_analysis/intermediate_files/'
    ces_pickle = 'ces.pickle'

    df_ces = pd.read_csv(url_path+'ce.data.00a.TotalNonfarm.Employment',sep='\t')
    df_ces = process_ces(df_ces)

    print(df_ces.head())

    print("...saving pickle")
    tmp = open(pickle_path+ces_pickle,'wb')
    pickle.dump(df_ces,tmp)
    tmp.close()
