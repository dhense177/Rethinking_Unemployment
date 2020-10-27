import numpy as np
import pandas as pd



def fips_mapper(df, level='State'):
    df_fips = pd.read_excel('/home/dhense/PublicData/FIPS.xlsx')
    df_fips.FIPS = df_fips.FIPS.apply(lambda x: str(x).zfill(5))
    if level == 'State':
        df_fips['State_FIPS'] = df_fips.FIPS.str[:2]
        df_fips = df_fips.groupby(['State','State_FIPS']).size().reset_index()[['State','State_FIPS']]
        df = df.merge(df_fips,on='State_FIPS',how='left')
    else:
        df_fips['State_and_county'] = df_fips.Name + ' County, ' + df_fips.State
        df_fips = df_fips[['FIPS','State_and_county']]
        df = df.merge(df_fips,on='FIPS',how='left')
    return df


def cut(arr):
    arr = arr.to_numpy()
    bins = np.empty(arr.shape[0])
    for idx, x in enumerate(arr):
        if (x >= 16) & (x < 20):
            bins[idx] = 1
        elif (x >= 20) & (x < 25):
            bins[idx] = 2
        elif (x >= 25) & (x < 30):
            bins[idx] = 3
        elif (x >= 30) & (x < 35):
            bins[idx] = 4
        elif (x >= 35) & (x < 40):
            bins[idx] = 5
        elif (x >= 40) & (x < 45):
            bins[idx] = 6
        elif (x >= 45) & (x < 50):
            bins[idx] = 7
        elif (x >= 50) & (x < 55):
            bins[idx] = 8
        elif (x >= 55) & (x < 60):
            bins[idx] = 9
        elif (x >= 60) & (x < 65):
            bins[idx] = 10
        elif (x >= 65) & (x < 70):
            bins[idx] = 11
        elif (x >= 70) & (x < 75):
            bins[idx] = 12
        elif (x >= 75) & (x < 80):
            bins[idx] = 13
        elif (x >= 80) & (x < 85):
            bins[idx] = 14
        else:
            bins[idx] = 15

    return bins.astype(int)
