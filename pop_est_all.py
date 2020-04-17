import os, pickle
import pandas as pd
import numpy as np
#Remove scientific notation
pd.set_option('display.float_format', lambda x: '%.1f' % x)


def process_90(df90):
    for i in range(1990,2001):
        num = str(i)[-2:]
        filename = 'e'+num+num+'smp.txt'
        df = pd.read_csv(filepath+folders[0]+'/'+subpop+'/'+filename,header=None)
        df = extract_record_90(df)

        df = df[df.Year!='2000']
        df.loc[(df['Age']=='999'),'Age']='-1'

        df90 = df90.append(df)
    return df90


def extract_record_90(df):
    col_dict = {'Month':(2,4),'Year':(4,8),'Age':(8,11),'Total_population':(12,22),'Total_male_population':(22,32),'Total_female_population':(32,42),'White_male_population':(42,52),'White_female_population':(52,62),'Black_male_population':(62,72),'Black_female_population':(72,82),'American_Indian_Eskimo_Aleut_male_population':(82,92),'American_Indian_Eskimo_Aleut_female_population':(92,102),'Asian_Pacific_Islander_male_population':(102,112),'Asian_Pacific_Islander_female_population':(112,122),'Hispanic_male_population':(122,132),'Hispanic_female_population':(132,142)}

    df_p = pd.DataFrame()


    for k,v in col_dict.items():
        df_p[k] = [i[0][v[0]:v[1]] for i in df.values]

    return df_p


def process_20plus(df):
    df = df[['MONTH', 'YEAR', 'AGE', 'TOT_POP', 'TOT_MALE', 'TOT_FEMALE', 'WA_MALE', 'WA_FEMALE', 'BA_MALE', 'BA_FEMALE', 'IA_MALE', 'IA_FEMALE', 'AA_MALE', 'AA_FEMALE', 'NA_MALE', 'NA_FEMALE','H_MALE','H_FEMALE']]

    df = df.rename(columns={'AGE':'Age','MONTH':'Month','YEAR':'Year','TOT_POP':'Total_population','TOT_MALE':'Total_male_population','TOT_FEMALE':'Total_female_population','WA_MALE':'White_male_population','WA_FEMALE':'White_female_population','BA_MALE':'Black_male_population','BA_FEMALE':'Black_female_population','IA_MALE':'American_Indian_Eskimo_Aleut_male_population','IA_FEMALE':'American_Indian_Eskimo_Aleut_female_population','AA_MALE':'Asian_male_population','AA_FEMALE':'Asian_female_population','NA_MALE':'Native_Hawaiian_Other_Pacific_Islander_male_population','NA_FEMALE':'Native_Hawaiian_Other_Pacific_Islander_female_population','H_MALE':'Hispanic_male_population','H_FEMALE':'Hispanic_female_population'})

    df.loc[(df['Age']==999),'Age']=-1
    df = df[df['Total_population']!='X']
    df = df[df.Month!=4.1]

    df['Asian_Pacific_Islander_male_population'] = df['Asian_male_population'].astype(int)+df['Native_Hawaiian_Other_Pacific_Islander_male_population'].astype(int)
    df['Asian_Pacific_Islander_female_population'] = df['Asian_female_population'].astype(int)+df['Native_Hawaiian_Other_Pacific_Islander_female_population'].astype(int)

    df.drop(['Asian_male_population','Asian_female_population','Native_Hawaiian_Other_Pacific_Islander_male_population','Native_Hawaiian_Other_Pacific_Islander_female_population'],axis=1,inplace=True)

    return df


def process_00(df00):
    for i in range(1,30):
        num = '0'+str(i) if len(str(i))<2 else str(i)
        filename = 'nc-est2009-alldata-n-file'+num+'.csv'
        df = pd.read_csv(filepath+folders[1]+'/'+subpop+'/'+filename)

        df00 = df00.append(df)

    df00 = process_20plus(df00)
    df00 = df00[(df00.Year<2010)|(df00.Month.isin([1.0,2.0,3.0]))]
    return df00


def process_10(df10):
    for i in range(1,21):
        num = '0'+str(i) if len(str(i))<2 else str(i)
        filename = 'nc-est2018-alldata-n-File'+num+'.csv'
        df = pd.read_csv(filepath+folders[2]+'/'+subpop+'/'+filename)

        df10 = df10.append(df)
    df10 = process_20plus(df10)
    return df10


def process_final_yr(df_list):
    df = pd.concat(df_list)
    df = df.astype(int)
    df = df[df.Age>15]
    df = pd.DataFrame(df.groupby('Year')[['Total_population','Total_male_population','Total_female_population','White_male_population','White_female_population','Black_male_population','Black_female_population','American_Indian_Eskimo_Aleut_male_population','American_Indian_Eskimo_Aleut_female_population','Asian_Pacific_Islander_male_population','Asian_Pacific_Islander_female_population','Hispanic_male_population','Hispanic_female_population']].mean()).reset_index()

    df = df.astype(int)
    df['Total_population'] = df['Total_male_population']+df['Total_female_population']
    return df

def process_final_month(df_list):
    df = pd.concat(df_list)
    df = df.astype(int)
    df = df[(df.Age>15)&(df.Year>2000)]

    df = pd.DataFrame(df.groupby(['Year','Month'])[['Total_population','Total_male_population','Total_female_population','White_male_population','White_female_population','Black_male_population','Black_female_population','American_Indian_Eskimo_Aleut_male_population','American_Indian_Eskimo_Aleut_female_population','Asian_Pacific_Islander_male_population','Asian_Pacific_Islander_female_population','Hispanic_male_population','Hispanic_female_population']].sum()).reset_index()

    df = df.astype(int)

    return df


def process_final_age(df_list):
    df = pd.concat(df_list)
    df = df.astype(int)
    df = df[(df.Age>15)&(df.Year>2000)]

    return df


def fips_mapper(df):
    df_fips = pd.read_excel('/home/dhense/PublicData/FIPS.xlsx')
    df_fips.FIPS = df_fips.FIPS.apply(lambda x: str(x).zfill(5))
    df_fips['State_and_county'] = df_fips.Name + ' County, ' + df_fips.State
    df_fips = df_fips[['FIPS','State_and_county']]
    df = df.merge(df_fips,on='FIPS',how='left')
    return df


def process_90c(df):
    col_dict = {'Year':(0,4),'FIPS':(5,10),'White_nonhispanic_population':(10,19),'Black_nonhispanic_population':(19,28),'American_Indian_Alaska_Native_nonhispanic_population':(28,37),'Asian_Pacific_Islander_nonhispanic_population':(37,46),'White_hispanic_population':(46,55),'Black_hispanic_population':(55,64),'American_Indian_Alaska_Native_hispanic_population':(64,73),'Asian_Pacific_Islander_hispanic_population':(73,82)}

    df_p = pd.DataFrame()


    for k,v in col_dict.items():
        df_p[k] = [i[0][v[0]:v[1]] for i in df.values]

    df_p = df_p.astype({'Year':int, 'White_nonhispanic_population':int, 'Black_nonhispanic_population':int, 'American_Indian_Alaska_Native_nonhispanic_population':int, 'Asian_Pacific_Islander_nonhispanic_population':int,'White_hispanic_population':int, 'Black_hispanic_population':int, 'American_Indian_Alaska_Native_hispanic_population':int, 'Asian_Pacific_Islander_hispanic_population':int})

    df_p['White_population'] = df_p['White_nonhispanic_population']+df_p['White_hispanic_population']

    df_p['Black_population'] = df_p['Black_nonhispanic_population']+df_p['Black_hispanic_population']

    df_p['American_Indian_Alaska_Native_population'] = df_p['American_Indian_Alaska_Native_nonhispanic_population']+df_p['American_Indian_Alaska_Native_hispanic_population']

    df_p['Asian_Pacific_Islander_population'] = df_p['Asian_Pacific_Islander_nonhispanic_population']+df_p['Asian_Pacific_Islander_hispanic_population']

    df_p['Hispanic_population'] = df_p['White_hispanic_population']+df_p['Black_hispanic_population']+df_p['American_Indian_Alaska_Native_hispanic_population']+df_p['Asian_Pacific_Islander_hispanic_population']

    df_p = fips_mapper(df_p)

    return df_p


def process_00c(df00c):
    for f in os.listdir(filepath+folders[1]+'/County'):
        dfc = pd.read_csv(filepath+folders[1]+'/County/'+f,encoding='latin-1')
        df00c = df00c.append(dfc)

    df00c = df00c[df00c.YEAR>2]
    df00c.YEAR = df00c.YEAR.replace({3:2000,4:2001,5:2002,6:2003,7:2004,8:2005,9:2006,10:2007,11:2008,12:2009})
    df00c = process_20plusc(df00c)
    return df00c


def process_10c(df10c):
    df10c = df10c[df10c.YEAR>2]
    df10c.YEAR = df10c.YEAR.replace({3:2010,4:2011,5:2012,6:2013,7:2014,8:2015,9:2016,10:2017,11:2018})
    df10c = process_20plusc(df10c)
    return df10c


def process_20plusc(df):
    df = df[['STATE', 'COUNTY','STNAME','CTYNAME','YEAR','AGEGRP', 'TOT_POP', 'TOT_MALE', 'TOT_FEMALE', 'WA_MALE', 'WA_FEMALE', 'BA_MALE', 'BA_FEMALE', 'IA_MALE', 'IA_FEMALE', 'AA_MALE', 'AA_FEMALE', 'NA_MALE', 'NA_FEMALE','H_MALE','H_FEMALE']]

    df = df.rename(columns={'STATE':'State_num','COUNTY':'County_num','STNAME':'State','CTYNAME':'County','YEAR':'Year','AGEGRP':'Age_group','TOT_POP':'Total_population','TOT_MALE':'Total_male_population','TOT_FEMALE':'Total_female_population','WA_MALE':'White_male_population','WA_FEMALE':'White_female_population','BA_MALE':'Black_male_population','BA_FEMALE':'Black_female_population','IA_MALE':'American_Indian_Eskimo_Aleut_male_population','IA_FEMALE':'American_Indian_Eskimo_Aleut_female_population','AA_MALE':'Asian_male_population','AA_FEMALE':'Asian_female_population','NA_MALE':'Native_Hawaiian_Other_Pacific_Islander_male_population','NA_FEMALE':'Native_Hawaiian_Other_Pacific_Islander_female_population','H_MALE':'Hispanic_male_population','H_FEMALE':'Hispanic_female_population'})

    df['FIPS'] = df['State_num'].apply(lambda x: str(x).zfill(2))+df['County_num'].apply(lambda x: str(x).zfill(3))

    df = fips_mapper(df)

    # df['Age_group'] = df['Age_group'].replace({0:'Total',1:'Age 0 to 4 years',2:'Age 5 to 9 years',3:'Age 10 to 14 years',4:'Age 15 to 19 years',5:'Age 20 to 24 years',6:'Age 25 to 29 years',7:'Age 30 to 34 years',8:'Age 35 to 39 years',9:'Age 40 to 44 years',10:'Age 45 to 49 years',11:'Age 50 to 54 years',12:'Age 55 to 59 years',13:'Age 60 to 64 years',14:'Age 65 to 69 years',15:'Age 70 to 74 years',16:'Age 75 to 79 years',17:'Age 80 to 84 years',18:'Age 85 years or older'})

    df['White_population'] = df['White_male_population']+df['White_female_population']

    df['Black_population'] = df['Black_male_population']+df['Black_female_population']

    df['American_Indian_Eskimo_Aleut_population'] = df['American_Indian_Eskimo_Aleut_male_population']+df['American_Indian_Eskimo_Aleut_female_population']

    df['Asian_Pacific_Islander_male_population'] = df['Asian_male_population']+df['Native_Hawaiian_Other_Pacific_Islander_male_population']

    df['Asian_Pacific_Islander_female_population'] = df['Asian_female_population']+df['Native_Hawaiian_Other_Pacific_Islander_female_population']
    df['Asian_Pacific_Islander_population'] = df['Asian_Pacific_Islander_male_population']+df['Asian_Pacific_Islander_female_population']

    df['Hispanic_population'] = df['Hispanic_male_population']+df['Hispanic_female_population']

    df = df[['State_and_county','Year','Age_group','Total_population','Total_male_population','Total_female_population','White_male_population','White_female_population','White_population','Black_male_population','Black_female_population','Black_population','American_Indian_Eskimo_Aleut_male_population','American_Indian_Eskimo_Aleut_female_population','American_Indian_Eskimo_Aleut_population','Asian_male_population','Asian_female_population','Native_Hawaiian_Other_Pacific_Islander_male_population','Native_Hawaiian_Other_Pacific_Islander_female_population','Asian_Pacific_Islander_male_population','Asian_Pacific_Islander_female_population','Asian_Pacific_Islander_population','Hispanic_male_population','Hispanic_female_population','Hispanic_population']]

    return df



if __name__=='__main__':
    picklepath = '/home/dhense/PublicData/Economic_analysis/'
    filepath = '/home/dhense/PublicData/popest/'
    folders = ['1990-2000','2000-2010','2010-2019']
    subpop = 'Civilian_noninstitutionalized'

    nat_pop_yr_pickle = 'nat_pop_yr.pickle'
    nat_pop_month_pickle = 'nat_pop_month.pickle'
    nat_pop_age_pickle = 'nat_pop_age.pickle'
    state_pop_pickle = 'state_pop.pickle'

################### National ###################

    #1990-2000
    df90 = pd.DataFrame()
    df90 = process_90(df90)

    #2000-2010
    df00 = pd.DataFrame()
    df00 = process_00(df00)

    #2010-2019
    df10 = pd.DataFrame()
    df10 = process_10(df10)

    df_list = [df90,df00,df10]
    #National yearly 1990-2019
    df_yr = process_final_yr(df_list)

    #National monthly 2001-2019
    df_month = process_final_month(df_list[1:])

    #National monthly by age 2001-2019
    df_age = process_final_age(df_list[1:])

    # print(df_yr.head())
    # print(df_month.head())

    print("...saving pickle")
    tmp = open(picklepath+'intermediate_files/'+nat_pop_yr_pickle,'wb')
    pickle.dump(df_yr,tmp)
    tmp.close()

    print("...saving pickle")
    tmp = open(picklepath+'intermediate_files/'+nat_pop_month_pickle,'wb')
    pickle.dump(df_month,tmp)
    tmp.close()

    print("...saving pickle")
    tmp = open(picklepath+'intermediate_files/'+nat_pop_age_pickle,'wb')
    pickle.dump(df_age,tmp)
    tmp.close()

################### States ####################

    # df00s = pd.read_csv('/home/dhense/PublicData/popest/2000-2010/st-est00int-alldata.csv')
    #
    # df00s = df00s[(df00s.STATE>0)&(df00s.DIVISION>0)&(df00s.SEX>0)&(df00s.ORIGIN>0)&(df00s.RACE>0)&(df00s.AGEGRP>0)]
    #
    # df00s = pd.DataFrame(df00s.groupby('STATE')[['POPESTIMATE2000','POPESTIMATE2001', 'POPESTIMATE2002', 'POPESTIMATE2003','POPESTIMATE2004', 'POPESTIMATE2005', 'POPESTIMATE2006','POPESTIMATE2007', 'POPESTIMATE2008', 'POPESTIMATE2009']].sum()).reset_index()
    #
    # df10s = pd.read_csv('/home/dhense/PublicData/popest/2010-2019/nst-est2019-alldata.csv')
    #
    # df10s = df10s[df10s.STATE.isin(np.arange(1,57))][['STATE','NAME','POPESTIMATE2010','POPESTIMATE2011','POPESTIMATE2012','POPESTIMATE2013','POPESTIMATE2014','POPESTIMATE2015','POPESTIMATE2016','POPESTIMATE2017','POPESTIMATE2018','POPESTIMATE2019']]
    #
    # df_state = df00s.merge(df10s,on='STATE',how='left')
    #
    # print("...saving pickle")
    # tmp = open(picklepath+'intermediate_files/'+state_pop_pickle,'wb')
    # pickle.dump(df_state,tmp)
    # tmp.close()

    df00s = pd.read_csv('/home/dhense/PublicData/popest/2000-2010/sc-est2009-alldata6-all.csv')
    df10s = pd.read_csv('/home/dhense/PublicData/popest/2010-2019/sc-est2018-alldata6.csv')

    df_state = df00s.merge(df10s,on=['STATE','SEX','ORIGIN','RACE','AGE'],how='left')[['STATE','SEX','ORIGIN','RACE','AGE','POPESTIMATE2000','POPESTIMATE2001', 'POPESTIMATE2002', 'POPESTIMATE2003','POPESTIMATE2004', 'POPESTIMATE2005', 'POPESTIMATE2006','POPESTIMATE2007', 'POPESTIMATE2008', 'POPESTIMATE2009','POPESTIMATE2010','POPESTIMATE2011','POPESTIMATE2012','POPESTIMATE2013','POPESTIMATE2014','POPESTIMATE2015','POPESTIMATE2016','POPESTIMATE2017','POPESTIMATE2018']]

    df_state = df_state[(df_state.STATE>0)&(df_state.SEX>0)&(df_state.ORIGIN>0)&(df_state.RACE>0)&(df_state.AGE>15)]

    sex_mapper = {1:'Male',2:'Female'}
    hispanic_mapper = {1:'Not Hispanic',2:'Hispanic'}
    race_mapper =  {1: 'White Only',2 :'Black Only',3:'Other',4:'Other',5:'Other',6:'Other'}

    df_state = df_state.replace({'SEX':sex_mapper, 'ORIGIN':hispanic_mapper, 'RACE':race_mapper})

    df_state = df_state.rename(columns={'AGE':'Age','RACE':'Race','SEX':'Sex','ORIGIN':'Hispanic'})


    print("...saving pickle")
    tmp = open(picklepath+'intermediate_files/'+state_pop_pickle,'wb')
    pickle.dump(df_state,tmp)
    tmp.close()

################### Counties ####################

    '''
    #1990-1999
    df90c = pd.read_csv(filepath+folders[0]+'/'+'CO-99-10.txt',skiprows=32,header=None)
    df90c = process_90c(df90c)

    # print(df90c.head())


    #2000-2010
    df00c = pd.DataFrame()
    df00c = process_00c(df00c)

    # print(df00c.head())

    #2010-2019
    df10c = pd.read_csv(filepath+folders[2]+'/County/cc-est2018-alldata.csv',encoding='latin-1')
    df10c = process_10c(df10c)

    # print(df10c.head())


    #Counties 2000-2018
    df_county = pd.concat([df00c,df10c])
    # print(df_county.head())

    #remove counties w/out full data throughout 2000-2018
    full_counties = df_county['State_and_county'].value_counts()[df_county['State_and_county'].value_counts()==361].index

    df_county = df_county[df_county['State_and_county'].isin(full_counties)]

    df_county_totals = df_county[df_county['Age_group']==0]

    #Age Groups: {0:'Total',1:'Age 0 to 4 years',2:'Age 5 to 9 years',3:'Age 10 to 14 years',4:'Age 15 to 19 years',5:'Age 20 to 24 years',6:'Age 25 to 29 years',7:'Age 30 to 34 years',8:'Age 35 to 39 years',9:'Age 40 to 44 years',10:'Age 45 to 49 years',11:'Age 50 to 54 years',12:'Age 55 to 59 years',13:'Age 60 to 64 years',14:'Age 65 to 69 years',15:'Age 70 to 74 years',16:'Age 75 to 79 years',17:'Age 80 to 84 years',18:'Age 85 years or older'}

    df_county_15plus = pd.DataFrame(df_county[df_county['Age_group']>3].groupby(['State_and_county','Year'])['Total_population','Total_male_population','Total_female_population','White_male_population','White_female_population','White_population','Black_male_population','Black_female_population','Black_population','American_Indian_Eskimo_Aleut_male_population','American_Indian_Eskimo_Aleut_female_population','American_Indian_Eskimo_Aleut_population','Asian_male_population','Asian_female_population','Native_Hawaiian_Other_Pacific_Islander_male_population','Native_Hawaiian_Other_Pacific_Islander_female_population','Asian_Pacific_Islander_male_population','Asian_Pacific_Islander_female_population','Asian_Pacific_Islander_population','Hispanic_male_population','Hispanic_female_population','Hispanic_population'].sum()).reset_index()

    print(df_county_15plus.head())
    '''
