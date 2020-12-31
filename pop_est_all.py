import os, pickle, requests
import pandas as pd
import numpy as np
from common_functions import fips_mapper, cut
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


def process_90c_overall(df):
    col_dict = {'Year':(0,2),'FIPS':(4,9),'Age':(10,12),'Race-Sex':(13,14),'Origin':(15,16),'Population':(16,23)}

    df_p = pd.DataFrame()


    for k,v in col_dict.items():
        df_p[k] = [i[0][v[0]:v[1]] for i in df.values]

    df_p = df_p.astype({'Year':int, 'Age':int, 'Race-Sex':int, 'Origin':int, 'Population':int})

    df_p['State_FIPS'] = df_p['FIPS'].str[0:2]
    df_p['County_FIPS'] = df_p['FIPS'].str[2:5]

    return df_p


def process_90c(county90_pop_pickle):
    if not os.path.isfile(picklepath+'intermediate_files/'+county90_pop_pickle):

        files = ['stch-icen'+str(yr)+'.txt' for yr in range(1990,2000)]
        df90c = pd.DataFrame()
        for f in files:
            df = pd.read_csv(filepath+folders[0]+'/County/'+f,header=None)
            # process_90c within process_90c? Something wrong...
            df = process_90c(df)
            df90c = df90c.append(df)

        print("...saving pickle")
        tmp = open(picklepath+'intermediate_files/'+county90_pop_pickle,'wb')
        pickle.dump(df90c,tmp)
        tmp.close()
    else:
        print("...loading pickle")
        tmp = open(picklepath+'intermediate_files/'+county90_pop_pickle,'rb')
        df90c = pickle.load(tmp)
        tmp.close()

    return df90c


def process_90s(df90c):
    df90c = df90c[df90c['Age']>3].rename(columns={'Age':'Age_group','Origin':'Hispanic'})

    df90c['Age_group'] = df90c['Age_group']-3
    df90c['Year'] = '19'+df90c['Year'].astype(str)
    df90c['Year'] = df90c['Year'].astype(int)

    origin_mapper = {1:'Not Hispanic',2:'Hispanic'}
    df90c = df90c.replace({'Hispanic':origin_mapper})

    df90c.loc[(df90c['Race-Sex'].isin([1,3,5,7])),'Sex'] = 'Male'
    df90c.loc[(df90c['Race-Sex'].isin([2,4,6,8])),'Sex'] = 'Female'

    df90c.loc[(df90c['Race-Sex'].isin([1,2])),'Race'] = 'White Only'
    df90c.loc[(df90c['Race-Sex'].isin([3,4])),'Race'] = 'Black Only'
    df90c.loc[(df90c['Race-Sex'].isin([5,6,7,8])),'Race'] = 'Other'

    df90s = df90c.groupby(['Year','State_FIPS','Sex','Race','Hispanic','Age_group'])['Population'].sum().reset_index()

    df90s = pd.pivot_table(df90s,values='Population',index=['State_FIPS','Sex','Race','Hispanic','Age_group'],columns='Year',aggfunc=np.sum).reset_index().rename(columns = {1990:'POPESTIMATE1990',1991:'POPESTIMATE1991',1992:'POPESTIMATE1992',1993:'POPESTIMATE1993',1994:'POPESTIMATE1994',1995:'POPESTIMATE1995',1996:'POPESTIMATE1996',1997:'POPESTIMATE1997',1998:'POPESTIMATE1998',1999:'POPESTIMATE1999'})

    return df90s


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

    df = fips_mapper(df,level='County')

    df['White_population'] = df['White_male_population']+df['White_female_population']

    df['Black_population'] = df['Black_male_population']+df['Black_female_population']

    df['American_Indian_Eskimo_Aleut_population'] = df['American_Indian_Eskimo_Aleut_male_population']+df['American_Indian_Eskimo_Aleut_female_population']

    df['Asian_Pacific_Islander_male_population'] = df['Asian_male_population']+df['Native_Hawaiian_Other_Pacific_Islander_male_population']

    df['Asian_Pacific_Islander_female_population'] = df['Asian_female_population']+df['Native_Hawaiian_Other_Pacific_Islander_female_population']
    df['Asian_Pacific_Islander_population'] = df['Asian_Pacific_Islander_male_population']+df['Asian_Pacific_Islander_female_population']

    df['Hispanic_population'] = df['Hispanic_male_population']+df['Hispanic_female_population']

    df = df[['State_and_county','Year','Age_group','Total_population','Total_male_population','Total_female_population','White_male_population','White_female_population','White_population','Black_male_population','Black_female_population','Black_population','American_Indian_Eskimo_Aleut_male_population','American_Indian_Eskimo_Aleut_female_population','American_Indian_Eskimo_Aleut_population','Asian_male_population','Asian_female_population','Native_Hawaiian_Other_Pacific_Islander_male_population','Native_Hawaiian_Other_Pacific_Islander_female_population','Asian_Pacific_Islander_male_population','Asian_Pacific_Islander_female_population','Asian_Pacific_Islander_population','Hispanic_male_population','Hispanic_female_population','Hispanic_population']]

    return df


def process_state(df90s,df00s,df10s):
    df_state = df00s.merge(df10s,on=['STATE','SEX','ORIGIN','RACE','AGE'],how='left')[['STATE','SEX','ORIGIN','RACE','AGE','POPESTIMATE2000','POPESTIMATE2001', 'POPESTIMATE2002', 'POPESTIMATE2003','POPESTIMATE2004', 'POPESTIMATE2005', 'POPESTIMATE2006','POPESTIMATE2007', 'POPESTIMATE2008', 'POPESTIMATE2009','POPESTIMATE2010','POPESTIMATE2011','POPESTIMATE2012','POPESTIMATE2013','POPESTIMATE2014','POPESTIMATE2015','POPESTIMATE2016','POPESTIMATE2017','POPESTIMATE2018']]

    df_state = df_state[(df_state.STATE>0)&(df_state.SEX>0)&(df_state.ORIGIN>0)&(df_state.RACE>0)&(df_state.AGE>15)]

    sex_mapper = {1:'Male',2:'Female'}
    hispanic_mapper = {1:'Not Hispanic',2:'Hispanic'}
    race_mapper =  {1: 'White Only',2 :'Black Only',3:'Other',4:'Other',5:'Other',6:'Other'}

    df_state = df_state.replace({'SEX':sex_mapper, 'ORIGIN':hispanic_mapper, 'RACE':race_mapper})

    df_state = df_state.rename(columns={'AGE':'Age','RACE':'Race','SEX':'Sex','ORIGIN':'Hispanic'})

    df_state['State_FIPS'] = df_state['STATE'].apply(lambda x: str(x).zfill(2))

    df_state = fips_mapper(df_state)

    df_state['Age_group'] = cut(df_state['Age'])
    df_state.drop(['State','Age'],axis=1,inplace=True)
    df_state = pd.DataFrame(df_state.groupby(by=['Age_group','Sex','Hispanic','Race','State_FIPS'])[['POPESTIMATE2000','POPESTIMATE2001','POPESTIMATE2002','POPESTIMATE2003','POPESTIMATE2004','POPESTIMATE2005','POPESTIMATE2006','POPESTIMATE2007','POPESTIMATE2008','POPESTIMATE2009','POPESTIMATE2010','POPESTIMATE2011','POPESTIMATE2012','POPESTIMATE2013','POPESTIMATE2014','POPESTIMATE2015','POPESTIMATE2016','POPESTIMATE2017','POPESTIMATE2018']].sum()).reset_index()

    df_state = df_state.merge(df90s,on=['State_FIPS','Age_group','Sex','Hispanic','Race'],how='left')[['Age_group','Sex','Hispanic','Race','State_FIPS','POPESTIMATE1990','POPESTIMATE1991','POPESTIMATE1992','POPESTIMATE1993','POPESTIMATE1994','POPESTIMATE1995','POPESTIMATE1996','POPESTIMATE1997','POPESTIMATE1998','POPESTIMATE1999','POPESTIMATE2000','POPESTIMATE2001','POPESTIMATE2002','POPESTIMATE2003','POPESTIMATE2004','POPESTIMATE2005','POPESTIMATE2006','POPESTIMATE2007','POPESTIMATE2008','POPESTIMATE2009','POPESTIMATE2010','POPESTIMATE2011','POPESTIMATE2012','POPESTIMATE2013','POPESTIMATE2014','POPESTIMATE2015','POPESTIMATE2016','POPESTIMATE2017','POPESTIMATE2018']]

    print("...saving pickle")
    tmp = open(picklepath+'intermediate_files/'+state_pop_pickle,'wb')
    pickle.dump(df_state,tmp)
    tmp.close()

    return df_state

def process_proj(df_proj):
    df_proj.drop(['POP_'+str(i) for i in range(0,16)],axis=1, inplace=True)

    df_proj['1'] = df_proj['POP_16']+df_proj['POP_17']+df_proj['POP_18']+df_proj['POP_19']
    df_proj['2'] = df_proj['POP_20']+df_proj['POP_21']+df_proj['POP_22']+df_proj['POP_23']+df_proj['POP_24']
    df_proj['3'] = df_proj['POP_25']+df_proj['POP_26']+df_proj['POP_27']+df_proj['POP_28']+df_proj['POP_29']
    df_proj['4'] = df_proj['POP_30']+df_proj['POP_31']+df_proj['POP_32']+df_proj['POP_33']+df_proj['POP_34']
    df_proj['5'] = df_proj['POP_35']+df_proj['POP_36']+df_proj['POP_37']+df_proj['POP_38']+df_proj['POP_39']
    df_proj['6'] = df_proj['POP_40']+df_proj['POP_41']+df_proj['POP_42']+df_proj['POP_43']+df_proj['POP_44']
    df_proj['7'] = df_proj['POP_45']+df_proj['POP_46']+df_proj['POP_47']+df_proj['POP_48']+df_proj['POP_49']
    df_proj['8'] = df_proj['POP_50']+df_proj['POP_51']+df_proj['POP_52']+df_proj['POP_53']+df_proj['POP_54']
    df_proj['9'] = df_proj['POP_55']+df_proj['POP_56']+df_proj['POP_57']+df_proj['POP_58']+df_proj['POP_59']
    df_proj['10'] = df_proj['POP_60']+df_proj['POP_61']+df_proj['POP_62']+df_proj['POP_63']+df_proj['POP_64']
    df_proj['11'] = df_proj['POP_65']+df_proj['POP_66']+df_proj['POP_67']+df_proj['POP_68']+df_proj['POP_69']
    df_proj['12'] = df_proj['POP_70']+df_proj['POP_71']+df_proj['POP_72']+df_proj['POP_73']+df_proj['POP_74']
    df_proj['13'] = df_proj['POP_75']+df_proj['POP_76']+df_proj['POP_77']+df_proj['POP_78']+df_proj['POP_79']
    df_proj['14'] = df_proj['POP_80']+df_proj['POP_81']+df_proj['POP_82']+df_proj['POP_83']+df_proj['POP_84']
    df_proj['15'] = df_proj['POP_85']+df_proj['POP_86']+df_proj['POP_87']+df_proj['POP_88']+df_proj['POP_89']+df_proj['POP_90']+df_proj['POP_91']+df_proj['POP_92']+df_proj['POP_93']+df_proj['POP_94']+df_proj['POP_95']+df_proj['POP_96']+df_proj['POP_97']+df_proj['POP_98']+df_proj['POP_99']+df_proj['POP_100']
                
    df_proj.drop(['POP_'+str(i) for i in range(16,101)],axis=1, inplace=True)

    sex_mapper = {1:'Male',2:'Female'}
    hispanic_mapper = {1:'Not Hispanic',2:'Hispanic'}
    race_mapper = {1:'White Only',2:'Black Only',3:'Other',4:'Other',5:'Other',6:'Other'}

    df_proj = df_proj.replace({'SEX':sex_mapper, 'ORIGIN':hispanic_mapper, 'RACE':race_mapper})
    df_proj = df_proj[(df_proj['SEX']!=0)&(df_proj['ORIGIN']!=0)&(df_proj['RACE'].isin([0,7,8,9,10,11])==False)]

    df_proj.drop('TOTAL_POP',axis=1,inplace=True)
    df_proj = pd.melt(df_proj,id_vars=['SEX','ORIGIN','RACE','YEAR'], value_vars=['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15']).rename(columns={'variable':'Age_group','value':'POPESTIMATE_SUB','SEX':'Sex','ORIGIN':'Hispanic','RACE':'Race','YEAR':'Year'})

    df_proj = pd.DataFrame(df_proj.groupby(['Sex','Race','Hispanic','Year','Age_group'])['POPESTIMATE_SUB'].sum()).reset_index()

    # print("...saving pickle")
    # tmp = open(picklepath+'intermediate_files/'+proj_pickle,'wb')
    # pickle.dump(df_proj,tmp)
    # tmp.close()

    df_proj.to_csv('/home/dhense/PublicData/Economic_analysis/popproj.csv',index=False)

    return df_proj


if __name__=='__main__':
    picklepath = '/home/dhense/PublicData/Economic_analysis/'
    filepath = '/home/dhense/PublicData/popest/'
    folders = ['1990-2000','2000-2010','2010-2019']
    subpop = 'Civilian_noninstitutionalized'

    nat_pop_yr_pickle = 'nat_pop_yr.pickle'
    nat_pop_month_pickle = 'nat_pop_month.pickle'
    nat_pop_age_pickle = 'nat_pop_age.pickle'
    state_pop_pickle = 'state_pop.pickle'
    county90_pop_pickle = 'county90.pickle'
    proj_pickle = 'projected.pickle'

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

################### States and Counties #################

    df90c = process_90c(county90_pop_pickle)
    df90s = process_90s(df90c)
    df00s = pd.read_csv('/home/dhense/PublicData/popest/2000-2010/sc-est2009-alldata6-all.csv')
    df10s = pd.read_csv('/home/dhense/PublicData/popest/2010-2019/sc-est2018-alldata6.csv')

    df_state = process_state(df90s,df00s,df10s)


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
############## Projections ####################
    df_proj  = pd.read_csv('/home/dhense/PublicData/popproj/np2017_d1.csv')
    df_proj = process_proj(df_proj)