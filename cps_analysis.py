import os, pickle, time, math
import pandas as pd
import numpy as np
from itertools import combinations, product, chain
import warnings
warnings.filterwarnings('ignore')

def avg_part_time(df_cps):
    hours = df_cps[(df_cps['FT_PT']=='Part_time')&(df_cps['Hours_per_week_last']!='-1')]['Hours_per_week_last'].value_counts().keys().tolist()

    counts = df_cps[(df_cps['FT_PT']=='Part_time')&(df_cps['Hours_per_week_last']!='-1')]['Hours_per_week_last'].value_counts().tolist()

    total_count = np.sum(counts)

    hour_dict = dict(zip(hours,counts/total_count))

    avg_hours = np.sum([k*v for k,v in hour_dict.items()])

    return avg_hours


def calc_urate(df_cps,recode_counts,type):
    if type =='U3':
        urate = (recode_counts['Unemployed - on layoff']+recode_counts['Unemployed - looking'])/(recode_counts['Employed - at work']+recode_counts['Employed - absent']+recode_counts['Unemployed - on layoff']+recode_counts['Unemployed - looking'])
    else:
        # df_cps['Unemployed_U6'] = np.where(((df_cps['LF_recode'].isin(['Unemployed - on layoff','Unemployed - looking']))|(df_cps['Look_last_year']=='Yes')|(df_cps['Look_last_month']=='Yes')|(df_cps['Job_offered_week']=='Yes')|(df_cps['Last_work']=='Within Last 12 Months')|(df_cps['Reason_not_looking'].isin(['Believes No Work Available In Area Of Expertise','Couldnt Find Any Work','Lacks Necessary Schooling/Training']))|(df_cps['Discouraged']=='Yes')|(df_cps['FT_PT_status'].isin(['PT Hrs, Usually Pt For Economic Reasons','PT for Economic Reasons, Usually Ft']))),'Yes','No')

        num_unemployed = len(df_cps[df_cps['Unemployed_U6']=='Yes'])

        num_employed = len(df_cps[(df_cps['LF_recode'].isin(['Employed - at work','Employed - absent']))&(df_cps['Unemployed_U6']=='No')])

        urate = num_unemployed/(num_employed+num_unemployed)

    return urate

def weight(df_cps):
 
    df_cps['Percent'+str(year)]=df_cps['POPESTIMATE'+str(year)]/df_state['POPESTIMATE'+str(year)].sum()

    df_cps['Count'] = np.arange(1,len(df_cps)+1)

    weight_mapper = df_cps.groupby(by=['Age_group','Sex','Race','Hispanic','State_FIPS'])['Count'].count().reset_index().rename(columns={'Count':'Ratio'})

    weight_mapper['Ratio'] = weight_mapper['Ratio']/len(df_cps)

    df_cps = df_cps.merge(weight_mapper,on=['Sex','Race','Age_group','Hispanic','State_FIPS'],how='left',left_index=True)

    #No detailed population data for 2019 - use 2018
    # if year == 2019:
    #     df_cps['Weight'] = df_cps['Percent2018']/df_cps['Ratio']
    # else:
    #     df_cps['Weight'] = df_cps['Percent'+str(year)]/df_cps['Ratio']
    df_cps['Weight'] = df_cps['Percent'+str(year)]/df_cps['Ratio']

    df_cps.drop(['Count','Ratio'],axis=1,inplace=True)

    #Try adjusting weights so they add up to length of df_cps
    #Doesn't seem to make much of a difference either way
    df_cps['Weight'] = df_cps['Weight']*(len(df_cps)/df_cps['Weight'].sum())

    return df_cps


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

'''
def process_fred(filepath):
    df_fred = pd.read_excel(filepath,skiprows=10)

    df_fred['Year'] = df_fred['observation_date'].astype(str).str[:4].astype(int)
    df_fred['Month'] = df_fred['observation_date'].astype(str).str[5:7].astype(int)

    df_fred = df_fred[(df_fred.Year>1994)&(df_fred.Year<2020)]

    df_fred.drop('observation_date',axis=1,inplace=True)

    return df_fred

def add_urates_ns(df_urates):

    df_fred_u3 = process_fred('/home/dhense/PublicData/Economic_analysis/Data/Unemployment_Analysis/UNRATENSA.xls')
    df_fred_u6 = process_fred('/home/dhense/PublicData/Economic_analysis/Data/Unemployment_Analysis/U6RATENSA.xls')

    df_urates = df_urates.merge(df_fred_u3,on=['Year','Month'],how='left')
    df_urates['UR_weighted'] = round(df_urates['UR_weighted']*100,1)

    df_urates = df_urates.merge(df_fred_u6,on=['Year','Month'],how='left')
    df_urates['U6_weighted'] = round(df_urates['U6_weighted']*100,1)

    return df_urates


# def add_u6(df_urates):
#     df_fred_u6 = process_fred('/home/dhense/PublicData/Economic_analysis/Data/Unemployment_Analysis/U6RATENSA.xls')
#     df_urates = df_urates.merge(df_fred_u6,on=['Year','Month'],how='left')
#     df_urates['U6_weighted'] = round(df_urates['U6_weighted']*100,1)


def add_seasonal(df_urates):
    df_seas_u3 = process_fred('/home/dhense/PublicData/Economic_analysis/Data/Unemployment_Analysis/UNRATE.xls')
    df_urates = df_urates.merge(df_seas_u3, on=['Year','Month'],how='left')


    df_urates_seas_u3 = df_urates[['Year','Month','UR_weighted']]
    df_urates_seas_u3['Day'] = 1
    df_urates_seas_u3['Date'] = pd.to_datetime(df_urates_seas_u3[['Year','Month','Day']])
    df_urates_seas_u3.drop(['Year','Month','Day'],axis=1,inplace=True)
    df_urates_seas_u3 = df_urates_seas_u3.set_index('Date')

    s_u3 = seasonal_decompose(df_urates_seas_u3)
    df_urates['UR_weighted_seasoned'] = s_u3.trend.values
    df_urates['UR_weighted_seasoned'] = round(df_urates['UR_weighted_seasoned'],1)


    df_seas_u6 = process_fred('/home/dhense/PublicData/Economic_analysis/Data/Unemployment_Analysis/U6RATE.xls')
    df_urates = df_urates.merge(df_seas_u6, on=['Year','Month'],how='left')

    df_urates_seas_u6 = df_urates[['Year','Month','U6_weighted']]
    df_urates_seas_u6['Day'] = 1
    df_urates_seas_u6['Date'] = pd.to_datetime(df_urates_seas_u6[['Year','Month','Day']])
    df_urates_seas_u6.drop(['Year','Month','Day'],axis=1,inplace=True)
    df_urates_seas_u6 = df_urates_seas_u6.set_index('Date')

    s_u6 = seasonal_decompose(df_urates_seas_u6)
    df_urates['U6_weighted_seasoned'] = s_u6.trend.values
    df_urates['U6_weighted_seasoned'] = round(df_urates['U6_weighted_seasoned'],1)

    df_urates_seas_self = df_urates[['Year','Month','Self_rate_weighted']]
    df_urates_seas_self['Day'] = 1
    df_urates_seas_self['Date'] = pd.to_datetime(df_urates_seas_self[['Year','Month','Day']])
    df_urates_seas_self.drop(['Year','Month','Day'],axis=1,inplace=True)
    df_urates_seas_self = df_urates_seas_self.set_index('Date')

    s_self = seasonal_decompose(df_urates_seas_self)
    df_urates['Self_rate_weighted_seasoned'] = s_self.trend.values
    df_urates['Self_rate_weighted_seasoned'] = round(df_urates['Self_rate_weighted_seasoned'],1)

    return df_urates


def add_subpops(data, k, df):
    recode_counts = dict(zip(df['LF_recode'].value_counts().keys().tolist(),df['LF_recode'].value_counts().tolist()))

    #Set all values that don't appear in recode_counts to zero
    recode_keys = ['Employed - at work', 'Not in labor force - retired', 'Not in labor force - other', 'Not in labor force - disabled', 'Unemployed - looking', 'Employed - absent', 'Unemployed - on layoff']

    for r in recode_keys:
        if r not in recode_counts.keys():
            recode_counts[r] = 0

    # print (k)
    # print (recode_counts)

    urate_sp = calc_urate(df,recode_counts,'U3')
    urate_weighted_sp = calc_urate_weighted(df,'U3')


    #2.56% diff on 1999-2018
    df['Unemployed_U6'] = np.where(((df['LF_recode'].isin(['Unemployed - on layoff','Unemployed - looking']))|(df['Look_last_year']=='Yes')|(df['Look_last_month']=='Yes')|(df['Job_offered_week']=='Yes')|(df['Last_work']=='Within Last 12 Months')|(df['Reason_not_looking'].isin(['Believes No Work Available In Area Of Expertise','Couldnt Find Any Work','Lacks Necessary Schooling/Training']))|(df['Discouraged']=='Yes')|(df['FT_PT_status'].isin(['PT Hrs, Usually Pt For Economic Reasons','PT for Economic Reasons, Usually Ft']))),'Yes','No')


    U6_rate_sp = calc_urate(df,recode_counts,'U6')
    U6_weighted_sp = calc_urate_weighted(df,'U6')

    self_rate_sp = self_calc(df,k)
    self_rate_weight_sp = self_calc_weight(df,k)

    data[0]['urate_'+k] = urate_sp
    data[0]['urate_weighted_'+k] = urate_weighted_sp
    data[0]['U6_rate_'+k] = U6_rate_sp
    data[0]['U6_weighted_'+k] = U6_weighted_sp
    data[0]['Self_rate_'+k] = self_rate_sp
    data[0]['Self_rate_weighted_'+k] = self_rate_weight_sp

    return data


def self_calc(df_cps,k):
    #Remove full-time students who also work full time as well as high school students
    df_self = df_cps[(df_cps['School_type']!='High School')&~((df_cps['In_school_ft_pt']=='Full-time')&(df_cps['FT_PT']=='Full_time'))]
    unemployed, only_disabled = self_calc_vars(df_self)

    pt_counts = df_self[df_self['FT_PT']=='Part_time']['Main_reason_part_time'].value_counts()

    #Set all values that don't appear in pt_counts to zero
    pt_keys = ['Slack Work/Business Conditions', 'Could Only Find Part-time Work', 'Seasonal Work', 'Child Care Problems', 'Health/Medical Limitations', 'Full-Time Workweek Is Less Than 35 Hrs','Retired/Social Security Limit On Earnings']

    for r in pt_keys:
        if r not in pt_counts.keys():
            pt_counts[r] = 0

    pt_perc = (pt_counts['Slack Work/Business Conditions']+pt_counts['Could Only Find Part-time Work']+pt_counts['Seasonal Work']+pt_counts['Child Care Problems']+pt_counts['Health/Medical Limitations']+pt_counts['Full-Time Workweek Is Less Than 35 Hrs']+pt_counts['Retired/Social Security Limit On Earnings'])/(pt_counts.sum()-pt_counts['-1'])

    underemployment_perc = 1-(avg_part_time(df_self)/35)

    num_pt = len(df_self[df_self['FT_PT']=='Part_time'])*pt_perc*underemployment_perc

    num_incarcerated = incarcerated_calc(df_cps,k)


    num_unemployed = len(unemployed)-(len(only_disabled)*.714)+num_pt+num_incarcerated*.215
    num_employed = len(df_self[df_self['LF_recode'].isin(['Employed - at work','Employed - absent'])])

    self_rate = num_unemployed/(num_unemployed+num_employed)

    return self_rate


def self_calc_weight(df_cps,k):
    #Weighted
    df_self = df_cps[df_cps['In_school_ft_pt']!='Full-time']
    df_self = weight(df_self)
    unemployed, only_disabled = self_calc_vars(df_self)

    pt_counts = df_self[df_self['FT_PT']=='Part_time'].groupby('Main_reason_part_time')['Weight'].sum()

    #Set all values that don't appear in pt_counts to zero
    pt_keys = ['Slack Work/Business Conditions', 'Could Only Find Part-time Work', 'Seasonal Work', 'Child Care Problems', 'Health/Medical Limitations', 'Full-Time Workweek Is Less Than 35 Hrs','Retired/Social Security Limit On Earnings']

    for r in pt_keys:
        if r not in pt_counts.keys():
            pt_counts[r] = 0

    pt_perc = (pt_counts['Slack Work/Business Conditions']+pt_counts['Could Only Find Part-time Work']+pt_counts['Seasonal Work']+pt_counts['Child Care Problems']+pt_counts['Health/Medical Limitations']+pt_counts['Full-Time Workweek Is Less Than 35 Hrs']+pt_counts['Retired/Social Security Limit On Earnings'])/(pt_counts.sum()-pt_counts['-1'])

    underemployment_perc = 1-(avg_part_time(df_self)/35)

    num_pt = (df_self[df_self['FT_PT']=='Part_time']['Weight'].sum())*pt_perc*underemployment_perc

    num_incarcerated = incarcerated_calc(df_cps,k)

    num_unemployed = unemployed['Weight'].sum()-(only_disabled['Weight'].sum()*.714)+num_pt+num_incarcerated*.215

    # num_unemployed = unemployed['Weight'].sum()-(only_disabled['Weight'].sum()*.714)+num_pt

    #Subtract part-time percentage from employed total
    num_employed = df_self.groupby('LF_recode')['Weight'].sum()[['Employed - at work','Employed - absent']].sum()-num_pt



    self_rate = num_unemployed/(num_unemployed+num_employed)

    return self_rate


def self_calc_vars(df_self):
    unemployed = df_self[(df_self['LF_recode'].isin(['Employed - at work','Employed - absent'])==False)&((df_self['LF_recode'].isin(['Unemployed - on layoff','Unemployed - looking']))|(df_self['Look_last_year']=='Yes')|(df_self['Look_last_month']=='Yes')|(df_self['Job_offered_week']=='Yes')|(df_self['Last_work']=='Within Last 12 Months')|(df_self['Reason_not_looking'].isin(['Believes No Work Available In Area Of Expertise','Couldnt Find Any Work','Lacks Necessary Schooling/Training','Employers Think Too Young Or Too Old','Other Types Of Discrimination','Cant Arrange Child Care']))|(df_self['Discouraged']=='Yes')|(df_self['Disabled']=='Yes'))]

    #Is this calc right??
    #Get rid of 71.4% of disabled who dont meet other criteria given that 28.6% of disabled are due to work-related matters
    only_disabled = df_self[(df_self['Disabled']=='Yes')&((df_self['LF_recode'].isin(['Employed - at work','Employed - absent'])==False)&(df_self['LF_recode'].isin(['Unemployed - on layoff','Unemployed - looking'])==False)&(df_self['Look_last_year']!='Yes')&(df_self['Look_last_month']!='Yes')&(df_self['Job_offered_week']!='Yes')&(df_self['Last_work']!='Within Last 12 Months')&(df_self['Reason_not_looking'].isin(['Believes No Work Available In Area Of Expertise','Couldnt Find Any Work','Lacks Necessary Schooling/Training','Employers Think Too Young Or Too Old','Other Types Of Discrimination','Cant Arrange Child Care'])==False)&(df_self['Discouraged']!='Yes'))]

    return unemployed, only_disabled


def incarcerated_calc(df_cps,k):
    # df_cps = df_cps.merge(df_state,on=['Sex','Race','Age_group','Hispanic','State_FIPS'],how='left',left_index=True)

    pop_ratio = df_cps['POPESTIMATE'+str(year)].sum()/len(df_cps)
    num_incarcerated = df_incarcerated[df_incarcerated.Year==year][incarceration_dict[k]].values[0]/pop_ratio

    return num_incarcerated


def calc_percentages(df_cps):
    ##### Total #####
    nilf = df_cps[df_cps['LF_recode'].isin(['Not in labor force - retired','Not in labor force - other','Not in labor force - disabled'])]
    perc_nilf = nilf['Weight'].sum()/len(df_cps)
    perc_retired_nilf = nilf[nilf['Retired']=='Yes']['Weight'].sum()/len(df_cps)
    perc_disabled_nilf = nilf[nilf['Disabled']=='Yes']['Weight'].sum()/len(df_cps)
    perc_student_nilf = nilf[nilf['In_school']=='Yes']['Weight'].sum()/len(df_cps)

    perc_looking = df_cps[df_cps['LF_recode']=='Unemployed - looking']['Weight'].sum()/len(df_cps)
    perc_layoff = df_cps[df_cps['LF_recode']=='Unemployed - on layoff']['Weight'].sum()/len(df_cps)
    perc_unemployed = perc_looking+perc_layoff

    perc_employed = df_cps[df_cps['LF_recode'].isin(['Employed - at work','Employed - absent'])]['Weight'].sum()/len(df_cps)
    perc_employed_ft = df_cps[df_cps['FT_PT']=='Full_time']['Weight'].sum()/len(df_cps)
    perc_employed_pt = df_cps[df_cps['FT_PT']=='Part_time']['Weight'].sum()/len(df_cps)

    perc_employed_ft_student = df_cps[(df_cps['FT_PT']=='Full_time')&(df_cps['In_school']=='Yes')]['Weight'].sum()/len(df_cps)
    perc_employed_pt_student = df_cps[(df_cps['FT_PT']=='Part_time')&(df_cps['In_school']=='Yes')]['Weight'].sum()/len(df_cps)
    # perc_employed_ft_student_ft = df_cps[(df_cps['FT_PT']=='Full_time')&(df_cps['In_school_ft_pt']=='Full-time')]['Weight'].sum()/len(df_cps)
    # perc_employed_ft_student_pt = df_cps[(df_cps['FT_PT']=='Full_time')&(df_cps['In_school_ft_pt']=='Part-time')]['Weight'].sum()/len(df_cps)
    # perc_employed_pt_student_ft = df_cps[(df_cps['FT_PT']=='Part_time')&(df_cps['In_school_ft_pt']=='Full-time')]['Weight'].sum()/len(df_cps)
    # perc_employed_pt_student_pt = df_cps[(df_cps['FT_PT']=='Part_time')&(df_cps['In_school_ft_pt']=='Part-time')]['Weight'].sum()/len(df_cps)

    ##### Labor Force #######
    lf = df_cps[df_cps['LF_recode'].isin(['Employed - at work','Employed - absent','Unemployed - looking','Unemployed - on layoff'])]['Weight'].sum()

    looking_lf = df_cps[df_cps['LF_recode']=='Unemployed - looking']['Weight'].sum()/lf
    layoff_lf = df_cps[df_cps['LF_recode']=='Unemployed - on layoff']['Weight'].sum()/lf
    unemployed_lf = looking_lf+layoff_lf

    employed_ft_lf = df_cps[df_cps['FT_PT']=='Full_time']['Weight'].sum()/lf
    employed_pt_lf = df_cps[df_cps['FT_PT']=='Part_time']['Weight'].sum()/lf
    employed_lf = employed_ft_lf+employed_pt_lf

    employed_pt_econ_lf = df_cps[(df_cps['FT_PT']=='Part_time')&(df_cps['FT_PT_status'].isin(['PT Hrs, Usually Pt For Economic Reasons','FT_Hours, Usually PT For Economic Reasons']))]['Weight'].sum()/lf

    employed_pt_nonecon_lf = df_cps[(df_cps['FT_PT']=='Part_time')&(df_cps['FT_PT_status'].isin(['PT Hrs, Usually Pt For Economic Reasons','FT_Hours, Usually PT For Economic Reasons'])==False)]['Weight'].sum()/lf


    return [perc_nilf,perc_retired_nilf,perc_disabled_nilf,perc_student_nilf,perc_unemployed,perc_looking,perc_layoff,perc_employed,perc_employed_ft,perc_employed_pt,perc_employed_ft_student,perc_employed_pt_student,looking_lf,layoff_lf,unemployed_lf,employed_ft_lf,employed_pt_lf,employed_lf,employed_pt_econ_lf,employed_pt_nonecon_lf]
'''

















if __name__=='__main__':
    import_path = '/home/dhense/PublicData/Economic_analysis/intermediate_files/cps_csv/'
    export_path = '/home/dhense/PublicData/Economic_analysis/'



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
    # age_list = [{'Age_group': 2},{'Age_group': 3},{'Age_group': 4},{'Age_group': 5},{'Age_group': 6},{'Age_group': 7},{'Age_group': 8},{'Age_group': 9},{'Age_group': 10}]
    #Combine both married items into one? Add absent to separated?
    marital_list = [{'Marital_status': 'Married'},{'Marital_status': 'Never married'},{'Marital_status': 'Divorced'},{'Marital_status': 'Widowed'},{'Marital_status': 'Separated'}]
    #simplify school completed to fewer categories
    school_list = [{'School_completed': 'High school degree'},{'School_completed': 'No high school degree'},{'School_completed': 'Bachelors degree'},{'School_completed': 'Masters degree'},{'School_completed': 'Associate degree'},{'School_completed': 'Professional school degree'},{'School_completed': 'Doctorate degree'}]
    division_list = [{'Division': 'South Atlantic'},{'Division': 'East North Central'},{'Division': 'Mid-Atlantic'},{'Division': 'Pacific'},{'Division': 'Mountain'},{'Division': 'West South Central'},{'Division': 'West North Central'},{'Division': 'New England'},{'Division': 'East South Central'}]
    state_list = [{'State_FIPS': '06'},{'State_FIPS': '36'},{'State_FIPS': '12'},{'State_FIPS': '48'},{'State_FIPS': '42'},{'State_FIPS': '17'},{'State_FIPS': '39'},{'State_FIPS': '26'},{'State_FIPS': '34'},{'State_FIPS': '37'},{'State_FIPS': '25'},{'State_FIPS': '13'},{'State_FIPS': '04'},{'State_FIPS': '51'},{'State_FIPS': '40'},{'State_FIPS': '01'},{'State_FIPS': '54'},{'State_FIPS': '16'},{'State_FIPS': '32'},{'State_FIPS': '27'},{'State_FIPS': '22'},{'State_FIPS': '08'},{'State_FIPS': '55'},{'State_FIPS': '47'},{'State_FIPS': '30'},{'State_FIPS': '05'},{'State_FIPS': '35'},{'State_FIPS': '31'},{'State_FIPS': '21'},{'State_FIPS': '20'},{'State_FIPS': '49'},{'State_FIPS': '46'},{'State_FIPS': '18'},{'State_FIPS': '19'},{'State_FIPS': '53'},{'State_FIPS': '38'},{'State_FIPS': '56'},{'State_FIPS': '41'},{'State_FIPS': '29'},{'State_FIPS': '45'},{'State_FIPS': '02'},{'State_FIPS': '28'},{'State_FIPS': '24'},{'State_FIPS': '23'},{'State_FIPS': '33'},{'State_FIPS': '44'},{'State_FIPS': '09'},{'State_FIPS': '50'},{'State_FIPS': '10'},{'State_FIPS': '11'},{'State_FIPS': '15'}]
    #disabled_list = [{'Disabled':i} for i in list(df['Disabled'].value_counts().index)]
    #duty_list = list(df['Ever_active_duty'].value_counts().index)

    all_list = [sex_list,race_list,hispanic_list,age_list,marital_list,school_list,division_list]
    # all_list_str = ['sex_list','race_list','hispanic_list','age_list','state_list']

    #print(len(list(product(*all_list))))



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
    # print(len(final_flat))

    
    # final_flat_sub = final_flat[300:302]

    #df[df[list(final_flat_sub[99][0].keys())[0]]==list(final_flat_sub[99][0].values())[0]]

    
    #examp = final_flat_sub[0]
    # months = ['jan']
    months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    df_data = pd.DataFrame()

    recodes = ['Employed - at work','Not in labor force - retired','Not in labor force - other','Not in labor force - disabled','Unemployed - looking','Employed - absent','Unemployed - on layoff']

    toc_count=toc1_count=toc2_count=toc3_count=toc4_count=toc5_count=toc6_count=0

    for year in range(1999,2021):
        for m in months:
            if year==2020 and m=='sep':
                break
            # print("...loading pickle")
            # tmp = open(pickle_path+'cps_'+m+str(year)+'.pickle','rb')
            # df = pickle.load(tmp)
            # tmp.close()
            df = pd.read_csv(import_path+'cps_'+m+str(year)+'.csv')
            #Set threshold to around .1% of population
            #MAY NEED TO ADJUST UPWARDS- SEE SOME ZEROS FOR U3 RATES. Maybe .25%?
            threshold = math.floor(len(df)/1000)
            #Weight sum
            weight_sum = df['Weight'].sum()

            '''
            ################################################
            #add to var mapper in cps_parser_db
            school_completed_mapper = {'LESS THAN 1ST GRADE':'No high school degree','1ST, 2ND, 3RD OR 4TH GRADE':'No high school degree','5TH OR 6TH GRADE':'No high school degree','7TH OR 8TH GRADE':'No high school degree','9TH GRADE':'No high school degree','10TH GRADE':'No high school degree','11TH GRADE':'No high school degree','12TH GRADE NO DIPLOMA':'No high school degree','HIGH SCHOOL GRAD-DIPLOMA OR EQUIV':'High school degree','SOME COLLEGE BUT NO DEGREE':'Some college but no degree','ASSOCIATE DEGREE-OCCUPATIONAL/VOCATIONAL':'Associate degree','ASSOCIATE DEGREE-ACADEMIC PROGRAM':'Associate degree','BACHELORS DEGREE':'Bachelors degree','MASTERS DEGREE':'Masters degree','PROFESSIONAL SCHOOL DEG':'Professional school degree','DOCTORATE DEGREE':'Doctorate degree'}

            marital_status_mapper = {'Married- spouse present':'Married','Married- spouse absent':'Separated','Widowed':'Widowed','Divorced':'Divorced','Separated':'Separated','Never married':'Never married'}

            df = df.replace({'School_completed':school_completed_mapper,'Marital_status':marital_status_mapper})
            #################################################
            '''
            count=0
            tic = time.perf_counter()
            for f in final_flat:
                df_sub = df
                #Timer start
                
                for i in f:
                    # Filter for age_group
                    if type(list(i.values())[0]) is tuple:
                        df_sub=df_sub[(df_sub[list(i.keys())[0]]>=list(i.values())[0][0])&(df_sub[list(i.keys())[0]]<=list(i.values())[0][1])]
                    else:
                        df_sub=df_sub[df_sub[list(i.keys())[0]]==list(i.values())[0]]
                    # df_sub=df_sub[df_sub[list(i.keys())[0]]==list(i.values())[0]]
                
                #If fewer than 100 people in this group, do not include in subpop analysis
                len_sub = len(df_sub)
                if len_sub<threshold:
                    continue

                
                
                # percentages = calc_percentages(df_sub)
                # percentage_list = [{'Year':year,'Month':m,'perc_nilf':percentages[0],'perc_retired_nilf':percentages[1],'perc_disabled_nilf':percentages[2],'perc_student_nilf':percentages[3],'perc_unemployed':percentages[4],'perc_looking':percentages[5],'perc_layoff':percentages[6],'perc_employed':percentages[7],'perc_employed_ft':percentages[8],'perc_employed_pt':percentages[9],'perc_employed_ft_student':percentages[10],'perc_employed_pt_student':percentages[11],'looking_lf':percentages[12],'layoff_lf':percentages[13],'unemployed_lf':percentages[14],'employed_ft_lf':percentages[15],'employed_pt_lf':percentages[16],'employed_lf':percentages[17],'employed_pt_econ_lf':percentages[18],'employed_pt_nonecon_lf':percentages[19]}]


                tic1 = time.perf_counter()
                data = pd.DataFrame([{'Year':year,'Month':m,'SubPop':[f[i] for i in range(len(f))],'Pop_percentage':df_sub['Weight'].sum()/weight_sum,'U3_Rate':len(df_sub[df_sub.Unemployed_U3=='Yes'])/len(df_sub[df_sub.Labor_Force_U3=='Yes']),'U3_LFPR':len(df_sub[df_sub.Labor_Force_U3=='Yes'])/len_sub,'U6_Rate':len(df_sub[df_sub.Unemployed_U6=='Yes'])/len(df_sub[df_sub.Labor_Force_U6=='Yes']),'U6_LFPR':len(df_sub[df_sub.Labor_Force_U6=='Yes'])/len_sub}])

                df_data = df_data.append(data).reset_index(drop=True)
                count+=1
                # print(count)


                '''
                #Make sure recode counts contains all items, and adds 0 to ones without any
                
                #df_sub['LF_recode'].value_counts().keys().tolist()
                tic1 = time.perf_counter()
                recode_counts = dict(zip(recodes,[0,0,0,0,0,0,0]))
                for r in recode_counts.keys():
                    if r in df_sub['LF_recode'].value_counts().index:
                        recode_counts[r] = df_sub['LF_recode'].value_counts()[r]
                toc1 = time.perf_counter()
                toc1_count += toc1-tic1

                tic2 = time.perf_counter()
                urate = calc_urate(df_sub,recode_counts,'U3')
                toc2 = time.perf_counter()
                toc2_count += toc2-tic2

                tic3 = time.perf_counter()
                urate_weighted = calc_urate_weighted(df_sub,'U3')
                toc3 = time.perf_counter()
                toc3_count += toc3-tic3

                tic4 = time.perf_counter()
                U6_rate = calc_urate(df_sub,recode_counts,'U6')
                toc4 = time.perf_counter()
                toc4_count += toc4-tic4

                tic5 = time.perf_counter()
                U6_weighted = calc_urate_weighted(df_sub,'U6')
                toc5 = time.perf_counter()
                toc5_count += toc5-tic5

                # self_rate = self_calc(df_sub,'total')
                # self_rate_weight = self_calc_weight(df_sub,'total')

                # data = pd.DataFrame([{'Year':2000,'Month':'jan','SubPop':[f[i] for i in range(len(f))],'Num_employed':recode_counts['Employed - at work']+recode_counts['Employed - absent'],'Num_unemployed': recode_counts['Unemployed - on layoff']+recode_counts['Unemployed - looking'],'Num_LF':(recode_counts['Employed - at work']+recode_counts['Employed - absent']+recode_counts['Unemployed - on layoff']+recode_counts['Unemployed - looking']),'Num_total':len(df_sub),'UR':urate,'UR_weighted':urate_weighted,'U6':U6_rate,'U6_weighted':U6_weighted,'Self_rate':self_rate,'Self_rate_weighted':self_rate_weight}])

                tic6 = time.perf_counter()
                #Should ideally consist of year, month, subpop, pop percentage, LFPR, UR. UR_Weighted, U6, U6_weighted, self_rate, self_rate_weighted
                data = pd.DataFrame([{'Year':year,'Month':m,'SubPop':str([f[i] for i in range(len(f))]),'LFPR':(recode_counts['Employed - at work']+recode_counts['Employed - absent']+recode_counts['Unemployed - on layoff']+recode_counts['Unemployed - looking'])/len(df_sub),'U3':urate,'U3_weighted':urate_weighted,'U6':U6_rate,'U6_weighted':U6_weighted}])
                toc6 = time.perf_counter()
                toc6_count += toc6-tic6
                

                #Remove subpops with low LFPR?
                # if data['LFPR'].values()<0.33:
                #     continue

                df_data = df_data.append(data).reset_index(drop=True)
                '''
    toc = time.perf_counter()
    toc_count += toc-tic    
    print(f"Subpop calc took {toc_count:0.1f} seconds")    
    
    #get subpops which have values for every month in dataset
    df_data['SubPop'] = df_data['SubPop'].astype(str)
    df_data['YearMonth'] = df_data['Year'].astype(str)+df_data['Month']
    subpops = df_data.groupby('SubPop')['YearMonth'].count()[df_data.groupby('SubPop')['YearMonth'].count()==260].index
    df_data = df_data[df_data['SubPop'].isin(subpops)]
    df_data.to_csv(export_path+'subpop_data.csv',index=False)

    # print("...saving pickle")
    # tmp = open('/home/dhense/PublicData/Economic_analysis/intermediate_files/subpop_data.pickle','wb')
    # pickle.dump(df_data,tmp)
    # tmp.close()
    
'''
    print(f"DataFrame filter took {toc_count:0.1f} seconds") 
    print(f"Recode counts took {toc1_count:0.1f} seconds")
    print(f"U3 calc took {toc2_count:0.1f} seconds")
    print(f"U3 weighting calc took {toc3_count:0.1f} seconds")
    print(f"U6 calc took {toc4_count:0.1f} seconds")
    print(f"U6 weighting calc took {toc5_count:0.1f} seconds")
    print(f"Constructing dashboard took {toc6 - tic6:0.1f} seconds")
    print(f"Total time: {toc_count+toc1_count+toc2_count+toc3_count+toc4_count+toc5_count+toc6_count:0.1f} seconds")    
'''