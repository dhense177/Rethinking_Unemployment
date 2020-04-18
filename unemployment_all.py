import os, pickle
import pandas as pd
import numpy as np
import pyspark.sql.functions as f
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext
from statsmodels.tsa.seasonal import seasonal_decompose
pd.set_option('display.float_format', lambda x: '%.4f' % x)


def extract_record(df,yr,m):
    col_dict = {'HHID':(0,15),'Person_type':(160,162),'Interview_Status':(56,58),'Age':(121,123),'Sex':(128,130),'Race':(138,140),'Hispanic':(140,142),'LF_recode':(179,181),'LF_recode2':(392,394),'Civilian_LF':(386,388),'Employed_nonfarm':(479,481),'Have_job':(205,207),'Unpaid_family_work':(183,185),'Job_search':(400,402),'Discouraged':(388,390),'Retired':(566,568),'Disabled':(203,205),'Situation':(568,570),'FT_PT':(396,398),'FT_PT_status':(415,417),'Detailed_reason_part_time':(404,406),'Main_reason_part_time':(228,230),'Main_reason_not_full_time':(230,232),'Want_job':(346,348),'Want_job_ft':(226,228),'Want_job_ft_pt':(199,201),'Want_job_nilf':(417,419),'Reason_unemployment':(411,413),'Reason_not_looking':(348,350),'Hours_per_week':(217,219),'Hours_per_week_last':(242,244),'In_school':(574,576),'In_school_ft_pt':(576,578),'In_school_nilf':(580,582),'State_FIPS':(92,94),'County_FIPS':(100,103),'Metro_Code':(95,100),'Metro_Size':(106,107)}

    df_p = pd.DataFrame()


    for k,v in col_dict.items():
        df_p[k] = [i[0][v[0]:v[1]] for i in df.values]

    df_p['Year'] = yr
    df_p['Month'] = m
    df_p['State_FIPS'] = df_p['State_FIPS'].astype(str).str.strip().apply(lambda x: str(x).zfill(2) if x != '' else '')
    df_p['County_FIPS'] = df_p['County_FIPS'].astype(str).str.strip().apply(lambda x: str(x).zfill(3) if x != '' else '')

    df_p['FIPS'] = df_p['State_FIPS']+df_p['County_FIPS']

    return df_p


def extract_record_b03(df,yr,m):
    col_dict = {'HHID':(0,15),'Person_type':(160,162),'Interview_Status':(56,58),'Age':(121,123),'Sex':(128,130),'Race':(138,140),'Hispanic':(156,158),'LF_recode':(179,181),'LF_recode2':(392,394),'Civilian_LF':(386,388),'Employed_nonfarm':(479,481),'Have_job':(205,207),'Unpaid_family_work':(183,185),'Job_search':(400,402),'Discouraged':(388,390),'Retired':(566,568),'Disabled':(203,205),'Situation':(568,570),'FT_PT':(396,398),'FT_PT_status':(415,417),'Detailed_reason_part_time':(404,406),'Main_reason_part_time':(228,230),'Main_reason_not_full_time':(230,232),'Want_job':(346,348),'Want_job_ft':(226,228),'Want_job_ft_pt':(199,201),'Want_job_nilf':(417,419),'Reason_unemployment':(411,413),'Reason_not_looking':(348,350),'Hours_per_week':(217,219),'Hours_per_week_last':(242,244),'In_school':(574,576),'In_school_ft_pt':(576,578),'In_school_nilf':(580,582),'State_FIPS':(92,94),'County_FIPS':(100,103),'Metro_Code':(95,100),'Metro_Size':(106,107)}

    df_p = pd.DataFrame()


    for k,v in col_dict.items():
        df_p[k] = [i[0][v[0]:v[1]] for i in df.values]

    df_p['Year'] = yr
    df_p['Month'] = m
    df_p['State_FIPS'] = df_p['State_FIPS'].astype(str).str.strip().apply(lambda x: str(x).zfill(2) if x != '' else '')
    df_p['County_FIPS'] = df_p['County_FIPS'].astype(str).str.strip().apply(lambda x: str(x).zfill(3) if x != '' else '')

    df_p['FIPS'] = df_p['State_FIPS']+df_p['County_FIPS']

    return df_p

def extract_record_95(df,yr,m):
    col_dict = {'HHID':(0,12),'Person_type':(160,162),'Interview_Status':(56,58),'Age':(121,123),'Sex':(128,130),'Race':(138,140),'Hispanic':(156,158),'LF_recode':(179,181),'LF_recode2':(392,394),'Civilian_LF':(386,388),'Employed_nonfarm':(479,481),'Have_job':(205,207),'Unpaid_family_work':(183,185),'Job_search':(400,402),'Discouraged':(388,390),'Retired':(566,568),'Disabled':(203,205),'Situation':(568,570),'FT_PT':(396,398),'FT_PT_status':(415,417),'Detailed_reason_part_time':(404,406),'Main_reason_part_time':(228,230),'Main_reason_not_full_time':(230,232),'Want_job':(346,348),'Want_job_ft':(226,228),'Want_job_ft_pt':(199,201),'Want_job_nilf':(417,419),'Reason_unemployment':(411,413),'Reason_not_looking':(348,350),'Hours_per_week':(217,219),'Hours_per_week_last':(242,244),'In_school':(574,576),'In_school_ft_pt':(576,578),'In_school_nilf':(580,582),'State_FIPS':(92,94)}

    df_p = pd.DataFrame()


    for k,v in col_dict.items():
        df_p[k] = [i[0][v[0]:v[1]] for i in df.values]

    df_p['Year'] = yr
    df_p['Month'] = m
    df_p['State_FIPS'] = df_p['State_FIPS'].astype(str).str.strip().apply(lambda x: str(x).zfill(2) if x != '' else '')

    return df_p


def var_mapper(df_cps):
    person_mapper = {' 1':'Child',' 2':'Adult Civilian',' 3':'Adult Armed Forces'}
    sex_mapper = {' 1':'Male',' 2':'Female'}
    race_mapper = {' 1':'White Only', ' 2':'Black Only', ' 3':'American Indian, Alaskan Native Only', ' 4':'Asian Only', ' 5':'Hawaiian/Pacific Islander Only', ' 6':'White-Black', ' 7':'White-AI', ' 8':'White-Asian', ' 9':'White-HP', '10':'Black-AI', '11':'Black-Asian', '12':'Black-HP', '13':'AI-Asian', '14':'AI-HP', '15':'Asian-HP', '16':'W-B-AI', '17':'W-B-A', '18':'W-B-HP', '19':'W-AI-A', '20':'W-AI-HP', '21':'W-A-HP', '22':'B-AI-A', '23':'W-B-AI-A', '24':'W-AI-A-HP', '25':'Other 3 Race Combinations', '26':'Other 4 and 5 Race Combinations'}


    if year < 2003:
        hispanic_mapper = {' 1':'Hispanic', ' 2':'Not Hispanic'}
    else:
        hispanic_mapper = {' 1':'Mexican', ' 2':'Peurto Rican',' 3':'Cuban',' 4':'Central/South American', ' 5':'Other Spanish'}
    lf_recode_mapper = {' 1':'Employed - at work', ' 2':'Employed - absent', ' 3':'Unemployed - on layoff', ' 4':'Unemployed - looking', ' 5':'Not in labor force - retired', ' 6':'Not in labor force - disabled', ' 7':'Not in labor force - other'}
    lf_recode2_mapper = {' 1':'Employed', ' 2':'Unemployed', ' 3':'NILF - discouraged', ' 4':'NILF - other'}
    civilian_lf_mapper = {' 1':'In Civilian LF', ' 2':'Not In Civilian LF'}
    employed_nonfarm_mapper = {' 1':'Employed (Excluding Farm & Private HH)'}
    job_search_mapper = {' 1':'Looked Last 12 Months', ' 2':'Looked And Worked Last 4 Weeks', ' 3':'Looked Last 4 Weeks - Layoff', ' 4':'Unavailable Job Seekers', ' 5':'No Recent Job Search'}
    discouraged_mapper = {' 1':'Discouraged Worker', ' 2':'Conditionally Interested', ' 3':'NA'}
    retired_mapper = {' 1':'Yes', ' 2':'No'}
    disabled_mapper = {' 1':'Yes', ' 2':'No'}
    situation_mapper = {' 1':'Disabled', ' 2':'Ill', ' 3':'In School', ' 4':'Taking Care Of House Or Family', ' 5':'In Retirement', ' 6':'Other'}
    ft_pt_mapper = {' 1':'Full Time LF', ' 2':'Part Time LF'}
    ft_pt_status_mapper = {' 1':'Not In Labor Force', ' 2':'FT Hours (35+), Usually Ft', ' 3':'PT For Economic Reasons, Usually Ft', ' 4':'PT For Non-Economic Reasons, Usually Ft', ' 5':'Not At Work, Usually Ft', ' 6':'PT Hrs, Usually Pt For Economic Reasons', ' 7':'PT Hrs, Usually Pt For Non-Economic Reasons', ' 8':'FT Hours, Usually Pt For Economic Reasons', ' 9':'FT Hours, Usually Pt For Non-Economic', '10':'Not At Work, Usually Part-time', '11':'Unemployed FT', '12':'Unemployed PT'}
    detailed_pt_mapper = {' 1':'Usu. FT-Slack Work/Business Conditions', ' 2':'Usu. FT-Seasonal Work', ' 3':'Usu. FT-Job Started/Ended During Week', ' 4':'Usu. FT-Vacation/Personal Day', ' 5':'Usu. FT-Own Illness/Injury/Medical Appointment', ' 6':'Usu. FT-Holiday (Religious Or Legal)', ' 7':'Usu. FT-Child Care Problems', ' 8':'Usu. FT-Other Fam/Pers Obligations', ' 9':'Usu. FT-Labor Dispute', '10':'Usu. FT-Weather Affected Job', '11':'Usu. FT-School/Training', '12':'Usu. FT-Civic/Military Duty', '13':'Usu. FT-Other Reason', '14':'Usu. PT-Slack Work/Business Conditions', '15':'Usu. PT-Could Only Find Pt Work', '16':'Usu. PT-Seasonal Work', '17':'Usu. PT-Child Care Problems', '18':'Usu. PT-Other Fam/Pers Obligations', '19':'Usu. PT-Health/Medical Limitations', '20':'Usu. PT-School/Training', '21':'Usu. PT-Retired/S.S. Limit On Earnings', '22':'Usu. PT-Workweek <35 Hours', '23':'Usu. PT-Other Reason'}
    main_pt_mapper = {' 1':'Slack Work/Business Conditions', ' 2':'Could Only Find Part-time Work', ' 3':'Seasonal Work', ' 4':'Child Care Problems', ' 5':'Other Family/Personal Obligations', ' 6':'Health/Medical Limitations', ' 7':'School/Training', ' 8':'Retired/Social Security Limit On Earnings', ' 9':'Full-Time Workweek Is Less Than 35 Hrs', '10':'Other - Specify'}
    main_not_ft_mapper = {' 1':'Child Care Problems', ' 2':'Other Family/Personal Obligations', ' 3':'Health/Medical Limitations',' 4':'School/Training', '5':'Retired/SS Earnings Limit', ' 6':'Full-time Work Week Less Than 35 Hours', ' 7':'Other'}
    have_job_mapper = {' 1':'Yes',' 2':'No',' 3':'Retired',' 4':'Disabled',' 5':'Unable To Work'}
    want_job_mapper = {' 1':'Yes, Or Maybe, It Depends', ' 2':'No', ' 3':'Retired', ' 4':'Disabled', ' 5':'Unable'}
    want_job_ft_mapper = {' 1':'Yes', ' 2':'No', ' 3':'Regular Hours Are Full-Time'}
    want_job_ft_pt_mapper = {' 1':'Yes', ' 2':'No', ' 3':'Has A Job'}
    want_job_nilf_mapper = {' 1':'Want A Job', ' 2':'Other Not In Labor Force'}
    reason_unemployment_mapper = {' 1':'Job Loser/On Layoff', ' 2':'Other Job Loser', ' 3':'Temporary Job Ended', ' 4':'Job Leaver', ' 5':'Re-Entrant', ' 6':'New-Entrant'}
    reason_not_looking_mapper = {' 1':'Believes No Work Available In Area Of Expertise', ' 2':'Couldnt Find Any Work', ' 3':'Lacks Necessary Schooling/Training', ' 4':'Employers Think Too Young Or Too Old', ' 5':'Other Types Of Discrimination', ' 6':'Cant Arrange Child Care', ' 7':'Family Responsibilities', ' 8':'In School Or Other Training', ' 9':'Ill-health, Physical Disability', '10':'Transportation Problems', '11':'Other - Specify'}
    in_school_mapper = {' 1':'Yes', ' 2':'No'}
    in_school_ft_pt = {' 1':'Full-time', ' 2':'Part-time'}
    in_school_nilf_mapper = {' 1':'In School', ' 2':'Not In School'}
    metro_size_mapper = {'0':'NOT IDENTIFIED OR NONMETROPOLITAN', '2':'100,000 - 249,999', '3':'250,000 - 499,999', '4':'500,000 - 999,999', '5':'1,000,000 - 2,499,999', '6':'2,500,000 - 4,999,999', '7':'5,000,000+'}

    df_cps = df_cps.replace({'Person_type':person_mapper, 'Sex':sex_mapper, 'Race':race_mapper, 'Hispanic':hispanic_mapper, 'LF_recode':lf_recode_mapper, 'LF_recode2':lf_recode2_mapper, 'Civilian_LF':civilian_lf_mapper, 'Employed_nonfarm':employed_nonfarm_mapper, 'Job_search':job_search_mapper, 'Discouraged':discouraged_mapper, 'Retired':retired_mapper, 'Disabled':disabled_mapper, 'Situation': situation_mapper, 'FT_PT':ft_pt_mapper, 'FT_PT_status':ft_pt_status_mapper, 'Detailed_reason_part_time':detailed_pt_mapper, 'Main_reason_part_time':main_pt_mapper, 'Main_reason_not_full_time':main_not_ft_mapper, 'Have_job':have_job_mapper, 'Want_job':want_job_mapper, 'Want_job_ft':want_job_ft_mapper, 'Want_job_ft_pt':want_job_ft_pt_mapper, 'Want_job_nilf':want_job_nilf_mapper, 'Reason_unemployment':reason_unemployment_mapper, 'Reason_not_looking':reason_not_looking_mapper, 'In_school':in_school_mapper, 'In_school_ft_pt':in_school_ft_pt, 'In_school_nilf':in_school_nilf_mapper, 'Metro_Size':metro_size_mapper})

    return df_cps

def var_mapper_95(df_cps):
    person_mapper = {' 1':'Child',' 2':'Adult Civilian',' 3':'Adult Armed Forces'}
    sex_mapper = {' 1':'Male',' 2':'Female'}
    race_mapper = {' 1':'White Only', ' 2':'Black Only', ' 3':'American Indian, Alaskan Native Only', ' 4':'Asian Only', ' 5':'Hawaiian/Pacific Islander Only', ' 6':'White-Black', ' 7':'White-AI', ' 8':'White-Asian', ' 9':'White-HP', '10':'Black-AI', '11':'Black-Asian', '12':'Black-HP', '13':'AI-Asian', '14':'AI-HP', '15':'Asian-HP', '16':'W-B-AI', '17':'W-B-A', '18':'W-B-HP', '19':'W-AI-A', '20':'W-AI-HP', '21':'W-A-HP', '22':'B-AI-A', '23':'W-B-AI-A', '24':'W-AI-A-HP', '25':'Other 3 Race Combinations', '26':'Other 4 and 5 Race Combinations'}
    hispanic_mapper = {' 1':'Hispanic', ' 2':'Not Hispanic'}
    lf_recode_mapper = {' 1':'Employed - at work', ' 2':'Employed - absent', ' 3':'Unemployed - on layoff', ' 4':'Unemployed - looking', ' 5':'Not in labor force - retired', ' 6':'Not in labor force - disabled', ' 7':'Not in labor force - other'}
    lf_recode2_mapper = {' 1':'Employed', ' 2':'Unemployed', ' 3':'NILF - discouraged', ' 4':'NILF - other'}
    civilian_lf_mapper = {' 1':'In Civilian LF', ' 2':'Not In Civilian LF'}
    employed_nonfarm_mapper = {' 1':'Employed (Excluding Farm & Private HH)'}
    job_search_mapper = {' 1':'Looked Last 12 Months', ' 2':'Looked And Worked Last 4 Weeks', ' 3':'Looked Last 4 Weeks - Layoff', ' 4':'Unavailable Job Seekers', ' 5':'No Recent Job Search'}
    discouraged_mapper = {' 1':'Discouraged Worker', ' 2':'Conditionally Interested', ' 3':'NA'}
    retired_mapper = {' 1':'Yes', ' 2':'No'}
    disabled_mapper = {' 1':'Yes', ' 2':'No'}
    situation_mapper = {' 1':'Disabled', ' 2':'Ill', ' 3':'In School', ' 4':'Taking Care Of House Or Family', ' 5':'In Retirement', ' 6':'Other'}
    ft_pt_mapper = {' 1':'Full Time LF', ' 2':'Part Time LF'}
    ft_pt_status_mapper = {' 1':'Not In Labor Force', ' 2':'FT Hours (35+), Usually Ft', ' 3':'PT For Economic Reasons, Usually Ft', ' 4':'PT For Non-Economic Reasons, Usually Ft', ' 5':'Not At Work, Usually Ft', ' 6':'PT Hrs, Usually Pt For Economic Reasons', ' 7':'PT Hrs, Usually Pt For Non-Economic Reasons', ' 8':'FT Hours, Usually Pt For Economic Reasons', ' 9':'FT Hours, Usually Pt For Non-Economic', '10':'Not At Work, Usually Part-time', '11':'Unemployed FT', '12':'Unemployed PT'}
    detailed_pt_mapper = {' 1':'Usu. FT-Slack Work/Business Conditions', ' 2':'Usu. FT-Seasonal Work', ' 3':'Usu. FT-Job Started/Ended During Week', ' 4':'Usu. FT-Vacation/Personal Day', ' 5':'Usu. FT-Own Illness/Injury/Medical Appointment', ' 6':'Usu. FT-Holiday (Religious Or Legal)', ' 7':'Usu. FT-Child Care Problems', ' 8':'Usu. FT-Other Fam/Pers Obligations', ' 9':'Usu. FT-Labor Dispute', '10':'Usu. FT-Weather Affected Job', '11':'Usu. FT-School/Training', '12':'Usu. FT-Civic/Military Duty', '13':'Usu. FT-Other Reason', '14':'Usu. PT-Slack Work/Business Conditions', '15':'Usu. PT-Could Only Find Pt Work', '16':'Usu. PT-Seasonal Work', '17':'Usu. PT-Child Care Problems', '18':'Usu. PT-Other Fam/Pers Obligations', '19':'Usu. PT-Health/Medical Limitations', '20':'Usu. PT-School/Training', '21':'Usu. PT-Retired/S.S. Limit On Earnings', '22':'Usu. PT-Workweek <35 Hours', '23':'Usu. PT-Other Reason'}
    main_pt_mapper = {' 1':'Slack Work/Business Conditions', ' 2':'Could Only Find Part-time Work', ' 3':'Seasonal Work', ' 4':'Child Care Problems', ' 5':'Other Family/Personal Obligations', ' 6':'Health/Medical Limitations', ' 7':'School/Training', ' 8':'Retired/Social Security Limit On Earnings', ' 9':'Full-Time Workweek Is Less Than 35 Hrs', '10':'Other - Specify'}
    main_not_ft_mapper = {' 1':'Child Care Problems', ' 2':'Other Family/Personal Obligations', ' 3':'Health/Medical Limitations',' 4':'School/Training', '5':'Retired/SS Earnings Limit', ' 6':'Full-time Work Week Less Than 35 Hours', ' 7':'Other'}
    have_job_mapper = {' 1':'Yes',' 2':'No',' 3':'Retired',' 4':'Disabled',' 5':'Unable To Work'}
    want_job_mapper = {' 1':'Yes, Or Maybe, It Depends', ' 2':'No', ' 3':'Retired', ' 4':'Disabled', ' 5':'Unable'}
    want_job_ft_mapper = {' 1':'Yes', ' 2':'No', ' 3':'Regular Hours Are Full-Time'}
    want_job_ft_pt_mapper = {' 1':'Yes', ' 2':'No', ' 3':'Has A Job'}
    want_job_nilf_mapper = {' 1':'Want A Job', ' 2':'Other Not In Labor Force'}
    reason_unemployment_mapper = {' 1':'Job Loser/On Layoff', ' 2':'Other Job Loser', ' 3':'Temporary Job Ended', ' 4':'Job Leaver', ' 5':'Re-Entrant', ' 6':'New-Entrant'}
    reason_not_looking_mapper = {' 1':'Believes No Work Available In Area Of Expertise', ' 2':'Couldnt Find Any Work', ' 3':'Lacks Necessary Schooling/Training', ' 4':'Employers Think Too Young Or Too Old', ' 5':'Other Types Of Discrimination', ' 6':'Cant Arrange Child Care', ' 7':'Family Responsibilities', ' 8':'In School Or Other Training', ' 9':'Ill-health, Physical Disability', '10':'Transportation Problems', '11':'Other - Specify'}
    in_school_mapper = {' 1':'Yes', ' 2':'No'}
    in_school_ft_pt = {' 1':'Full-time', ' 2':'Part-time'}
    in_school_nilf_mapper = {' 1':'In School', ' 2':'Not In School'}

    df_cps = df_cps.replace({'Person_type':person_mapper, 'Sex':sex_mapper, 'Race':race_mapper, 'Hispanic':hispanic_mapper, 'LF_recode':lf_recode_mapper, 'LF_recode2':lf_recode2_mapper, 'Civilian_LF':civilian_lf_mapper, 'Employed_nonfarm':employed_nonfarm_mapper, 'Job_search':job_search_mapper, 'Discouraged':discouraged_mapper, 'Retired':retired_mapper, 'Disabled':disabled_mapper, 'Situation': situation_mapper, 'FT_PT':ft_pt_mapper, 'FT_PT_status':ft_pt_status_mapper, 'Detailed_reason_part_time':detailed_pt_mapper, 'Main_reason_part_time':main_pt_mapper, 'Main_reason_not_full_time':main_not_ft_mapper, 'Have_job':have_job_mapper, 'Want_job':want_job_mapper, 'Want_job_ft':want_job_ft_mapper, 'Want_job_ft_pt':want_job_ft_pt_mapper, 'Want_job_nilf':want_job_nilf_mapper, 'Reason_unemployment':reason_unemployment_mapper, 'Reason_not_looking':reason_not_looking_mapper, 'In_school':in_school_mapper, 'In_school_ft_pt':in_school_ft_pt, 'In_school_nilf':in_school_nilf_mapper})

    return df_cps


def turn_int(df_cps):
    df_cps = df_cps.astype({'Month':int, 'Year':int, 'Age':int, 'Hours_per_week':int, 'Hours_per_week_last':int})
    return df_cps

def refine_vars(df_cps):
    #Calculate full time vs. part time
    df_cps.loc[(df_cps['FT_PT']=='Full Time LF'),'FT_PT']='Full_time'
    df_cps.loc[(df_cps['Hours_per_week_last']<35)&((df_cps['FT_PT']=='Part Time LF')|(df_cps['Detailed_reason_part_time'].isin(['Usu. PT-Slack Work/Business Conditions','Usu. PT-Could Only Find Pt Work','Usu. PT-Seasonal Work','Usu. PT-Child Care Problems','Usu. PT-Other Fam/Pers Obligations','Usu. PT-Health/Medical Limitations','Usu. PT-Workweek <35 Hours']))|(df_cps['Main_reason_part_time'].isin(['Slack Work/Business Conditions','Could Only Find Part-time Work','Seasonal Work','Child Care Problems','Other Family/Personal Obligations','Health/Medical Limitations']))),'FT_PT']='Part_time'
    df_cps.loc[(df_cps['FT_PT']!='Full_time')&(df_cps['FT_PT']!='Part_time'),'FT_PT']='NA'

    #Calculate those in school
    df_cps.loc[(df_cps['Situation']=='In School')|(df_cps['In_school']=='Yes'),'In_school']='Yes'
    df_cps.loc[df_cps['In_school']!='Yes','In_school']='No'

    #Shows that all in school in situation column only are NILF
    # df_cps[(df_cps['Situation']==3)&(df_cps['In_school']!=1)]['LF_recode'].value_counts()

    #Calculate those in school full time vs part time
    # df_cps.loc[(df_cps['In_school_ft_pt']==1)|(df_cps['In_school']==1),'In_school']='Yes'
    # df_cps.loc[df_cps['In_school']!='Yes','In_school']='No'

    #Calculate those disabled
    df_cps.loc[(df_cps['Situation']=='Disabled')|(df_cps['Disabled']=='Yes')|(df_cps['LF_recode']=='Not in labor force - disabled'),'Disabled']='Yes'
    df_cps.loc[df_cps['Disabled']!='Yes','Disabled']='No'

    #Calculate those discouraged
    df_cps.loc[(df_cps['Discouraged']=='Discouraged Worker')|(df_cps['Discouraged']=='Conditionally Interested')|(df_cps['LF_recode2']=='NILF - discouraged'),'Discouraged']='Yes'
    df_cps.loc[df_cps['Discouraged']!='Yes','Discouraged']='No'

    #Calculate those retired
    df_cps.loc[(df_cps['LF_recode']=='Not in labor force - retired')|(df_cps['Retired']=='Yes')|(df_cps['Situation']=='In Retirement'),'Retired']='Yes'
    df_cps.loc[df_cps['Retired']!='Yes','Retired']='No'

    #Calculate those retired and want work
    df_cps['Retired_want_work'] = np.where((df_cps['Retired']=='Yes')&((df_cps['Want_job']=='Yes, Or Maybe, It Depends')|(df_cps['Want_job_nilf']=='Want A Job')|(df_cps['Want_job_ft_pt']=='Yes')),'Yes',np.where((df_cps['Retired']=='Yes')&((df_cps['Want_job']!='Yes, Or Maybe, It Depends')|(df_cps['Want_job_nilf']!='Want A Job')),'No','NA'))

    df_cps['Want_work'] = np.where((df_cps['Want_job']=='Yes, Or Maybe, It Depends')|(df_cps['Want_job_nilf']=='Want A Job')|(df_cps['Want_job_ft_pt']=='Yes'),'Yes','No')

    return df_cps


def avg_part_time(df_cps):
    hours = df_cps[(df_cps['FT_PT']=='Part_time')&(df_cps['Hours_per_week_last']!='-1')]['Hours_per_week_last'].value_counts().keys().tolist()

    counts = df_cps[(df_cps['FT_PT']=='Part_time')&(df_cps['Hours_per_week_last']!='-1')]['Hours_per_week_last'].value_counts().tolist()

    total_count = np.sum(counts)

    hour_dict = dict(zip(hours,counts/total_count))

    avg_hours = np.sum([k*v for k,v in hour_dict.items()])

    return avg_hours


def fips_mapper(df):
    df_fips = pd.read_excel('/home/dhense/PublicData/FIPS.xlsx')
    df_fips.FIPS = df_fips.FIPS.apply(lambda x: str(x).zfill(5))
    df_fips['State_FIPS'] = df_fips.FIPS.str[:2]
    df_fips = df_fips.groupby(['State','State_FIPS']).size().reset_index()[['State','State_FIPS']]
    df = df.merge(df_fips,on='State_FIPS',how='left')
    return df

def cut(arr):
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



if __name__=='__main__':
    fp = '/home/dhense/PublicData/cps_files/'
    filepath = '/home/dhense/PublicData/Economic_analysis/'
    pickle_path = '/home/dhense/PublicData/Economic_analysis/intermediate_files/'

    cpsp_pickle = 'cpsp.pickle'
    pop_pickle = 'pop.pickle'
    pop_pickle2 = 'pop2.pickle'
    nat_pop_month_pickle = 'nat_pop_month.pickle'
    nat_pop_age_pickle = 'nat_pop_age.pickle'
    state_pop_pickle = 'state_pop.pickle'
    county90_pop_pickle = 'county90.pickle'
    urate_pickle = 'urate.pickle'
    ces_pickle = 'ces.pickle'

    print("...loading pickle")
    tmp = open(pickle_path+nat_pop_month_pickle,'rb')
    df_month = pickle.load(tmp)
    tmp.close()

    print("...loading pickle")
    tmp = open(pickle_path+nat_pop_age_pickle,'rb')
    df_age = pickle.load(tmp)
    tmp.close()

    print("...loading pickle")
    tmp = open(pickle_path+state_pop_pickle,'rb')
    df_state = pickle.load(tmp)
    tmp.close()

    #County-level population data from SEER
    print("...loading pickle")
    tmp = open(pickle_path+pop_pickle2,'rb')
    df_pop2 = pickle.load(tmp)
    tmp.close()

    print("...loading pickle")
    tmp = open(pickle_path+ces_pickle,'rb')
    df_ces = pickle.load(tmp)
    tmp.close()

    print("...loading pickle")
    tmp = open(pickle_path+county90_pop_pickle,'rb')
    df90c = pickle.load(tmp)
    tmp.close()

#################################################################################

    df_state['State_FIPS'] = df_state['STATE'].apply(lambda x: str(x).zfill(2))

    df_state = fips_mapper(df_state)

    df_state['Age_group'] = cut(df_state['Age'].to_numpy())
    df_state.drop(['State','Age'],axis=1,inplace=True)
    df_state = pd.DataFrame(df_state.groupby(by=['Age_group','Sex','Hispanic','Race','State_FIPS'])[['POPESTIMATE2000','POPESTIMATE2001','POPESTIMATE2002','POPESTIMATE2003','POPESTIMATE2004','POPESTIMATE2005','POPESTIMATE2006','POPESTIMATE2007','POPESTIMATE2008','POPESTIMATE2009','POPESTIMATE2010','POPESTIMATE2011','POPESTIMATE2012','POPESTIMATE2013','POPESTIMATE2014','POPESTIMATE2015','POPESTIMATE2016','POPESTIMATE2017','POPESTIMATE2018']].sum()).reset_index()

#################################################################################
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

    df_state = df_state.merge(df90s,on=['State_FIPS','Age_group','Sex','Hispanic','Race'],how='left')[['Age_group','Sex','Hispanic','Race','State_FIPS','POPESTIMATE1990','POPESTIMATE1991','POPESTIMATE1992','POPESTIMATE1993','POPESTIMATE1994','POPESTIMATE1995','POPESTIMATE1996','POPESTIMATE1997','POPESTIMATE1998','POPESTIMATE1999','POPESTIMATE2000','POPESTIMATE2001','POPESTIMATE2002','POPESTIMATE2003','POPESTIMATE2004','POPESTIMATE2005','POPESTIMATE2006','POPESTIMATE2007','POPESTIMATE2008','POPESTIMATE2009','POPESTIMATE2010','POPESTIMATE2011','POPESTIMATE2012','POPESTIMATE2013','POPESTIMATE2014','POPESTIMATE2015','POPESTIMATE2016','POPESTIMATE2017','POPESTIMATE2018']]


#################################################################################
    #Monthly CPS data
    df_urates = pd.DataFrame()
    urate_list = []

    df_weights = pd.DataFrame()
    months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    months_dict = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}
    # months = ['jan']

    if not os.path.isfile(pickle_path+urate_pickle):

        for year in range(1995,2020):
            for m in months:
                df_cps = pd.read_csv(fp+str(year)+'/'+m+str(year)[-2:]+'pub.dat')
                if year < 2003:
                    if year==1995:
                        df_cps = extract_record_95(df_cps,str(year),str(months_dict[m]))
                        df_cps = var_mapper_95(df_cps)
                    else:
                        df_cps = extract_record_b03(df_cps,str(year),str(months_dict[m]))
                        df_cps = var_mapper(df_cps)
                else:
                    df_cps = extract_record(df_cps,str(year),str(months_dict[m]))
                    df_cps = var_mapper(df_cps)
                df_cps = df_cps[(df_cps['Person_type']=='Adult Civilian')]
                df_cps = turn_int(df_cps)
                df_cps = df_cps[df_cps.Age>15]

                recode_counts = dict(zip(df_cps['LF_recode'].value_counts().keys().tolist(),df_cps['LF_recode'].value_counts().tolist()))

                urate = (recode_counts['Unemployed - on layoff']+recode_counts['Unemployed - looking'])/(recode_counts['Employed - at work']+recode_counts['Employed - absent']+recode_counts['Unemployed - on layoff']+recode_counts['Unemployed - looking'])

                urate_list.append(urate)
                # print(urate)

                #################

                df_cps.loc[(df_cps['Hispanic']=='-1'),'Hispanic']='Not Hispanic'
                df_cps.loc[(df_cps['Hispanic']!='Not Hispanic'),'Hispanic']='Hispanic'

                df_cps.loc[(df_cps['Race'].isin(['White Only','Black Only'])==False),'Race']='Other'

                df_cps['Age_group'] = cut(df_cps['Age'].to_numpy())

                df_cps = df_cps.merge(df_state,on=['Sex','Race','Age_group','Hispanic','State_FIPS'],how='left',left_index=True)

                for y in range(2000,2019):
                    df_cps['POPESTIMATE'+str(y)]=df_cps['POPESTIMATE'+str(y)]/df_state['POPESTIMATE'+str(y)].sum()

                df_cps['Count'] = np.arange(1,len(df_cps)+1)

                weight_mapper = df_cps.groupby(by=['Age_group','Sex','Race','Hispanic','State_FIPS'])['Count'].count().reset_index().rename(columns={'Count':'Ratio'})

                weight_mapper['Ratio'] = weight_mapper['Ratio']/len(df_cps)

                df_cps = df_cps.merge(weight_mapper,on=['Sex','Race','Age_group','Hispanic','State_FIPS'],how='left',left_index=True)

                #No detailed population data for 2019 - use 2018
                if year == 2019:
                    df_cps['Weight'] = df_cps['POPESTIMATE2018']/df_cps['Ratio']
                else:
                    df_cps['Weight'] = df_cps['POPESTIMATE'+str(year)]/df_cps['Ratio']



                ############################################################

                # ratio = (df_ces[(df_ces.year==year)&(df_ces.period=='M'+str(months_dict[m]).zfill(2))&(df_ces['series_id']=='CEU0000000001')]['value'].values[0]/(df_curr_month['Total_population'].values[0]/1000))/((recode_counts['Employed - at work']+recode_counts['Employed - absent'])/len(df_cps))

                recode_weighted = df_cps.groupby('LF_recode')['Weight'].sum()

                urate_weighted = (recode_weighted['Unemployed - on layoff']+recode_weighted['Unemployed - looking'])/((recode_weighted['Employed - at work']+recode_weighted['Employed - absent'])+recode_weighted['Unemployed - on layoff']+recode_weighted['Unemployed - looking'])

                # urate_weighted = ((recode_weighted['Unemployed - on layoff']+recode_weighted['Unemployed - looking'])*(1/ratio))/((recode_weighted['Employed - at work']+recode_weighted['Employed - absent'])*ratio+(recode_weighted['Unemployed - on layoff']+recode_weighted['Unemployed - looking'])*1/ratio)

                # urate_list.append(urate_weighted)
                # print(urate)

                data = [{'Year':year,'Month':months_dict[m],'Num_employed':recode_counts['Employed - at work']+recode_counts['Employed - absent'],'Num_unemployed': recode_counts['Unemployed - on layoff']+recode_counts['Unemployed - looking'],'Num_LF':(recode_counts['Employed - at work']+recode_counts['Employed - absent']+recode_counts['Unemployed - on layoff']+recode_counts['Unemployed - looking']),'Num_total':len(df_cps),'UR':urate,'UR_weighted':urate_weighted}]

                df_ur = pd.DataFrame(data)
                print(urate_weighted)

                df_urates = df_urates.append(df_ur)

                #############
                df_cps = refine_vars(df_cps)
                df_cps['Employed'] = np.where((df_cps['In_school']!='Yes')&(df_cps['LF_recode'].isin(['Employed - at work','Employed - absent'])),'Yes','No')

                num_employed = len(df_cps[df_cps['Employed']=='Yes'])

                df_cps['Unemployed'] = np.where(((df_cps['Retired']!='Yes')&(df_cps['In_school']!='Yes'))&((df_cps['LF_recode'].isin(['Unemployed - on layoff','Unemployed - looking']))|(df_cps['Want_work']=='Yes')|(df_cps['Discouraged']=='Yes')|(df_cps['Disabled']=='Yes')),'Yes','No')

                num_unemployed = len(df_cps[df_cps['Unemployed']=='Yes'])

                num_part_time = len(df_cps[df_cps['FT_PT']=='Part_time'])

                avg_pt_hours = avg_part_time(df_cps)

                #Calculate number of part-timers to subtract from employed and add to unemployed based on average number of hours they work less than full time (35).
                remove_employed = num_part_time-int(round(num_part_time*(avg_pt_hours/35)))
                num_employed = num_employed-remove_employed
                num_unemployed = num_unemployed+remove_employed

                adj_urate = num_unemployed/(num_employed+num_unemployed)
                # print(adj_urate)

        print("...saving pickle")
        tmp = open(pickle_path+urate_pickle,'wb')
        pickle.dump(df_urates,tmp)
        tmp.close()
    else:
        print("...loading pickle")
        tmp = open(pickle_path+urate_pickle,'rb')
        df_urates = pickle.load(tmp)
        tmp.close()

    print(df_urates)



########################################################################

    #Compare with official numbers
    df_fred = pd.read_excel('/home/dhense/PublicData/Economic_analysis/Data/Unemployment_Analysis/UNRATENSA.xls',skiprows=10)

    df_fred['Year'] = df_fred['observation_date'].astype(str).str[:4].astype(int)
    df_fred['Month'] = df_fred['observation_date'].astype(str).str[5:7].astype(int)

    df_fred = df_fred[(df_fred.Year>1994)&(df_fred.Year<2020)]

    df_fred.drop('observation_date',axis=1,inplace=True)

    df_urates = df_urates.merge(df_fred,on=['Year','Month'])
    df_urates['UR_weighted'] = round(df_urates['UR_weighted']*100,1)

    #1.2% !!!!!!!!!!
    percent_off = np.mean(np.abs(df_urates['UR_weighted']-df_urates['UNRATENSA'])/((df_urates['UR_weighted']+df_urates['UNRATENSA'])/2))


########################################################################
    #Seasonal decomp
    # df_urates['Date'] = pd.to_datetime(df_urates['Year'].astype(str)+'-'+df_urates['Month'].astype(str))
    #
    # df_urates = df_urates.set_index('Date')
    #
    # s = seasonal_decompose(df_urates['UR_weighted']*100)
    #
    # print(s.trend.values)


    ### Employment, LF and Unemployment Data - BLS Local Area Unemployment Stats
##############################################################################

    print("...loading pickle")
    tmp = open('/home/dhense/PublicData/intermediate_files/la.pickle','rb')
    df_la = pickle.load(tmp)
    tmp.close()

    #Get rid of Puerto rico Counties
    df_la = df_la[df_la['State_and_county'].str[-2:]!='PR']

    df_lf = pd.DataFrame(df_la[(df_la.period=='M13')&(df_la.Year<2020)].groupby('Year')[['Employment','Unemployment','Labor Force']].sum()).reset_index()

    #Import population data, aggregate
    print("...loading pickle")
    tmp = open(pickle_path+pop_pickle,'rb')
    df_pop = pickle.load(tmp)
    tmp.close()

    df_pop['Age'] = df_pop['Age'].astype(int)
    df_pop['Year'] = df_pop['Year'].astype(int)
    df_pop = df_pop[df_pop.Age>15]
    grouped = pd.DataFrame(df_pop.groupby('Year')['Population'].sum()).reset_index()

    df_lf = df_lf.merge(grouped, on='Year')

    df_lf['LFPR'] = df_lf['Labor Force']/df_lf['Population']
    df_lf['UR_orig'] = df_lf['Unemployment']/df_lf['Labor Force']
    df_lf['UR_total'] = 1-(df_lf['Employment']/df_lf['Population'])

    #Unemployment rate by year and period
    df_lf_period = pd.DataFrame(df_la.groupby(['Year','period'])['Unemployment'].sum()/df_la.groupby(['Year','period'])['Labor Force'].sum()).reset_index().rename(columns={0:'Unemployment_rate'})

    # df_lf_period['Unemployment_rate_cps'] = urate_list

    #Get rid of M13
    df_lf_period = df_lf_period[df_lf_period.period!='M13']
    df_lf_period = df_lf_period[df_lf_period.Year>1994]

    #Seasonal adjustment
    df_lf_seasonal = pd.DataFrame(df_la.groupby(['Year','period'])['Unemployment'].sum()/df_la.groupby(['Year','period'])['Labor Force'].sum()).reset_index().rename(columns={0:'Unemployment_rate'})

    monthly_means = df_lf_seasonal.groupby('period')['Unemployment_rate'].mean()
    overall_mean = df_lf_seasonal['Unemployment_rate'].mean()

    monthly_weights = pd.DataFrame(overall_mean/monthly_means).reset_index().rename(columns={'Unemployment_rate':'Seasonal_weight'})

    df_lf_period = pd.merge(df_lf_period,monthly_weights,on='period',how='left')
    df_lf_period['Unemployment_rate_seas'] = df_lf_period['Unemployment_rate']*df_lf_period['Seasonal_weight']

    df_lf_period['Unemployment_rate_cps'] = df_urates['UR_weighted'].values

    # monthly_means_cps = df_lf_period.groupby('period')['Unemployment_rate_cps'].mean()
    # overall_mean_cps = df_lf_period['Unemployment_rate_cps'].mean()
    #
    # monthly_weights_cps = pd.DataFrame(overall_mean_cps/monthly_means_cps).reset_index().rename(columns={'Unemployment_rate_cps':'Seasonal_weight_cps'})
    #
    # df_lf_period = pd.merge(df_lf_period,monthly_weights_cps,on='period',how='left')

    df_lf_period['Unemployment_rate_cps_seas'] = df_lf_period['Unemployment_rate_cps']*df_lf_period['Seasonal_weight']


    df_pop2['FIPS'] = df_pop2['State_FIPS']+df_pop2['County_FIPS']
    df_pop2['Year'] = df_pop2['Year'].astype(int)

    df_fips = pd.read_excel('/home/dhense/PublicData/FIPS.xlsx')
    df_fips['FIPS'] = df_fips['FIPS'].astype(str).str.pad(5,fillchar='0')
    df_fips['State_and_county'] = df_fips['Name'] + ' County, '+df_fips['State']

    df_pop2 = pd.merge(df_pop2, df_fips[['FIPS','State_and_county']],on='FIPS',how='left')

    df_lf_county = pd.DataFrame(df_la[df_la.period!='M13'].groupby(['Year','State_and_county'])['Employment','Unemployment','Labor Force'].sum()).reset_index()

    df_lf_county = pd.merge(df_lf_county,df_pop2[['Year','State_and_county','Population']],on=['Year','State_and_county'],how='left')
