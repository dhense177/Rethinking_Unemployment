import os, pickle, time
import pandas as pd
import numpy as np
from itertools import chain
from pyspark.sql.functions import create_map, lit, udf, when, col, monotonically_increasing_id
import pyspark.sql.functions as func
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *
from pyspark import SparkContext
from common_functions import fips_mapper, cut

pd.set_option('display.float_format', lambda x: '%.4f' % x)

sc = SparkContext("local", "CPS Parser")
sqlContext = SQLContext(sc)


'''
schema = StructType([
    StructField('HHID', StringType(), True),StructField('Ref_person', StringType(), True),StructField('Person_line_num', StringType(), True),StructField('Person_type', StringType(), True),StructField('Interview_Status', StringType(), True),StructField('Age', StringType(), True),StructField('Sex', StringType(), True),StructField('Race', StringType(), True),StructField('Hispanic', StringType(), True),StructField('Marital_status', StringType(), True),StructField('Country_of_birth', StringType(), True),StructField('School_completed', StringType(), True),StructField('Ever_active_duty', StringType(), True),StructField('LF_recode', StringType(), True),StructField('LF_recode2', StringType(), True),StructField('Civilian_LF', StringType(), True),StructField('Employed_nonfarm', StringType(), True),StructField('Have_job', StringType(), True),StructField('Unpaid_family_work', StringType(), True),StructField('Recall_return', StringType(), True),StructField('Recall_look', StringType(), True),
    StructField('Job_offered', StringType(), True),StructField('Job_offered_week', StringType(), True),StructField('Available_ft', StringType(), True),StructField('Job_search', StringType(), True),StructField('Look_last_month', StringType(), True),StructField('Look_last_year', StringType(), True),StructField('Last_work', StringType(), True),StructField('Discouraged', StringType(), True),StructField('Retired', StringType(), True),StructField('Disabled', StringType(), True),StructField('Situation', StringType(), True),StructField('FT_PT', StringType(), True),StructField('FT_PT_status', StringType(), True),StructField('Detailed_reason_part_time', StringType(), True),StructField('Main_reason_part_time', StringType(), True),StructField('Main_reason_not_full_time', StringType(), True),StructField('Want_job', StringType(), True),StructField('Want_job_ft', StringType(), True),StructField('Want_job_ft_pt', StringType(), True),StructField('Want_job_nilf', StringType(), True),StructField('Reason_unemployment', StringType(), True),StructField('Reason_not_looking', StringType(), True),StructField('Hours_per_week', StringType(), True),StructField('Hours_per_week_last', StringType(), True),StructField('In_school', StringType(), True),StructField('In_school_ft_pt', StringType(), True),StructField('School_type', StringType(), True),StructField('In_school_nilf', StringType(), True),StructField('State_FIPS', StringType(), True),StructField('County_FIPS', StringType(), True),StructField('Metro_Code', StringType(), True),StructField('Metro_Size', StringType(), True),StructField('Metro_Status', StringType(), True),StructField('Region', StringType(), True),StructField('Division', StringType(), True),StructField('Year', StringType(), True),StructField('Month', StringType(), True),StructField('FIPS', StringType(), True),StructField('PID', StringType(), True),StructField('SID', StringType(), True),StructField('Age_group', StringType(), True),StructField('Retired_want_work', StringType(), True),StructField('Want_work', StringType(), True),StructField('Pop_estimate', StringType(), True),StructField('Weight', StringType(), True)
])
'''


def var_mapper(df_master):
    '''
        Maps numerical codes for each variable into understandable values
    '''
    person_mapper = {1.0:'Child',2.0:'Adult Civilian',3.0:'Adult Armed Forces'}
    person_expr = create_map([lit(x) for x in chain(*person_mapper.items())])

    sex_mapper = {1.0:'Male',2.0:'Female'}
    sex_expr = create_map([lit(x) for x in chain(*sex_mapper.items())])

    race_mapper = {1.0:'White Only', 2.0:'Black Only', 3.0:'American Indian, Alaskan Native Only', 4.0:'Asian Only', 5.0:'Hawaiian/Pacific Islander Only', 6.0:'White-Black', 7.0:'White-AI', 8.0:'White-Asian', 9.0:'White-HP', 10.0:'Black-AI', 11.0:'Black-Asian', 12.0:'Black-HP', 13.0:'AI-Asian', 14.0:'AI-HP', 15.0:'Asian-HP', 16.0:'W-B-AI', 17.0:'W-B-A', 18.0:'W-B-HP', 19.0:'W-AI-A', 20.0:'W-AI-HP', 21.0:'W-A-HP', 22.0:'B-AI-A', 23.0:'W-B-AI-A', 24.0:'W-AI-A-HP', 25.0:'Other 3 Race Combinations', 26.0:'Other 4 and 5 Race Combinations'}
    race_expr = create_map([lit(x) for x in chain(*race_mapper.items())])

    d1 = df_master.filter(df_master.Year<2003)
    d2 = df_master.filter(df_master.Year>2002)
    
    hispanic_mapper1 = {1.0:'Hispanic', 2.0:'Not Hispanic'}
    hispanic1_expr = create_map([lit(x) for x in chain(*hispanic_mapper1.items())])
    hispanic_mapper2 = {1.0:'Mexican', 2.0:'Peurto Rican',3.0:'Cuban',4.0:'Central/South American', 5.0:'Other Spanish'}
    hispanic2_expr = create_map([lit(x) for x in chain(*hispanic_mapper2.items())])

    d1 = d1.withColumn('Hispanic', hispanic1_expr[d1['Hispanic']])
    d2 = d2.withColumn('Hispanic', hispanic2_expr[d2['Hispanic']])

    df_master = d1.union(d2)

    
    marital_status_mapper = {1.0:'Married',2.0:'Not married',3.0:'Not married',4.0:'Not married',5.0:'Not married',6.0:'Not married'}
    marital_status_expr = create_map([lit(x) for x in chain(*marital_status_mapper.items())])



    country_birth_mapper = {57.0:'United States',72.0:'Puerto Rico',96.0:'U.S. OUTLYING AREA',231.0:'Philippines',60.0:'American Samoa', 233.0:'Saudi Arabia',66.0:'Guam',234.0:'Singapore',72.0:'Puerto Rico',237.0:'Syria',78.0:'U.S. Virgin Islands', 238.0:'Taiwan',102.0:'Austria',239.0:'Thailand',103.0:'Belgium',240.0:'Turkey',105.0:'Czechoslovakia',242.0:'Vietnam',106.0:'Denmark',245.0:'Asia',108.0:'Finland',252.0:'Middle East',109.0:'France',253.0:'Palestine',110.0:'Germany',300.0:'Bermuda',116.0:'Greece',301.0:'Canada',117.0:'Hungary',304.0:'North America',119.0:'Ireland/Eire',310.0:'Belize',120.0:'Italy',311.0:'Costa Rica',126.0:'Holland',312.0:'El Salvador',126.0:'Netherlands',313.0:'Guatemala',127.0:'Norway',314.0:'Honduras',128.0:'Poland',315.0:'Mexico',129.0:'Portugal',316.0:'Nicaragua',130.0:'Azores',317.0:'Panama',132.0:'Romania',318.0:'Central America',134.0:'Spain',333.0:'Bahamas',16.0:'Sweden',334.0:'Barbados',137.0:'Switzerland',337.0:'Cuba',138.0:'Great Britain',338.0:'Dominica',139.0:'England',339.0:'Dominican Republic',140.0:'Scotland',340.0:'Grenada',142.0:'Northern Ireland',342.0:'Haiti',147.0:'Yugoslavia',343.0:'Jamaica',148.0:'Europe',351.0:'Trinidad & Tobago',155.0:'Czech Republic',353.0:'Caribbean',156.0:'Slovakia/Slovak Republic',375.0:'Argentina',180.0:'USSR',376.0:'Bolivia',183.0:'Latvia',377.0:'Brazil',184.0:'Lithuania',378.0:'Chile',185.0:'Armenia',379.0:'Colombia',192.0:'Russia',380.0:'Ecuador',195.0:'Ukraine',383.0:'Guyana',200.0:'Afghanistan',385.0:'Peru',202.0:'Bangladesh',387.0:'Uruguay',205.0:'Burma',388.0:'Venezuela',206.0:'Cambodia',389.0:'South America',207.0:'China',415.0:'Egypt',209.0:'Hong Kong',417.0:'Ethiopia',210.0:'India',421.0:'Ghana',211.0:'Indonesia',427.0:'Kenya',212.0:'Iran',436.0:'Morocco',213.0:'Iraq',440.0:'Nigeria',214.0:'Israel',449.0:'South Africa',215.0:'Japan',462.0:'Other Africa',216.0:'Jordan',468.0:'North Africa',217.0:'Korea/South Korea', 501.0:'Australia',221.0:'Laos',507.0:'Figi',222.0:'Lebanon',514.0:'New Zealand',224.0:'Malaysia',527.0:'Pacific Islands',229.0:'Pakistan',555.0:'Abroad, country not known'}
    country_birth_expr = create_map([lit(x) for x in chain(*country_birth_mapper.items())])

    # school_completed_mapper = {'31':'LESS THAN 1ST GRADE','32':'1ST, 2ND, 3RD OR 4TH GRADE','33':'5TH OR 6TH GRADE','34':'7TH OR 8TH GRADE','35':'9TH GRADE','36':'10TH GRADE','37':'11TH GRADE','38':'12TH GRADE NO DIPLOMA','39':'HIGH SCHOOL GRAD-DIPLOMA OR EQUIV','40':'SOME COLLEGE BUT NO DEGREE','41':'ASSOCIATE DEGREE-OCCUPATIONAL/VOCATIONAL','42':'ASSOCIATE DEGREE-ACADEMIC PROGRAM','43':'BACHELORS DEGREE','44':'MASTERS DEGREE','45':'PROFESSIONAL SCHOOL DEG','46':'DOCTORATE DEGREE'}
    school_completed_mapper = {31:'No high school degree',32:'No high school degree',33:'No high school degree',34:'No high school degree',35:'No high school degree',36:'No high school degree',37:'No high school degree',38:'No high school degree',39:'High school degree',40:'High school degree',41:'Associate degree',42:'Associate degree',43:'Bachelors degree',44:'Masters degree',45:'Professional school degree',46:'Doctorate degree'}
    school_completed_expr = create_map([lit(x) for x in chain(*school_completed_mapper.items())])
    
    active_duty_mapper = {1.0:'Yes',2.0:'No'}
    active_duty_expr = create_map([lit(x) for x in chain(*active_duty_mapper.items())])

    lf_recode_mapper = {1.0:'Employed - at work', 2.0:'Employed - absent', 3.0:'Unemployed - on layoff', 4.0:'Unemployed - looking', 5.0:'Not in labor force - retired', 6.0:'Not in labor force - disabled', 7.0:'Not in labor force - other'}
    lf_recode_expr = create_map([lit(x) for x in chain(*lf_recode_mapper.items())])

    lf_recode2_mapper = {1.0:'Employed', 2.0:'Unemployed', 3.0:'NILF - discouraged', 4.0:'NILF - other'}
    lf_recode2_expr = create_map([lit(x) for x in chain(*lf_recode2_mapper.items())])

    civilian_lf_mapper = {1.0:'In Civilian LF', 2.0:'Not In Civilian LF'}
    civilian_lf_expr = create_map([lit(x) for x in chain(*civilian_lf_mapper.items())])

    employed_nonfarm_mapper = {1.0:'Employed (Excluding Farm & Private HH)'}
    employed_nonfarm_expr = create_map([lit(x) for x in chain(*employed_nonfarm_mapper.items())])

    recall_return_mapper = {1.0:'Yes',2.0:'No'}
    recall_return_expr = create_map([lit(x) for x in chain(*recall_return_mapper.items())])

    recall_look_mapper = {1.0:'Yes',2.0:'No'}
    recall_look_expr = create_map([lit(x) for x in chain(*recall_look_mapper.items())])

    job_offered_mapper = {1.0:'Yes',2.0:'No'}
    job_offered_expr = create_map([lit(x) for x in chain(*job_offered_mapper.items())])

    job_offered_week_mapper = {1.0:'Yes',2.0:'No'}
    job_offered_week_expr = create_map([lit(x) for x in chain(*job_offered_week_mapper.items())])

    available_ft_mapper = {1.0:'Yes',2.0:'No'}
    available_ft_expr = create_map([lit(x) for x in chain(*available_ft_mapper.items())])

    job_search_mapper = {1.0:'Looked Last 4 Weeks', 2.0:'Looked And Worked Last 4 Weeks', 3.0:'Looked Last 4 Weeks - Layoff', 4.0:'Unavailable Job Seekers', 5.0:'No Recent Job Search'}
    job_search_expr = create_map([lit(x) for x in chain(*job_search_mapper.items())])

    look_last_month_mapper = {1.0:'Yes',2.0:'No',3.0:'Retired',4.0:'Disabled', 5.0:'Unable to work'}
    look_last_month_expr = create_map([lit(x) for x in chain(*look_last_month_mapper.items())])

    look_last_year_mapper = {1.0:'Yes',2.0:'No'}
    look_last_year_expr = create_map([lit(x) for x in chain(*look_last_year_mapper.items())])

    last_work_mapper = {1.0:'Within Last 12 Months',2.0:'More Than 12 Months Ago',3.0:'Never Worked'}
    last_work_expr = create_map([lit(x) for x in chain(*last_work_mapper.items())])

    discouraged_mapper = {1.0:'Discouraged Worker', 2.0:'Conditionally Interested', 3.0:'NA'}
    discouraged_expr = create_map([lit(x) for x in chain(*discouraged_mapper.items())])

    retired_mapper = {1.0:'Yes', 2.0:'No'}
    retired_expr = create_map([lit(x) for x in chain(*retired_mapper.items())])

    disabled_mapper = {1.0:'Yes', 2.0:'No'}
    disabled_expr = create_map([lit(x) for x in chain(*disabled_mapper.items())])

    situation_mapper = {1.0:'Disabled', 2.0:'Ill', 3.0:'In School', 4.0:'Taking Care Of House Or Family', 5.0:'In Retirement', 6.0:'Other'}
    situation_expr = create_map([lit(x) for x in chain(*situation_mapper.items())])

    ft_pt_mapper = {1.0:'Full Time LF', 2.0:'Part Time LF'}
    ft_pt_expr = create_map([lit(x) for x in chain(*ft_pt_mapper.items())])

    ft_pt_status_mapper = {1.0:'Not In Labor Force', 2.0:'FT Hours (35+), Usually Ft', 3.0:'PT For Economic Reasons, Usually Ft', 4.0:'PT For Non-Economic Reasons, Usually Ft', 5.0:'Not At Work, Usually Ft', 6.0:'PT Hrs, Usually Pt For Economic Reasons', 7.0:'PT Hrs, Usually Pt For Non-Economic Reasons', 8.0:'FT Hours, Usually Pt For Economic Reasons', 9.0:'FT Hours, Usually Pt For Non-Economic', 10.0:'Not At Work, Usually Part-time', 11.0:'Unemployed FT', 12.0:'Unemployed PT'}
    ft_pt_status_expr = create_map([lit(x) for x in chain(*ft_pt_status_mapper.items())])

    detailed_pt_mapper = {1.0:'Usu. FT-Slack Work/Business Conditions', 2.0:'Usu. FT-Seasonal Work', 3.0:'Usu. FT-Job Started/Ended During Week', 4.0:'Usu. FT-Vacation/Personal Day', 5.0:'Usu. FT-Own Illness/Injury/Medical Appointment', 6.0:'Usu. FT-Holiday (Religious Or Legal)', 7.0:'Usu. FT-Child Care Problems', 8.0:'Usu. FT-Other Fam/Pers Obligations', 9.0:'Usu. FT-Labor Dispute', 10.0:'Usu. FT-Weather Affected Job', 11.0:'Usu. FT-School/Training', 12.0:'Usu. FT-Civic/Military Duty', 13.0:'Usu. FT-Other Reason', 14.0:'Usu. PT-Slack Work/Business Conditions', 15.0:'Usu. PT-Could Only Find Pt Work', 16.0:'Usu. PT-Seasonal Work', 17.0:'Usu. PT-Child Care Problems', 18.0:'Usu. PT-Other Fam/Pers Obligations', 19.0:'Usu. PT-Health/Medical Limitations', 20.0:'Usu. PT-School/Training', 21.0:'Usu. PT-Retired/S.S. Limit On Earnings', 22.0:'Usu. PT-Workweek <35 Hours', 23.0:'Usu. PT-Other Reason'}
    detailed_pt_expr = create_map([lit(x) for x in chain(*detailed_pt_mapper.items())])

    main_pt_mapper = {1.0:'Slack Work/Business Conditions', 2.0:'Could Only Find Part-time Work', 3.0:'Seasonal Work', 4.0:'Child Care Problems', 5.0:'Other Family/Personal Obligations', 6.0:'Health/Medical Limitations', 7.0:'School/Training', 8.0:'Retired/Social Security Limit On Earnings', 9.0:'Full-Time Workweek Is Less Than 35 Hrs', 10.0:'Other - Specify'}
    main_pt_expr = create_map([lit(x) for x in chain(*main_pt_mapper.items())])

    main_not_ft_mapper = {1.0:'Child Care Problems', 2.0:'Other Family/Personal Obligations', 3.0:'Health/Medical Limitations',4.0:'School/Training', 5.0:'Retired/SS Earnings Limit', 6.0:'Full-time Work Week Less Than 35 Hours', 7.0:'Other'}
    main_not_ft_expr = create_map([lit(x) for x in chain(*main_not_ft_mapper.items())])

    have_job_mapper = {1.0:'Yes',2.0:'No',3.0:'Retired',4.0:'Disabled',5.0:'Unable To Work'}
    have_job_expr = create_map([lit(x) for x in chain(*have_job_mapper.items())])

    want_job_mapper = {1.0:'Yes, Or Maybe, It Depends', 2.0:'No', 3.0:'Retired', 4.0:'Disabled', 5.0:'Unable'}
    want_job_expr = create_map([lit(x) for x in chain(*want_job_mapper.items())])

    want_job_ft_mapper = {1.0:'Yes', 2.0:'No', 3.0:'Regular Hours Are Full-Time'}
    want_job_ft_expr = create_map([lit(x) for x in chain(*want_job_ft_mapper.items())])

    want_job_ft_pt_mapper = {1.0:'Yes', 2.0:'No', 3.0:'Has A Job'}
    want_job_ft_pt_expr = create_map([lit(x) for x in chain(*want_job_ft_pt_mapper.items())])

    want_job_nilf_mapper = {1.0:'Want A Job', 2.0:'Other Not In Labor Force'}
    want_job_nilf_expr = create_map([lit(x) for x in chain(*want_job_nilf_mapper.items())])

    reason_unemployment_mapper = {1.0:'Job Loser/On Layoff', 2.0:'Other Job Loser', 3.0:'Temporary Job Ended', 4.0:'Job Leaver', 5.0:'Re-Entrant', 6.0:'New-Entrant'}
    reason_unemployment_expr = create_map([lit(x) for x in chain(*reason_unemployment_mapper.items())])

    reason_not_looking_mapper = {1.0:'Believes No Work Available In Area Of Expertise', 2.0:'Couldnt Find Any Work', 3.0:'Lacks Necessary Schooling/Training', 4.0:'Employers Think Too Young Or Too Old', 5.0:'Other Types Of Discrimination', 6.0:'Cant Arrange Child Care', 7.0:'Family Responsibilities', 8.0:'In School Or Other Training', 9.0:'Ill-health, Physical Disability', 10.0:'Transportation Problems', 11.0:'Other - Specify'}
    reason_not_looking_expr = create_map([lit(x) for x in chain(*reason_not_looking_mapper.items())])

    in_school_mapper = {1.0:'Yes', 2.0:'No'}
    in_school_expr = create_map([lit(x) for x in chain(*in_school_mapper.items())])

    in_school_ft_pt_mapper = {1.0:'Full-time', 2.0:'Part-time'}
    in_school_ft_pt_expr = create_map([lit(x) for x in chain(*in_school_ft_pt_mapper.items())])

    school_type_mapper = {1.0:'High School', 2.0:'College or University'}
    school_type_expr = create_map([lit(x) for x in chain(*school_type_mapper.items())])

    in_school_nilf_mapper = {1.0:'In School', 2.0:'Not In School'}
    in_school_nilf_expr = create_map([lit(x) for x in chain(*in_school_nilf_mapper.items())])

    metro_size_mapper = {0:'NOT IDENTIFIED OR NONMETROPOLITAN', 2:'100,000 - 249,999', 3:'250,000 - 499,999', 4:'500,000 - 999,999', 5:'1,000,000 - 2,499,999', 6:'2,500,000 - 4,999,999', 7:'5,000,000+'}
    metro_size_expr = create_map([lit(x) for x in chain(*metro_size_mapper.items())])

    metro_status_mapper = {1:'Metropolitan',2:'Nonmetropolitan',3:'NA'}
    metro_status_expr = create_map([lit(x) for x in chain(*metro_status_mapper.items())])

    region_mapper = {1.0:'Northeast',2.0:'Midwest',3.0:'South',4.0:'West'}
    region_expr = create_map([lit(x) for x in chain(*region_mapper.items())])

    division_mapper = {1:'New England',2:'Mid-Atlantic',3:'East North Central',4:'West North Central',5:'South Atlantic',6:'East South Central',7:'West South Central',8:'Mountain',9:'Pacific'}
    division_expr = create_map([lit(x) for x in chain(*division_mapper.items())])

    state_mapper = {1:'AL',30:'MT',2:'AK',31:'NE',4:'AZ',32:'NV',5:'AR',33:'NH',6:'CA',34:'NJ',8:'CO',35:'NM',9:'CT',36:'NY',10:'DE',37:'NC',11:'DC',38:'ND',12:'FL',39:'OH',13:'GA',40:'OK',15:'HI',41:'OR',16:'ID',42:'PA',17:'IL',44:'RI',18:'IN',45:'SC',19:'IA',46:'SD',20:'KS',47:'TN',21:'KY',48:'TX',22:'LA',49:'UT',23:'ME',50:'VT',24:'MD',51:'VA',25:'MA',53:'WA',26:'MI',54:'WV',27:'MN',55:'WI',28:'MS',56:'WY',29:'MO'}
    state_expr = create_map([lit(x) for x in chain(*state_mapper.items())])


    ######################################## End individual variable mappers ########################################
    df_master = df_master.withColumn('State',df_master['State_FIPS'])

    df_master = df_master.withColumn('Person_type', person_expr[df_master['Person_type']]).withColumn('Sex', sex_expr[df_master['Sex']]).withColumn('Race', race_expr[df_master['Race']]).withColumn('Marital_status', marital_status_expr[df_master['Marital_status']]).withColumn('Country_of_birth', country_birth_expr[df_master['Country_of_birth']]).withColumn('School_completed', school_completed_expr[df_master['School_completed']]).withColumn('Ever_active_duty', active_duty_expr[df_master['Ever_active_duty']]).withColumn('LF_recode', lf_recode_expr[df_master['LF_recode']]).withColumn('LF_recode2', lf_recode2_expr[df_master['LF_recode2']]).withColumn('Civilian_LF', civilian_lf_expr[df_master['Civilian_LF']]).withColumn('Employed_nonfarm', employed_nonfarm_expr[df_master['Employed_nonfarm']]).withColumn('Recall_return', recall_return_expr[df_master['Recall_return']]).withColumn('Recall_look', recall_look_expr[df_master['Recall_look']]).withColumn('Job_offered', job_offered_expr[df_master['Job_offered']]).withColumn('Job_offered_week', job_offered_week_expr[df_master['Job_offered_week']]).withColumn('Available_ft', available_ft_expr[df_master['Available_ft']]).withColumn('Job_search', job_search_expr[df_master['Job_search']]).withColumn('Look_last_month', look_last_month_expr[df_master['Look_last_month']]).withColumn('Look_last_year', look_last_year_expr[df_master['Look_last_year']]).withColumn('Last_work', last_work_expr[df_master['Last_work']]).withColumn('Discouraged', discouraged_expr[df_master['Discouraged']]).withColumn('Retired', retired_expr[df_master['Retired']]).withColumn('Disabled', disabled_expr[df_master['Disabled']]).withColumn('Situation', situation_expr[df_master['Situation']]).withColumn('FT_PT', ft_pt_expr[df_master['FT_PT']]).withColumn('FT_PT_status', ft_pt_status_expr[df_master['FT_PT_status']]).withColumn('Detailed_reason_part_time', detailed_pt_expr[df_master['Detailed_reason_part_time']]).withColumn('Main_reason_part_time', main_pt_expr[df_master['Main_reason_part_time']]).withColumn('Main_reason_not_full_time', main_not_ft_expr[df_master['Main_reason_not_full_time']]).withColumn('Have_job', have_job_expr[df_master['Have_job']]).withColumn('Want_job', want_job_expr[df_master['Want_job']]).withColumn('Want_job_ft', want_job_ft_expr[df_master['Want_job_ft']]).withColumn('Want_job_ft_pt', want_job_ft_pt_expr[df_master['Want_job_ft_pt']]).withColumn('Want_job_nilf', want_job_nilf_expr[df_master['Want_job_nilf']]).withColumn('Reason_unemployment', reason_unemployment_expr[df_master['Reason_unemployment']]).withColumn('Reason_not_looking', reason_not_looking_expr[df_master['Reason_not_looking']]).withColumn('In_school', in_school_expr[df_master['In_school']]).withColumn('In_school_ft_pt', in_school_ft_pt_expr[df_master['In_school_ft_pt']]).withColumn('School_type', school_type_expr[df_master['School_type']]).withColumn('In_school_nilf', in_school_nilf_expr[df_master['In_school_nilf']]).withColumn('Metro_Size', metro_size_expr[df_master['Metro_Size']]).withColumn('Metro_Status', metro_status_expr[df_master['Metro_Status']]).withColumn('Region', region_expr[df_master['Region']]).withColumn('Division', division_expr[df_master['Division']]).withColumn('State', state_expr[df_master['State']])

    #What about MSA FIPS Code? (Metro)
    #Need to map it to code on livingwage website (different metro codes) - use mapping spreadsheet downloaded


    # if year != 1995:
    #     df_cps = df_cps.replace({'Metro_Size':metro_size_mapper})
    #     df_cps = df_cps.replace({'Metro_Status':metro_status_mapper})
    return df_master

def refine_vars(df_cps):
    '''
        Performs various changes, mappings and calculations to variable
    '''

    #Hispanic and Race adjustments
    df_cps = df_cps.fillna({'Hispanic':'Not Hispanic'})
    df_cps = df_cps.withColumn('Hispanic',func.when(df_cps['Hispanic']!='Not Hispanic','Hispanic').otherwise(df_cps['Hispanic']))

    df_cps = df_cps.withColumn('Race',func.when(~df_cps['Race'].isin(['White Only','Black Only']),'Other').otherwise(df_cps['Race']))

    #Bucket Age column into Age_group
    bucket_udf = udf(categorizer, StringType() )
    df_cps = df_cps.withColumn('Age_group', bucket_udf('Age'))

    #Calculate full time vs. part time LOOK INTO THIS CALC TO ENSURE ACCURACY
    df_cps = df_cps.withColumn('FT_PT',func.when(((df_cps['LF_recode'].isin(['Employed - at work','Employed - absent']))&(df_cps['FT_PT']=='Full Time LF')),'Full_time').otherwise(df_cps['FT_PT']))
    df_cps = df_cps.withColumn('FT_PT',func.when(((df_cps['FT_PT']!='Full_time')&(df_cps['LF_recode'].isin(['Employed - at work','Employed - absent'])))&(((df_cps['Hours_per_week_last']<35)&(df_cps['Hours_per_week_last']!=-1))|(df_cps['FT_PT']=='Part Time LF')|(df_cps['Detailed_reason_part_time']!='-1')|(df_cps['Main_reason_part_time']!='-1')|(df_cps['LF_recode'].isin(['Employed - at work','Employed - absent']))),'Part_time').otherwise(df_cps['FT_PT']))
    df_cps = df_cps.withColumn('FT_PT',func.when(~df_cps['FT_PT'].isin(['Full_time','Part_time']),'NA').otherwise(df_cps['FT_PT']))
    df_cps = df_cps.fillna({'FT_PT':'NA'})

    #Calculate those in school
    df_cps = df_cps.withColumn('In_school',func.when((df_cps['Situation']=='In School')|(df_cps['In_school']=='Yes')|(df_cps['In_school_ft_pt']!='-1'),'Yes').otherwise(df_cps['In_school']))
    df_cps = df_cps.withColumn('In_school',func.when(~df_cps['In_school'].isin(['Yes','In_school']),'No').otherwise(df_cps['In_school']))
    df_cps = df_cps.fillna({'In_school':'No'})

    '''
    #Calculate those in school full time vs part time
    df_cps.loc[(df_cps['In_school_ft_pt']=='Full-time')|((df_cps['Situation']=='In School')&(df_cps['In_school_ft_pt']!='Part-time')),'In_school_ft_pt']='Full-time'
    # df_cps.loc[df_cps['In_school_ft_pt']!='Full_time','In_school_ft_pt']='No'

    df_cps.loc[(df_cps['In_school']=='Yes')&(df_cps['School_type']=='-1')&(df_cps['Age']<18),'School_type']='High School'
    df_cps.loc[(df_cps['In_school']=='Yes')&(df_cps['School_type']=='-1')&(df_cps['Age']>17),'School_type']='College or University'
    '''

    #Calculate those disabled
    df_cps = df_cps.withColumn('Disabled',func.when((df_cps['Situation']=='Disabled')|(df_cps['Disabled']=='Yes')|(df_cps['LF_recode']=='Not in labor force - disabled'),'Yes').otherwise(df_cps['Disabled']))
    df_cps = df_cps.fillna({'Disabled':'No'})

    #Calculate those discouraged
    df_cps = df_cps.withColumn('Discouraged',func.when((df_cps['Discouraged']=='Discouraged Worker')|(df_cps['Discouraged']=='Conditionally Interested')|(df_cps['LF_recode2']=='NILF - discouraged'),'Yes').otherwise(df_cps['Discouraged']))
    df_cps = df_cps.withColumn('Discouraged',func.when(df_cps['Discouraged']=='NA','No').otherwise(df_cps['Discouraged']))
    df_cps = df_cps.fillna({'Discouraged':'No'})


    #Calculate those retired
    df_cps = df_cps.withColumn('Retired',func.when((df_cps['LF_recode']=='Not in labor force - retired')|(df_cps['Retired']=='Yes')|(df_cps['Situation']=='In Retirement'),'Yes').otherwise(df_cps['Retired']))
    df_cps = df_cps.withColumn('Retired',func.when(df_cps['Retired']!='Yes','No').otherwise(df_cps['Retired']))
    df_cps = df_cps.fillna({'Retired':'No'})

    #Calculate those retired and want work
    '''
    df_cps['Retired_want_work'] = np.where((df_cps['Retired']=='Yes')&((df_cps['Want_job']=='Yes, Or Maybe, It Depends')|(df_cps['Want_job_nilf']=='Want A Job')|(df_cps['Want_job_ft_pt']=='Yes')),'Yes',np.where((df_cps['Retired']=='Yes')&((df_cps['Want_job']!='Yes, Or Maybe, It Depends')|(df_cps['Want_job_nilf']!='Want A Job')),'No','NA'))

    df_cps['Want_work'] = np.where((df_cps['Want_job']=='Yes, Or Maybe, It Depends')|(df_cps['Want_job_nilf']=='Want A Job')|(df_cps['Want_job_ft_pt']=='Yes'),'Yes','No')
    '''
    #Calculate U3 Unemployed
    df_cps = df_cps.withColumn('Unemployed_U3',df_cps['LF_recode'])
    df_cps = df_cps.withColumn('Unemployed_U3',func.when(df_cps['LF_recode'].isin(['Unemployed - on layoff','Unemployed - looking']),'Yes').otherwise(df_cps['Unemployed_U3']))
    df_cps = df_cps.withColumn('Unemployed_U3',func.when(df_cps['Unemployed_U3']!='Yes','No').otherwise(df_cps['Unemployed_U3']))
    df_cps = df_cps.fillna({'Unemployed_U3':'No'})

    #Calculate U6 Unemployed
    # df_cps['Unemployed_U6'] = np.where(((df_cps['LF_recode'].isin(['Unemployed - on layoff','Unemployed - looking']))|(df_cps['Look_last_year']=='Yes')|(df_cps['Look_last_month']=='Yes')|(df_cps['Job_offered_week']=='Yes')|(df_cps['Last_work']=='Within Last 12 Months')|(df_cps['Reason_not_looking'].isin(['Believes No Work Available In Area Of Expertise','Couldnt Find Any Work','Lacks Necessary Schooling/Training']))|(df_cps['Discouraged']=='Yes')|(df_cps['FT_PT_status'].isin(['PT Hrs, Usually Pt For Economic Reasons','PT for Economic Reasons, Usually Ft']))),'Yes','No')
    df_cps = df_cps.withColumn('Unemployed_U6',df_cps['LF_recode'])
    df_cps = df_cps.withColumn('Unemployed_U6',func.when(((df_cps['LF_recode'].isin(['Unemployed - on layoff','Unemployed - looking']))|(df_cps['Look_last_year']=='Yes')|(df_cps['Look_last_month']=='Yes')|(df_cps['Job_offered_week']=='Yes')|(df_cps['Last_work']=='Within Last 12 Months')|(df_cps['Reason_not_looking'].isin(['Believes No Work Available In Area Of Expertise','Couldnt Find Any Work','Lacks Necessary Schooling/Training']))|(df_cps['Discouraged']=='Yes')|(df_cps['FT_PT_status'].isin(['PT Hrs, Usually Pt For Economic Reasons','PT for Economic Reasons, Usually Ft']))),'Yes').otherwise(df_cps['Unemployed_U6']))
    df_cps = df_cps.withColumn('Unemployed_U6',func.when(df_cps['Unemployed_U6']!='Yes','No').otherwise(df_cps['Unemployed_U6']))
    df_cps = df_cps.fillna({'Unemployed_U6':'No'})

    #Calculate in Labor Force U3
    df_cps = df_cps.withColumn('Labor_force_U3',df_cps['LF_recode'])
    df_cps = df_cps.withColumn('Labor_force_U3',func.when(df_cps['LF_recode'].isin(['Unemployed - on layoff','Unemployed - looking','Employed - at work','Employed - absent']),'Yes').otherwise(df_cps['Labor_force_U3']))
    df_cps = df_cps.withColumn('Labor_force_U3',func.when(df_cps['Labor_force_U3']!='Yes','No').otherwise(df_cps['Labor_force_U3']))
    df_cps = df_cps.fillna({'Labor_force_U3':'No'})

    #Calculate in Labor Force U6
    df_cps = df_cps.withColumn('Labor_force_U6',df_cps['LF_recode'])
    df_cps = df_cps.withColumn('Labor_force_U6',func.when((df_cps['LF_recode'].isin(['Unemployed - on layoff','Unemployed - looking','Employed - at work','Employed - absent'])|(df_cps['Unemployed_U6']=='Yes')),'Yes').otherwise(df_cps['Labor_force_U6']))
    df_cps = df_cps.withColumn('Labor_force_U6',func.when(df_cps['Labor_force_U6']!='Yes','No').otherwise(df_cps['Labor_force_U6']))
    df_cps = df_cps.fillna({'Labor_force_U6':'No'})

    return df_cps


def weight(df_cps):

    # df_cps = df_cps.withColumn('Pop_percent',df_cps['POPESTIMATE'])
    df_cps = df_cps.join(df_totals,on=['Year'],how='left')
    ##################Not sure why I need to multiply by 10 but it isn't doing proper division otherwise
    df_cps = df_cps.withColumn('Pop_percent',10*df_cps['POPESTIMATE']/df_cps['sum(POPESTIMATE)'])
    # print(df_cps.show())
    '''
    df_cps['Percent'+str(year)]=df_cps['POPESTIMATE'+str(year)]/df_state['POPESTIMATE'+str(year)].sum()

    df_cps['Count'] = np.arange(1,len(df_cps)+1)
    '''
    weight_mapper = df_cps.groupBy('Age_group','Sex','Race','Hispanic','State_FIPS','Year','Month').count()
    # print(weight_mapper.show())
    # print(weight_mapper.groupby().sum('count').collect()[0][0])
    weight_mapper = weight_mapper.withColumn('Ratio',weight_mapper['count']/weight_mapper.groupby().sum('count').collect()[0][0])
    # print(weight_mapper.show())
    df_cps = df_cps.join(weight_mapper,on=['Age_group','Sex','Race','Hispanic','State_FIPS','Year','Month'],how='left')
    df_cps = df_cps.withColumn('Weight',df_cps['Pop_percent']/df_cps['Ratio'])
    # print(df_cps.groupby().sum('Weight').collect()[0][0])
    # print(df_cps.show())
    # df_cps = df_cps.drop(*['Pop_percent','sum(POPESTIMATE)','count','Ratio'])
    '''
    weight_mapper['Ratio'] = weight_mapper['Ratio']/len(df_cps)

    df_cps = df_cps.merge(weight_mapper,on=['Sex','Race','Age_group','Hispanic','State_FIPS'],how='left',left_index=True)

    df_cps['Weight'] = df_cps['Percent'+str(year)]/df_cps['Ratio']

    df_cps.drop(['Count','Ratio','Percent'+str(year)],axis=1,inplace=True)

    #Try adjusting weights so they add up to length of df_cps
    #Doesn't seem to make much of a difference either way
    df_cps['Weight'] = df_cps['Weight']*(len(df_cps)/df_cps['Weight'].sum())
    '''
    df_cps = df_cps.withColumn('Weight',df_cps['Weight']*(df_cps.count()/df_cps.groupby().sum('Weight').collect()[0][0]))
    # print(df_cps.groupby().sum('Weight').collect()[0][0])
    return df_cps

def categorizer(age):
    if age < 20:
        return 1
    elif age < 25:
        return 2
    elif age < 30:
        return 3
    elif age < 35:
        return 4
    elif age < 40:
        return 5
    elif age < 45:
        return 6
    elif age < 50:
        return 7
    elif age < 55:
        return 8
    elif age < 60:
        return 9
    elif age < 65:
        return 10
    elif age < 70:
        return 11
    elif age < 75:
        return 12
    elif age < 80:
        return 13
    elif age < 85:
        return 14
    else: 
        return 15


def refine_df_state(df_state):
    df_state['POPESTIMATE2019'] = df_state['POPESTIMATE2018']
    df_state['POPESTIMATE2020'] = df_state['POPESTIMATE2018']

    df_state = pd.melt(df_state,id_vars=['Age_group','Sex','Hispanic','Race','State_FIPS'],value_vars=['POPESTIMATE1990','POPESTIMATE1991','POPESTIMATE1992','POPESTIMATE1993','POPESTIMATE1994','POPESTIMATE1995','POPESTIMATE1996','POPESTIMATE1997','POPESTIMATE1998','POPESTIMATE1999','POPESTIMATE2000','POPESTIMATE2001','POPESTIMATE2002','POPESTIMATE2003','POPESTIMATE2004','POPESTIMATE2005','POPESTIMATE2006','POPESTIMATE2007','POPESTIMATE2008','POPESTIMATE2009','POPESTIMATE2010','POPESTIMATE2011','POPESTIMATE2012','POPESTIMATE2013','POPESTIMATE2014','POPESTIMATE2015','POPESTIMATE2016','POPESTIMATE2017','POPESTIMATE2018','POPESTIMATE2019','POPESTIMATE2020']).replace({'POPESTIMATE1990':1990,'POPESTIMATE1991':1991,'POPESTIMATE1992':1992,'POPESTIMATE1993':1993,'POPESTIMATE1994':1994,'POPESTIMATE1995':1995,'POPESTIMATE1996':1996,'POPESTIMATE1997':1997,'POPESTIMATE1998':1998,'POPESTIMATE1999':1999,'POPESTIMATE2000':2000,'POPESTIMATE2001':2001,'POPESTIMATE2002':2002,'POPESTIMATE2003':2003,'POPESTIMATE2004':2004,'POPESTIMATE2005':2005,'POPESTIMATE2006':2006,'POPESTIMATE2007':2007,'POPESTIMATE2008':2008,'POPESTIMATE2009':2009,'POPESTIMATE2010':2010,'POPESTIMATE2011':2011,'POPESTIMATE2012':2012,'POPESTIMATE2013':2013,'POPESTIMATE2014':2014,'POPESTIMATE2015':2015,'POPESTIMATE2016':2016,'POPESTIMATE2017':2017,'POPESTIMATE2018':2018,'POPESTIMATE2019':2019,'POPESTIMATE2020':2020}).rename(columns={'variable':'Year','value':'POPESTIMATE'})

    schema = StructType([
        StructField("Age_group", StringType(), True),
        StructField("Sex", StringType(), True),
        StructField("Hispanic", StringType(), True),
        StructField("Race", StringType(), True),
        StructField("State_FIPS", StringType(), True),
        StructField("Year", IntegerType(), True),
        StructField("POPESTIMATE", IntegerType(), True)
    ])
    df = sqlContext.createDataFrame(df_state, schema)
    df = df.withColumn("State_FIPS", df["State_FIPS"].cast(IntegerType()))
    return df







if __name__=='__main__':
    fp = '/media/dhense/Elements/PublicData/cps_csv_files/'
    state_pop_pickle = 'state_pop.pickle'
    pickle_path = '/home/dhense/PublicData/Economic_analysis/intermediate_files/'
    export_path = '/home/dhense/PublicData/Economic_analysis/intermediate_files/cps_csv/'

    print("...loading pickle")
    tmp = open(pickle_path+state_pop_pickle,'rb')
    df_state = pickle.load(tmp)
    tmp.close()

    
    df_state = refine_df_state(df_state)
    # print(df_state.show())
    df_totals = df_state.groupBy('Year').sum('POPESTIMATE')
    

    tic = time.perf_counter()
    
    year = 1999
    m='jan'
    df_master = sqlContext.read.format('csv').options(header='true', inferSchema='true').load(fp+'cps_'+m+str(year)+'.csv')
    
    months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    # months = ['feb']
    months_dict = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}
    for year in range(1999,2000):
        for m in months:
            if year == 1999 and m == 'jan':
                continue
            if year==2020 and m=='sep':
                break
            df = sqlContext.read.format('csv').options(header='true', inferSchema='true').load(fp+'cps_'+m+str(year)+'.csv')
            df_master = df_master.union(df)

    toc = time.perf_counter()
    print(f"Loading took {toc - tic:0.1f} seconds") 
    # print(df_master.count())
    
    
    # df_sub = df_master.limit(2)
    
    tic1 = time.perf_counter()
    df_master = var_mapper(df_master)
    toc1 = time.perf_counter()
    print(f"Mapping took {toc1 - tic1:0.1f} seconds") 

    tic2 = time.perf_counter()
    df_master = df_master.filter(df_master['Person_type']=='Adult Civilian')
    df_master = df_master.filter(df_master.Age>15)
    df_master = refine_vars(df_master)
    toc2 = time.perf_counter()
    print(f"Refining took {toc2 - tic2:0.1f} seconds") 

    tic3 = time.perf_counter()
    df_master = df_master.join(df_state,on=['Sex','Race','Age_group','Hispanic','State_FIPS','Year'], how='left')
    toc3 = time.perf_counter()
    print(f"Merging took {toc3 - tic3:0.1f} seconds")
    # print(df_state.show())
    # df_master = df_master.join(df_totals,on=['Year'],how='left')
    # df_master = df_master.withColumn('Pop_percent',df_master['POPESTIMATE']/df_master['sum(POPESTIMATE)'])
    # df_master = df_master.select("*").withColumn("id", monotonically_increasing_id())
    # weight_mapper = df_master.groupBy('Age_group','Sex','Race','Hispanic','State_FIPS','Year','Month').agg({'id':'count'})
    # weight_mapper = weight_mapper.withColumn('Ratio',weight_mapper['count(id)']/weight_mapper.groupby().sum('count(id)').collect()[0][0])
    # df_master = df_master.join(weight_mapper,on=['Age_group','Sex','Race','Hispanic','State_FIPS','Year','Month'],how='left')
    # df_master = df_master.withColumn('Weight',df_master['Pop_percent']/df_master['Ratio'])
    # print(df_master.groupby().sum('Weight').collect()[0][0])
    # print(weight_mapper.show())
    # print(df_master.show())
    # print(df_master.groupBy('Age_group').count().show())
    # print(df_master.groupBy('Sex').count().show())
    # print(df_master.groupBy('Race').count().show())
    # print(df_master.groupBy('Hispanic').count().show())
    # print(df_master.groupBy('State_FIPS').count().show())
    # print(df_master.groupBy('Year').count().show())
    
    # print(df_master.dtypes)
    tic4 = time.perf_counter()
    df_master = weight(df_master)
    toc4 = time.perf_counter()
    print(f"Weighting took {toc4 - tic4:0.1f} seconds") 

    #Drop unnecessary columns
    df_master = df_master.drop(*['sum(POPESTIMATE)','count','Ratio','Ref_person','Person_line_num','Person_type','Interview_Status'])
    
    #print(df_master.count())
    # print(df_master.agg(func.sum('Weight')).collect()[0][0])

    # print(df_master.groupBy('Unemployed_U3').count().show())
    # print(df_master.groupBy('Unemployed_U6').count().show())
    # print(df_master.groupBy('Labor_force_U3').count().where(col('Labor_force_U3')=='Yes').select(col('count')).collect()[0][0])
    # print(df_master.groupBy('Labor_force_U6').count().show())



    # df_master.write.format('parquet').save(pickle_path+'cps_spark.parquet')
    # df_master1 = df_master.filter()


    #year=1999
    #m = 'jan'
    #months_dict = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}

    #dff = df_master.filter((df_master.Year==year)&(df_master.Month==months_dict[m]))
    '''
    tic5 = time.perf_counter()
    # df_master.write.format('csv').mode('overwrite').option('header', 'true').save(export_path+'cps_'+str(year)+'.csv')
    tic6 = time.perf_counter()
    dfp = df_master.toPandas()
    toc6 = time.perf_counter()
    print(f"toPandas took {toc6 - tic6:0.1f} seconds") 
    dfp.to_csv(export_path+'cps_'+str(year)+'.csv',index=False)
    toc5 = time.perf_counter()
    print(f"Writing took {toc5 - tic5:0.1f} seconds") 
    #print(dff.count())
    # dff = sqlContext.read.format('csv').options(header='true', inferSchema='true').load(export_path+'cps_'+str(year)+'.csv')
    # print(dff.show())
    '''

    
    #Monthly export takes around 1 minute per file. This is too long...
    # months = ['jan']
    # months_dict = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}
    tic5 = time.perf_counter()
    for year in range(1999,2000):
        for m in months:
            if year==2020 and m=='sep':
                break
            dff = df_master.filter((df_master.Year==year)&(df_master.Month==months_dict[m]))
            dff = dff.toPandas()
            dff.to_csv(export_path+'cps_'+m+str(year)+'.csv',index=False)
            # dff.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(export_path+'cps_'+m+str(year)+'.csv')

    toc5 = time.perf_counter()
    print(f"Writing took {toc5 - tic5:0.1f} seconds") 
    




    #No need to export! calculate right here
    # (df['Unemployed_U6']=='Yes').groupby([df['Month'],df['Year']]).sum()/(df['Labor_force_U6']=='Yes').groupby([df['Month'],df['Year']]).sum()
    #df.groupby(['Month','Year']).apply(lambda x: x[x['Unemployed_U3'] == 'Yes']['Weight'].sum())

    #IS Weight being calculated correctly?? Something wrong with weight...
    # df.groupby(['Month','Year']).apply(lambda x: x[x['Unemployed_U3'] == 'Yes']['Weight'].sum())

    #Get: Month, Year, U3_Weighted, U6_Weighted, LFPR_U3, LFPR_U6,
    #This is great - just need to fix Weight function
    #Unweighted
    #print(df_master.groupBy('Unemployed_U3').count().where(col('Unemployed_U3')=='Yes').select(col('count')).collect()[0][0]/lfu3)

    tic6 = time.perf_counter()

    lendf = df_master.count()

    lfu3 = df_master.groupBy('Month','Year','Labor_force_U3').count().where(col('Labor_force_U3')=='Yes').select(col('count')).collect()[0][0]
    #U3 rate
    df_u3 = df_master.groupBy('Month','Year','Unemployed_U3').agg(when(col('Unemployed_U3')=='Yes', func.sum('Weight')/lfu3).alias('U3_Weighted')).where(col('Unemployed_U3')=='Yes').select('Month','Year','U3_Weighted')
    # print(df_u3.show())

    #LF counts U3
    # print(df_master.groupBy('Month','Year','Labor_force_U3').count().where(col('Labor_force_U3')=='Yes').select(col('count')).show())
    df_lfpr_u3 = df_master.groupBy('Month','Year','Labor_force_U3').agg(when(col('Labor_force_U3')=='Yes', func.sum('Weight')/lendf).alias('LFPR_U3_Weighted')).where(col('Labor_force_U3')=='Yes').select('Month','Year','LFPR_U3_Weighted')
    # print(df_lfpr_u3.show())

    lfu6 = df_master.groupBy('Month','Year','Labor_force_U6').count().where(col('Labor_force_U6')=='Yes').select(col('count')).collect()[0][0]
    #U6 rate
    df_u6 = df_master.groupBy('Month','Year','Unemployed_U6').agg(when(col('Unemployed_U6')=='Yes', func.sum('Weight')/lfu6).alias('U6_Weighted')).where(col('Unemployed_U6')=='Yes').select('Month','Year','U6_Weighted')
    # print(df_u6.show())
    #LF counts U6
    df_lfpr_u6 = df_master.groupBy('Month','Year','Labor_force_U6').agg(when(col('Labor_force_U6')=='Yes', func.sum('Weight')/lendf).alias('LFPR_U6_Weighted')).where(col('Labor_force_U6')=='Yes').select('Month','Year','LFPR_U6_Weighted')
    # print(df_lfpr_u6.show())

    df_stats = df_u3.join(df_lfpr_u3,on=['Month','Year'], how='left')
    df_stats = df_stats.join(df_u6,on=['Month','Year'], how='left')
    df_stats = df_stats.join(df_lfpr_u6,on=['Month','Year'], how='left')
    print(df_stats.show())
    

    toc6 = time.perf_counter()
    print(f"Stat calcs took {toc6 - tic6:0.1f} seconds")
    