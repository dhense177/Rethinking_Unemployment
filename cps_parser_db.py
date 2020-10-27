import os, pickle, time
import pandas as pd
import numpy as np
import pyspark.sql.functions as f
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
#from statsmodels.tsa.seasonal import seasonal_decompose
from common_functions import fips_mapper, cut

pd.set_option('display.float_format', lambda x: '%.4f' % x)

# spark = SparkSession.builder.appName('pandasToSparkDF').getOrCreate()
#sc = SparkContext("local", "CPS Parser")
#sqlContext = SQLContext(sc)


def extract_record(df,yr,m):
    col_dict = {'HHID':(0,15),'Ref_person':(117,119),'Person_line_num':(146,148),'Person_type':(160,162),'Interview_Status':(56,58),'Age':(121,123),'Sex':(128,130),'Race':(138,140),'Hispanic':(140,142),'Marital_status':(124,126),'Country_of_birth':(162,165),'School_completed':(136,138),'Ever_active_duty':(130,132),'LF_recode':(179,181),'LF_recode2':(392,394),'Civilian_LF':(386,388),'Employed_nonfarm':(479,481),'Have_job':(205,207),'Unpaid_family_work':(183,185),'Recall_return':(276,278),'Recall_look':(280,282),'Job_offered':(331,333),'Job_offered_week':(358,360),'Available_ft':(249,251),'Job_search':(400,402),'Look_last_month':(293,295),'Look_last_year':(350,352),'Last_work':(564,566),'Discouraged':(388,390),'Retired':(566,568),'Disabled':(203,205),'Situation':(568,570),'FT_PT':(396,398),'FT_PT_status':(415,417),'Detailed_reason_part_time':(404,406),'Main_reason_part_time':(228,230),'Main_reason_not_full_time':(230,232),'Want_job':(346,348),'Want_job_ft':(226,228),'Want_job_ft_pt':(199,201),'Want_job_nilf':(417,419),'Reason_unemployment':(411,413),'Reason_not_looking':(348,350),'Hours_per_week':(217,219),'Hours_per_week_last':(242,244),'In_school':(574,576),'In_school_ft_pt':(576,578),'School_type':(578,580),'In_school_nilf':(580,582),'State_FIPS':(92,94),'County_FIPS':(100,103),'Metro_Code':(95,100),'Metro_Size':(106,107),'Metro_Status':(104,105),'Region':(88,90),'Division':(90,91)}

    df_p = pd.DataFrame()


    for k,v in col_dict.items():
        if ((year < 2003)&(k=='Hispanic')):
            df_p[k] = [i[0][156:158] for i in df.values]
        elif ((year==1995)&((k=='County_FIPS')|(k=='Metro_Code')|(k=='Metro_Size'))):
            continue
        else:
            df_p[k] = [i[0][v[0]:v[1]] for i in df.values]

    df_p['Year'] = yr
    df_p['Month'] = m
    df_p['State_FIPS'] = df_p['State_FIPS'].astype(str).str.strip().apply(lambda x: str(x).zfill(2) if x != '' else '')

    if year != 1995:
        df_p['County_FIPS'] = df_p['County_FIPS'].astype(str).str.strip().apply(lambda x: str(x).zfill(3) if x != '' else '')

        df_p['FIPS'] = df_p['State_FIPS']+df_p['County_FIPS']

    #Calculate unique Person ID (PID)
    df_p['PID'] = df_p['HHID']+df_p['Ref_person'].str.strip().apply(lambda x: str(x).zfill(2))+df_p['Person_line_num'].str.strip().apply(lambda x: str(x).zfill(2))+df_p['Sex'].str[1]+df_p['Race'].str[1]+df_p['Hispanic'].str[1]+df_p['Country_of_birth'].str.strip().apply(lambda x: str(x).zfill(3))

    #Calculate unique Survey ID (SID)
    df_p['SID'] = df_p['PID']+df_p['Year']+df_p['Month'].str.zfill(2)

    return df_p


def var_mapper(df_cps):
    person_mapper = {' 1':'Child',' 2':'Adult Civilian',' 3':'Adult Armed Forces'}
    sex_mapper = {' 1':'Male',' 2':'Female'}
    race_mapper = {' 1':'White Only', ' 2':'Black Only', ' 3':'American Indian, Alaskan Native Only', ' 4':'Asian Only', ' 5':'Hawaiian/Pacific Islander Only', ' 6':'White-Black', ' 7':'White-AI', ' 8':'White-Asian', ' 9':'White-HP', '10':'Black-AI', '11':'Black-Asian', '12':'Black-HP', '13':'AI-Asian', '14':'AI-HP', '15':'Asian-HP', '16':'W-B-AI', '17':'W-B-A', '18':'W-B-HP', '19':'W-AI-A', '20':'W-AI-HP', '21':'W-A-HP', '22':'B-AI-A', '23':'W-B-AI-A', '24':'W-AI-A-HP', '25':'Other 3 Race Combinations', '26':'Other 4 and 5 Race Combinations'}


    if year < 2003:
        hispanic_mapper = {' 1':'Hispanic', ' 2':'Not Hispanic'}
    else:
        hispanic_mapper = {' 1':'Mexican', ' 2':'Peurto Rican',' 3':'Cuban',' 4':'Central/South American', ' 5':'Other Spanish'}
    
    # marital_status_mapper = {' 1':'Married- spouse present',' 2':'Married- spouse absent',' 3':'Widowed',' 4':'Divorced',' 5':'Separated',' 6':'Never married'}
    marital_status_mapper = {' 1':'Married',' 2':'Not married',' 3':'Not married',' 4':'Not married',' 5':'Not married',' 6':'Not married'}
    country_birth_mapper = {'057':'United States','072':'Puerto Rico','096':'U.S. OUTLYING AREA','231':'Philippines','060':'American Samoa', '233':'Saudi Arabia','066':'Guam','234':'Singapore','072':'Puerto Rico','237':'Syria','078':'U.S. Virgin Islands', '238':'Taiwan','102':'Austria','239':'Thailand','103':'Belgium','240':'Turkey','105':'Czechoslovakia','242':'Vietnam','106':'Denmark','245':'Asia','108':'Finland','252':'Middle East','109':'France','253':'Palestine','110':'Germany','300':'Bermuda','116':'Greece','301':'Canada','117':'Hungary','304':'North America','119':'Ireland/Eire','310':'Belize','120':'Italy','311':'Costa Rica','126':'Holland','312':'El Salvador','126':'Netherlands','313':'Guatemala','127':'Norway','314':'Honduras','128':'Poland','315':'Mexico','129':'Portugal','316':'Nicaragua','130':'Azores','317':'Panama','132':'Romania','318':'Central America','134':'Spain','333':'Bahamas','136':'Sweden','334':'Barbados','137':'Switzerland','337':'Cuba','138':'Great Britain','338':'Dominica','139':'England','339':'Dominican Republic','140':'Scotland','340':'Grenada','142':'Northern Ireland','342':'Haiti','147':'Yugoslavia','343':'Jamaica','148':'Europe','351':'Trinidad & Tobago','155':'Czech Republic','353':'Caribbean','156':'Slovakia/Slovak Republic','375':'Argentina','180':'USSR','376':'Bolivia','183':'Latvia','377':'Brazil','184':'Lithuania','378':'Chile','185':'Armenia','379':'Colombia','192':'Russia','380':'Ecuador','195':'Ukraine','383':'Guyana','200':'Afghanistan','385':'Peru','202':'Bangladesh','387':'Uruguay','205':'Burma','388':'Venezuela','206':'Cambodia','389':'South America','207':'China','415':'Egypt','209':'Hong Kong','417':'Ethiopia','210':'India','421':'Ghana','211':'Indonesia','427':'Kenya','212':'Iran','436':'Morocco','213':'Iraq','440':'Nigeria','214':'Israel','449':'South Africa','215':'Japan','462':'Other Africa','216':'Jordan','468':'North Africa','217':'Korea/South Korea', '501':'Australia','221':'Laos','507':'Figi','222':'Lebanon','514':'New Zealand','224':'Malaysia','527':'Pacific Islands','229':'Pakistan','555':'Abroad, country not known'}

    # school_completed_mapper = {'31':'LESS THAN 1ST GRADE','32':'1ST, 2ND, 3RD OR 4TH GRADE','33':'5TH OR 6TH GRADE','34':'7TH OR 8TH GRADE','35':'9TH GRADE','36':'10TH GRADE','37':'11TH GRADE','38':'12TH GRADE NO DIPLOMA','39':'HIGH SCHOOL GRAD-DIPLOMA OR EQUIV','40':'SOME COLLEGE BUT NO DEGREE','41':'ASSOCIATE DEGREE-OCCUPATIONAL/VOCATIONAL','42':'ASSOCIATE DEGREE-ACADEMIC PROGRAM','43':'BACHELORS DEGREE','44':'MASTERS DEGREE','45':'PROFESSIONAL SCHOOL DEG','46':'DOCTORATE DEGREE'}
    school_completed_mapper = {'31':'No high school degree','32':'No high school degree','33':'No high school degree','34':'No high school degree','35':'No high school degree','36':'No high school degree','37':'No high school degree','38':'No high school degree','39':'High school degree','40':'High school degree','41':'Associate degree','42':'Associate degree','43':'Bachelors degree','44':'Masters degree','45':'Professional school degree','46':'Doctorate degree'}
    # school_completed_mapper = {'LESS THAN 1ST GRADE':'No high school degree','1ST, 2ND, 3RD OR 4TH GRADE':'No high school degree','5TH OR 6TH GRADE':'No high school degree','7TH OR 8TH GRADE':'No high school degree','9TH GRADE':'No high school degree','10TH GRADE':'No high school degree','11TH GRADE':'No high school degree','12TH GRADE NO DIPLOMA':'No high school degree','HIGH SCHOOL GRAD-DIPLOMA OR EQUIV':'High school degree','SOME COLLEGE BUT NO DEGREE':'Some college but no degree','ASSOCIATE DEGREE-OCCUPATIONAL/VOCATIONAL':'Associate degree','ASSOCIATE DEGREE-ACADEMIC PROGRAM':'Associate degree','BACHELORS DEGREE':'Bachelors degree','MASTERS DEGREE':'Masters degree','PROFESSIONAL SCHOOL DEG':'Professional school degree','DOCTORATE DEGREE':'Doctorate degree'}
    
    active_duty_mapper = {' 1':'Yes',' 2':'No'}

    lf_recode_mapper = {' 1':'Employed - at work', ' 2':'Employed - absent', ' 3':'Unemployed - on layoff', ' 4':'Unemployed - looking', ' 5':'Not in labor force - retired', ' 6':'Not in labor force - disabled', ' 7':'Not in labor force - other'}
    lf_recode2_mapper = {' 1':'Employed', ' 2':'Unemployed', ' 3':'NILF - discouraged', ' 4':'NILF - other'}
    civilian_lf_mapper = {' 1':'In Civilian LF', ' 2':'Not In Civilian LF'}
    employed_nonfarm_mapper = {' 1':'Employed (Excluding Farm & Private HH)'}
    recall_return_mapper = {' 1':'Yes',' 2':'No'}
    recall_look_mapper = {' 1':'Yes',' 2':'No'}
    job_offered_mapper = {' 1':'Yes',' 2':'No'}
    job_offered_week_mapper = {' 1':'Yes',' 2':'No'}
    available_ft_mapper = {' 1':'Yes',' 2':'No'}
    job_search_mapper = {' 1':'Looked Last 4 Weeks', ' 2':'Looked And Worked Last 4 Weeks', ' 3':'Looked Last 4 Weeks - Layoff', ' 4':'Unavailable Job Seekers', ' 5':'No Recent Job Search'}
    look_last_month_mapper = {' 1':'Yes',' 2':'No',' 3':'Retired',' 4':'Disabled', ' 5':'Unable to work'}
    look_last_year_mapper = {' 1':'Yes',' 2':'No'}
    last_work_mapper = {' 1':'Within Last 12 Months',' 2':'More Than 12 Months Ago',' 3':'Never Worked'}
    discouraged_mapper = {' 1':'Discouraged Worker', ' 2':'Conditionally Interested', ' 3':'NA'}
    retired_mapper = {' 1':'Yes', ' 2':'No'}
    disabled_mapper = {' 1':'Yes', ' 2':'No'}
    situation_mapper = {' 1':'Disabled', ' 2':'Ill', ' 3':'In School', ' 4':'Taking Care Of House Or Family', ' 5':'In Retirement', ' 6':'Other'}
    ft_pt_mapper = {' 1':'Full Time LF', ' 2':'Part Time LF'}
    ft_pt_status_mapper = {' 1':'Not In Labor Force', ' 2':'FT Hours (35+), Usually Ft', ' 3':'PT For Economic Reasons, Usually Ft', ' 4':'PT For Non-Economic Reasons, Usually Ft', ' 5':'Not At Work, Usually Ft', ' 6':'PT Hrs, Usually Pt For Economic Reasons', ' 7':'PT Hrs, Usually Pt For Non-Economic Reasons', ' 8':'FT Hours, Usually Pt For Economic Reasons', ' 9':'FT Hours, Usually Pt For Non-Economic', '10':'Not At Work, Usually Part-time', '11':'Unemployed FT', '12':'Unemployed PT'}
    detailed_pt_mapper = {' 1':'Usu. FT-Slack Work/Business Conditions', ' 2':'Usu. FT-Seasonal Work', ' 3':'Usu. FT-Job Started/Ended During Week', ' 4':'Usu. FT-Vacation/Personal Day', ' 5':'Usu. FT-Own Illness/Injury/Medical Appointment', ' 6':'Usu. FT-Holiday (Religious Or Legal)', ' 7':'Usu. FT-Child Care Problems', ' 8':'Usu. FT-Other Fam/Pers Obligations', ' 9':'Usu. FT-Labor Dispute', '10':'Usu. FT-Weather Affected Job', '11':'Usu. FT-School/Training', '12':'Usu. FT-Civic/Military Duty', '13':'Usu. FT-Other Reason', '14':'Usu. PT-Slack Work/Business Conditions', '15':'Usu. PT-Could Only Find Pt Work', '16':'Usu. PT-Seasonal Work', '17':'Usu. PT-Child Care Problems', '18':'Usu. PT-Other Fam/Pers Obligations', '19':'Usu. PT-Health/Medical Limitations', '20':'Usu. PT-School/Training', '21':'Usu. PT-Retired/S.S. Limit On Earnings', '22':'Usu. PT-Workweek <35 Hours', '23':'Usu. PT-Other Reason'}
    main_pt_mapper = {' 1':'Slack Work/Business Conditions', ' 2':'Could Only Find Part-time Work', ' 3':'Seasonal Work', ' 4':'Child Care Problems', ' 5':'Other Family/Personal Obligations', ' 6':'Health/Medical Limitations', ' 7':'School/Training', ' 8':'Retired/Social Security Limit On Earnings', ' 9':'Full-Time Workweek Is Less Than 35 Hrs', '10':'Other - Specify'}
    main_not_ft_mapper = {' 1':'Child Care Problems', ' 2':'Other Family/Personal Obligations', ' 3':'Health/Medical Limitations',' 4':'School/Training', ' 5':'Retired/SS Earnings Limit', ' 6':'Full-time Work Week Less Than 35 Hours', ' 7':'Other'}
    have_job_mapper = {' 1':'Yes',' 2':'No',' 3':'Retired',' 4':'Disabled',' 5':'Unable To Work'}
    want_job_mapper = {' 1':'Yes, Or Maybe, It Depends', ' 2':'No', ' 3':'Retired', ' 4':'Disabled', ' 5':'Unable'}
    want_job_ft_mapper = {' 1':'Yes', ' 2':'No', ' 3':'Regular Hours Are Full-Time'}
    want_job_ft_pt_mapper = {' 1':'Yes', ' 2':'No', ' 3':'Has A Job'}
    want_job_nilf_mapper = {' 1':'Want A Job', ' 2':'Other Not In Labor Force'}
    reason_unemployment_mapper = {' 1':'Job Loser/On Layoff', ' 2':'Other Job Loser', ' 3':'Temporary Job Ended', ' 4':'Job Leaver', ' 5':'Re-Entrant', ' 6':'New-Entrant'}
    reason_not_looking_mapper = {' 1':'Believes No Work Available In Area Of Expertise', ' 2':'Couldnt Find Any Work', ' 3':'Lacks Necessary Schooling/Training', ' 4':'Employers Think Too Young Or Too Old', ' 5':'Other Types Of Discrimination', ' 6':'Cant Arrange Child Care', ' 7':'Family Responsibilities', ' 8':'In School Or Other Training', ' 9':'Ill-health, Physical Disability', '10':'Transportation Problems', '11':'Other - Specify'}
    in_school_mapper = {' 1':'Yes', ' 2':'No'}
    in_school_ft_pt_mapper = {' 1':'Full-time', ' 2':'Part-time'}
    school_type_mapper = {' 1':'High School', ' 2':'College or University'}

    in_school_nilf_mapper = {' 1':'In School', ' 2':'Not In School'}
    metro_size_mapper = {'0':'NOT IDENTIFIED OR NONMETROPOLITAN', '2':'100,000 - 249,999', '3':'250,000 - 499,999', '4':'500,000 - 999,999', '5':'1,000,000 - 2,499,999', '6':'2,500,000 - 4,999,999', '7':'5,000,000+'}
    metro_status_mapper = {'1':'Metropolitan','2':'Nonmetropolitan','3':'NA'}
    region_mapper = {' 1':'Northeast',' 2':'Midwest',' 3':'South',' 4':'West'}
    division_mapper = {'1':'New England','2':'Mid-Atlantic','3':'East North Central','4':'West North Central','5':'South Atlantic','6':'East South Central','7':'West South Central','8':'Mountain','9':'Pacific'}
    state_mapper = {'01':'AL','30':'MT','02':'AK','31':'NE','04':'AZ','32':'NV','05':'AR','33':'NH','06':'CA','34':'NJ','08':'CO','35':'NM','09':'CT','36':'NY','10':'DE','37':'NC','11':'DC','38':'ND','12':'FL','39':'OH','13':'GA','40':'OK','15':'HI','41':'OR','16':'ID','42':'PA','17':'IL','44':'RI','18':'IN','45':'SC','19':'IA','46':'SD','20':'KS','47':'TN','21':'KY','48':'TX','22':'LA','49':'UT','23':'ME','50':'VT','24':'MD','51':'VA','25':'MA','53':'WA','26':'MI','54':'WV','27':'MN','55':'WI','28':'MS','56':'WY','29':'MO'}

    df_cps['State'] = df_cps['State_FIPS']

    df_cps = df_cps.replace({'Person_type':person_mapper, 'Sex':sex_mapper, 'Race':race_mapper, 'Hispanic':hispanic_mapper, 'Marital_status':marital_status_mapper, 'Country_of_birth':country_birth_mapper,'School_completed':school_completed_mapper,'Ever_active_duty':active_duty_mapper,'LF_recode':lf_recode_mapper, 'LF_recode2':lf_recode2_mapper, 'Civilian_LF':civilian_lf_mapper, 'Employed_nonfarm':employed_nonfarm_mapper, 'Recall_return':recall_return_mapper, 'Recall_look':recall_look_mapper,'Job_offered':job_offered_mapper,'Job_offered_week':job_offered_week_mapper, 'Available_ft':available_ft_mapper, 'Job_search':job_search_mapper, 'Look_last_month':look_last_month_mapper, 'Look_last_year':look_last_year_mapper, 'Last_work':last_work_mapper, 'Discouraged':discouraged_mapper, 'Retired':retired_mapper, 'Disabled':disabled_mapper, 'Situation': situation_mapper, 'FT_PT':ft_pt_mapper, 'FT_PT_status':ft_pt_status_mapper, 'Detailed_reason_part_time':detailed_pt_mapper, 'Main_reason_part_time':main_pt_mapper, 'Main_reason_not_full_time':main_not_ft_mapper, 'Have_job':have_job_mapper, 'Want_job':want_job_mapper, 'Want_job_ft':want_job_ft_mapper, 'Want_job_ft_pt':want_job_ft_pt_mapper, 'Want_job_nilf':want_job_nilf_mapper, 'Reason_unemployment':reason_unemployment_mapper, 'Reason_not_looking':reason_not_looking_mapper, 'In_school':in_school_mapper, 'In_school_ft_pt':in_school_ft_pt_mapper, 'School_type':school_type_mapper,'In_school_nilf':in_school_nilf_mapper,'Region':region_mapper,'Division':division_mapper,'State':state_mapper})


    if year != 1995:
        df_cps = df_cps.replace({'Metro_Size':metro_size_mapper})
        df_cps = df_cps.replace({'Metro_Status':metro_status_mapper})
    return df_cps


def turn_int(df_cps):
    df_cps = df_cps.astype({'Month':int, 'Year':int, 'Age':int, 'Hours_per_week':int, 'Hours_per_week_last':int})
    return df_cps


def refine_vars(df_cps):
    #Hispanic, Race and Age group adjustments
    df_cps.loc[(df_cps['Hispanic']=='-1'),'Hispanic']='Not Hispanic'
    df_cps.loc[(df_cps['Hispanic']!='Not Hispanic'),'Hispanic']='Hispanic'

    df_cps.loc[(df_cps['Race'].isin(['White Only','Black Only'])==False),'Race']='Other'

    df_cps['Age_group'] = cut(df_cps['Age'])

    #Calculate full time vs. part time
    df_cps.loc[((df_cps['LF_recode'].isin(['Employed - at work','Employed - absent']))&(df_cps['FT_PT']=='Full Time LF')),'FT_PT']='Full_time'
    df_cps.loc[((df_cps['FT_PT']!='Full_time')&(df_cps['LF_recode'].isin(['Employed - at work','Employed - absent'])))&(((df_cps['Hours_per_week_last']<35)&(df_cps['Hours_per_week_last']!=-1))|(df_cps['FT_PT']=='Part Time LF')|(df_cps['Detailed_reason_part_time']!='-1')|(df_cps['Main_reason_part_time']!='-1')|(df_cps['LF_recode'].isin(['Employed - at work','Employed - absent']))),'FT_PT']='Part_time'
    df_cps.loc[(df_cps['FT_PT']!='Full_time')&(df_cps['FT_PT']!='Part_time'),'FT_PT']='NA'

    #Calculate those in school
    df_cps.loc[(df_cps['Situation']=='In School')|(df_cps['In_school']=='Yes')|(df_cps['In_school_ft_pt']!='-1'),'In_school']='Yes'
    df_cps.loc[df_cps['In_school']!='Yes','In_school']='No'

    #Shows that all in school in situation column only are NILF
    # df_cps[(df_cps['Situation']==3)&(df_cps['In_school']!=1)]['LF_recode'].value_counts()

    #Calculate those in school full time vs part time
    df_cps.loc[(df_cps['In_school_ft_pt']=='Full-time')|((df_cps['Situation']=='In School')&(df_cps['In_school_ft_pt']!='Part-time')),'In_school_ft_pt']='Full-time'
    # df_cps.loc[df_cps['In_school_ft_pt']!='Full_time','In_school_ft_pt']='No'

    df_cps.loc[(df_cps['In_school']=='Yes')&(df_cps['School_type']=='-1')&(df_cps['Age']<18),'School_type']='High School'
    df_cps.loc[(df_cps['In_school']=='Yes')&(df_cps['School_type']=='-1')&(df_cps['Age']>17),'School_type']='College or University'

    #Calculate those disabled
    df_cps.loc[(df_cps['Situation']=='Disabled')|(df_cps['Disabled']=='Yes')|(df_cps['LF_recode']=='Not in labor force - disabled'),'Disabled']='Yes'
    df_cps.loc[df_cps['Disabled']!='Yes','Disabled']='No'

    #Calculate those discouraged
    df_cps.loc[(df_cps['Discouraged']=='Discouraged Worker')|(df_cps['Discouraged']=='Conditionally Interested')|(df_cps['LF_recode2']=='NILF - discouraged'),'Discouraged']='Yes'
    df_cps.loc[df_cps['Discouraged']!='Yes','Discouraged']='No'
    # df_cps.loc[(df_cps['Discouraged']=='Discouraged Worker')|(df_cps['LF_recode2']=='NILF - discouraged'),'Discouraged']='Yes'
    # df_cps.loc[df_cps['Discouraged']!='Yes','Discouraged']='No'


    #Calculate those retired
    df_cps.loc[(df_cps['LF_recode']=='Not in labor force - retired')|(df_cps['Retired']=='Yes')|(df_cps['Situation']=='In Retirement'),'Retired']='Yes'
    df_cps.loc[df_cps['Retired']!='Yes','Retired']='No'

    #Calculate those retired and want work
    df_cps['Retired_want_work'] = np.where((df_cps['Retired']=='Yes')&((df_cps['Want_job']=='Yes, Or Maybe, It Depends')|(df_cps['Want_job_nilf']=='Want A Job')|(df_cps['Want_job_ft_pt']=='Yes')),'Yes',np.where((df_cps['Retired']=='Yes')&((df_cps['Want_job']!='Yes, Or Maybe, It Depends')|(df_cps['Want_job_nilf']!='Want A Job')),'No','NA'))

    df_cps['Want_work'] = np.where((df_cps['Want_job']=='Yes, Or Maybe, It Depends')|(df_cps['Want_job_nilf']=='Want A Job')|(df_cps['Want_job_ft_pt']=='Yes'),'Yes','No')

    #Calculate U3 unemployment
    df_cps['Unemployed_U3'] = np.where(df_cps['LF_recode'].isin(['Unemployed - on layoff','Unemployed - looking']),'Yes','No')

    #Calculate U6 unemployment
    df_cps['Unemployed_U6'] = np.where(((df_cps['LF_recode'].isin(['Unemployed - on layoff','Unemployed - looking']))|(df_cps['Look_last_year']=='Yes')|(df_cps['Look_last_month']=='Yes')|(df_cps['Job_offered_week']=='Yes')|(df_cps['Last_work']=='Within Last 12 Months')|(df_cps['Reason_not_looking'].isin(['Believes No Work Available In Area Of Expertise','Couldnt Find Any Work','Lacks Necessary Schooling/Training']))|(df_cps['Discouraged']=='Yes')|(df_cps['FT_PT_status'].isin(['PT Hrs, Usually Pt For Economic Reasons','PT for Economic Reasons, Usually Ft']))),'Yes','No')

    #Calculate U3 unemployment
    df_cps['Labor_force_U3'] = np.where(df_cps['LF_recode'].isin(['Employed - at work','Employed - absent','Unemployed - on layoff','Unemployed - looking']),'Yes','No')

    #Calculate U6 unemployment
    df_cps['Labor_force_U6'] = np.where((df_cps['LF_recode'].isin(['Employed - at work','Employed - absent','Unemployed - on layoff','Unemployed - looking'])|df_cps['Unemployed_U6']=='Yes'),'Yes','No')

    return df_cps

def weight(df_cps):

    df_cps['Percent'+str(year)]=df_cps['POPESTIMATE'+str(year)]/df_state['POPESTIMATE'+str(year)].sum()

    df_cps['Count'] = np.arange(1,len(df_cps)+1)

    weight_mapper = df_cps.groupby(by=['Age_group','Sex','Race','Hispanic','State_FIPS'])['Count'].count().reset_index().rename(columns={'Count':'Ratio'})

    weight_mapper['Ratio'] = weight_mapper['Ratio']/len(df_cps)

    df_cps = df_cps.merge(weight_mapper,on=['Sex','Race','Age_group','Hispanic','State_FIPS'],how='left',left_index=True)

    df_cps['Weight'] = df_cps['Percent'+str(year)]/df_cps['Ratio']

    df_cps.drop(['Count','Ratio','Percent'+str(year)],axis=1,inplace=True)

    #Try adjusting weights so they add up to length of df_cps
    #Doesn't seem to make much of a difference either way
    # df_cps['Weight'] = df_cps['Weight']*(len(df_cps)/df_cps['Weight'].sum())

    return df_cps


if __name__=='__main__':
    fp = '/media/dhense/Elements/PublicData/cps_files/'
    pickle_path = '/home/dhense/PublicData/Economic_analysis/intermediate_files/'
    # export_path = '/home/dhense/PublicData/Economic_analysis/intermediate_files/cps/'
    export_path = '/home/dhense/Documents/'
    state_pop_pickle = 'state_pop.pickle'

    print("...loading pickle")
    tmp = open(pickle_path+state_pop_pickle,'rb')
    df_state = pickle.load(tmp)
    tmp.close()

    df_state['POPESTIMATE2019'] = df_state['POPESTIMATE2018']
    df_state['POPESTIMATE2020'] = df_state['POPESTIMATE2018']

    # months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    months = ['jan']
    months_dict = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}

    schema = StructType([
        StructField('HHID', StringType(), True),StructField('Age', IntegerType(), True),StructField('Sex', StringType(), True),StructField('Race', StringType(), True),StructField('Hispanic', StringType(), True),StructField('Marital_status', StringType(), True),StructField('Country_of_birth', StringType(), True),StructField('School_completed', StringType(), True),StructField('Ever_active_duty', StringType(), True),StructField('LF_recode', StringType(), True),StructField('LF_recode2', StringType(), True),StructField('Civilian_LF', StringType(), True),StructField('Employed_nonfarm', StringType(), True),StructField('Have_job', StringType(), True),StructField('Unpaid_family_work', StringType(), True),StructField('Recall_return', StringType(), True),StructField('Recall_look', StringType(), True),
        StructField('Job_offered', StringType(), True),StructField('Job_offered_week', StringType(), True),StructField('Available_ft', StringType(), True),StructField('Job_search', StringType(), True),StructField('Look_last_month', StringType(), True),StructField('Look_last_year', StringType(), True),StructField('Last_work', StringType(), True),StructField('Discouraged', StringType(), True),StructField('Retired', StringType(), True),StructField('Disabled', StringType(), True),StructField('Situation', StringType(), True),StructField('FT_PT', StringType(), True),StructField('FT_PT_status', StringType(), True),StructField('Detailed_reason_part_time', StringType(), True),StructField('Main_reason_part_time', StringType(), True),StructField('Main_reason_not_full_time', StringType(), True),StructField('Want_job', StringType(), True),StructField('Want_job_ft', StringType(), True),StructField('Want_job_ft_pt', StringType(), True),StructField('Want_job_nilf', StringType(), True),StructField('Reason_unemployment', StringType(), True),StructField('Reason_not_looking', StringType(), True),StructField('Hours_per_week', StringType(), True),StructField('Hours_per_week_last', StringType(), True),StructField('In_school', StringType(), True),StructField('In_school_ft_pt', StringType(), True),StructField('School_type', StringType(), True),StructField('In_school_nilf', StringType(), True),StructField('State_FIPS', StringType(), True),StructField('County_FIPS', StringType(), True),StructField('Metro_Code', StringType(), True),StructField('Metro_Size', StringType(), True),StructField('Metro_Status', StringType(), True),StructField('Region', StringType(), True),StructField('Division', StringType(), True),StructField('Year', StringType(), True),StructField('Month', StringType(), True),StructField('FIPS', StringType(), True),StructField('PID', StringType(), True),StructField('SID', StringType(), True),StructField('Age_group', StringType(), True),StructField('Retired_want_work', StringType(), True),StructField('Want_work', StringType(), True),StructField('POPESTIMATE2000', StringType(), True),StructField('Weight', StringType(), True)
    ])

    for year in range(1999,2000):
        # df_year = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)
        #df_year = pd.DataFrame()
        for m in months:
            tic = time.perf_counter()
            if year==2020 and m=='sep':
                break
            df_cps = pd.read_csv(fp+str(year)+'/'+m+str(year)[-2:]+'pub.dat')

            df_cps = extract_record(df_cps,str(year),str(months_dict[m]))

            #Removes rows where there are duplicate PIDs (very few in each file)
            df_cps.drop_duplicates(subset='PID',keep=False, inplace=True)

            df_cps = var_mapper(df_cps)

            df_cps = df_cps[(df_cps['Person_type']=='Adult Civilian')]
            df_cps = turn_int(df_cps)
            df_cps = df_cps[df_cps.Age>15]
            df_cps = refine_vars(df_cps)

            df_cps = df_cps.merge(df_state[['Sex','Race','Age_group','Hispanic','State_FIPS','POPESTIMATE'+str(year)]],on=['Sex','Race','Age_group','Hispanic','State_FIPS'],how='left',left_index=True)
            '''
            df_cps = weight(df_cps)

            #Drop unnecessary columns
            df_cps = df_cps.drop(columns=['Ref_person','Person_line_num','Person_type','Interview_Status']).reset_index(drop=True)
            
            # print("...saving pickle")
            # tmp = open(export_path+'cps_'+m+str(year)+'.pickle','wb')
            # pickle.dump(df_cps,tmp)
            # tmp.close()

            toc = time.perf_counter()
            print(f"Calculations took {toc - tic:0.1f} seconds")
            '''

            # df_year = df_year.append(df_cps)
            # df = spark.createDataFrame(df_cps)
            # df_year = df_year.union(df)

        # df_year = df_year.reset_index(drop=True)
              
        # print("...saving pickle")
        # tmp = open(pickle_path+'cps_'+str(year)+'.pickle','wb')
        # pickle.dump(df_year,tmp)
        # tmp.close()

        




        #Notes
        #Instead/in addition to part time counting as percentage of full time job depending on how many hours worked:
        #Get salary data from those working to calculate percentage of living wage, and use that in unemployment calc

