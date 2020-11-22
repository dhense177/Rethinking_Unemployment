import os, pickle, time
import pandas as pd
import numpy as np




def extract_record(df,yr,m,df_wage,df_wage_state):
    '''
        Preprocessing:
            - Rows begin as single string of numerical values
            - Breaks them up into unique fields and inserts those values into Pandas DataFrame columns 
    '''
    col_dict = {'HHID':(0,15),'HHID2':(70,75),'Ref_person':(117,119),'Person_line_num':(146,148),'Person_type':(160,162),'Age':(121,123),'Sex':(128,130),'Race':(138,140),'Hispanic':(140,142),'Marital_status':(124,126),'Country_of_birth':(162,165),'Mother_Country_of_birth':(165,168),'Type_of_mother':(891,893),'Father_Country_of_birth':(168,171),'Type_of_father':(889,891),'Citizenship_Status':(171,173),'Immigrant_Year_Entry':(175,177),'When_Serve':(875,877),'High_School_Diploma':(821,823),'Highest_Grade_GED':(823,825),'School_completed':(136,138),'Ever_active_duty':(130,132),'LF_recode':(179,181),'LF_recode2':(392,394),'Civilian_LF':(386,388),'Employed_nonfarm':(479,481),'Have_job':(205,207),'Unpaid_family_work':(183,185),'Recall_return':(276,278),'Recall_look':(280,282),'Job_offered':(331,333),'Job_offered_week':(358,360),'Available_ft':(249,251),'Job_search':(400,402),'Look_last_month':(293,295),'Look_last_year':(350,352),'Last_work':(564,566),'Discouraged':(388,390),'Retired':(566,568),'Disabled':(203,205),'Situation':(568,570),'FT_PT':(396,398),'FT_PT_status':(415,417),'Detailed_reason_part_time':(404,406),'Main_reason_part_time':(228,230),'Main_reason_not_full_time':(230,232),'Want_job':(346,348),'Want_job_ft':(226,228),'Want_job_ft_pt':(199,201),'Want_job_nilf':(417,419),'Reason_unemployment':(411,413),'Reason_not_looking':(348,350),'Hours_per_week':(223,226),'Hours_per_week_last':(242,244),'In_school':(574,576),'In_school_ft_pt':(576,578),'School_type':(578,580),'In_school_nilf':(580,582),'State_FIPS':(92,94),'County_FIPS':(100,103),'Metro_Code_Old':(96,100),'Metro_Size':(106,107),'Metro_Status':(104,105),'Region':(88,90),'Division':(90,91),'Family_Income':(38,40),'Household_Members':(58,60),'Family_Recode':(152,154),'Periodicity':(501,503),'Hourly_Status':(505,507),'Usual_Hours':(524,526),'Hourly_Pay_Main':(511,515),'Hourly_Rate_Out':(515,519),'Hourly_Rate_Recode':(519,523),'Weekly_Earnings':(526,534),'Overtime_Weekly':(539,547),'Overtime_Weekly2':(547,555),'Weeks_Paid':(558,560)}

    df_p = pd.DataFrame()

    #Deals with index differences between files in different years for certain variables
    for k,v in col_dict.items():
        if ((int(yr) < 2003)&(k=='Hispanic')):
            df_p[k] = [i[0][156:158] for i in df.values]
        elif ((int(yr)==1995)&((k=='County_FIPS')|(k=='Metro_Code')|(k=='Metro_Size'))):
            continue
        else:
            df_p[k] = [i[0][v[0]:v[1]] for i in df.values]

    df_p['Year'] = yr
    df_p['Month'] = m
    df_p['State_FIPS'] = df_p['State_FIPS'].astype(str).str.strip().apply(lambda x: str(x).zfill(2) if x != '' else '')

    if int(yr) != 1995:
        df_p['County_FIPS'] = df_p['County_FIPS'].astype(str).str.strip().apply(lambda x: str(x).zfill(3) if x != '' else '')
        df_p['FIPS'] = df_p['State_FIPS']+df_p['County_FIPS']

    #Calculate unique Person ID (PID)
    df_p['PID'] = df_p['HHID']+df_p['Ref_person'].str.strip().apply(lambda x: str(x).zfill(2))+df_p['Person_line_num'].str.strip().apply(lambda x: str(x).zfill(2))+df_p['Sex'].str[1]+df_p['Race'].str[1]+df_p['Hispanic'].str[1]+df_p['Country_of_birth'].str.strip().apply(lambda x: str(x).zfill(3))

    #Calculate unique Survey ID (SID)
    df_p['SID'] = df_p['PID']+df_p['Year']+df_p['Month'].str.zfill(2)

    ######################################################################################################################################

    #Remove Armed Forces Members from DF
    df_p = df_p[df_p['Person_type']!=' 3']

    df_p['Family_Income'] = df_p['Family_Income'].astype(int)

    #Map new Metro Codes to Old ones using metro mapper file
    metro_mapper = pd.read_excel('/home/dhense/PublicData/Economic_analysis/Unemployment/metro_mapper.xlsx',converters={'C/MSA_1999_Code':str})

    metro_mapper = metro_mapper[['CBSA_2003_Code','C/MSA_1999_Code']].rename(columns={'CBSA_2003_Code':'Metro_Code','C/MSA_1999_Code':'Metro_Code_Old'})

    df_p = df_p.merge(metro_mapper,on='Metro_Code_Old',how='left')
    df_p['Metro_Code'] = df_p['Metro_Code'].fillna(0)
    df_p['Metro_Code'] = df_p['Metro_Code'].astype(int)

    df_p['Hourly_Rate_Recode'] = df_p['Hourly_Rate_Recode'].astype(int)
    df_p['Usual_Hours'] = df_p['Usual_Hours'].astype(int)
    df_p['Weekly_Earnings'] = df_p['Weekly_Earnings'].astype(int)
    df_p['Hours_per_week']=df_p['Hours_per_week'].astype(int)
    # df_p['Employed'] = np.where(df_p['LF_recode'].isin([' 1',' 2']),1,0)
    
    #Calculate number of dependent children per Household (children who are not employed)
    children = pd.DataFrame(df_p[(df_p['Family_Recode']==' 3')&(df_p['LF_recode'].isin([' 1',' 2'])==False)].groupby('HHID')['SID'].count()).reset_index().rename(columns={'SID':'Num_Children_Dependent'})
    df_p = df_p.merge(children,on='HHID',how='left').fillna(0)
    df_p['Num_Children_Dependent'] = df_p['Num_Children_Dependent'].astype(int)

    #Calculate number of adults per Household
    adults = pd.DataFrame(df_p[df_p['Family_Recode'].isin([' 1',' 2'])].groupby('HHID')['SID'].count()).reset_index().rename(columns={'SID':'Num_Family_Adults'})
    df_p = df_p.merge(adults,on='HHID',how='left').fillna(0)
    df_p['Num_Family_Adults'] = df_p['Num_Family_Adults'].astype(int)
    df_p.loc[(df_p['Num_Family_Adults']==0)&(df_p['Person_type']==' 2'),'Num_Family_Adults']=1

    #Calculate number of working family members per Household (only reference person and spouse, not children)
    df_p['Employed'] = np.where(df_p['LF_recode'].isin([' 1',' 2']),1,0)
    working_fam = pd.DataFrame(df_p[df_p['Family_Recode'].isin([' 1',' 2',' 3'])].groupby('HHID')['Employed'].sum()).reset_index().rename(columns={'Employed':'Num_Working_Family_Members'})
    df_p = df_p.merge(working_fam,on='HHID',how='left').fillna(0)
    df_p['Num_Working_Family_Members'] = df_p['Num_Working_Family_Members'].astype(int)

    #Calculate number of working individuals in family(not family members and other relatives within a household not included)
    df_p['Working_Individual'] = np.where((df_p['Family_Recode']==' 0')&(df_p['LF_recode'].isin([' 1',' 2'])),1,0)
    working_individual_hh = pd.DataFrame(df_p.groupby('HHID')['Working_Individual'].sum()).reset_index().rename(columns={'Working_Individual':'Num_Working_Individual'})
    df_p = df_p.merge(working_individual_hh,on='HHID',how='left')

    #########################################################################################################################


    df_p['LivingWage_Category'] = np.where((df_p['Num_Family_Adults']==1)&(df_p['Num_Working_Family_Members']==1)&(df_p['Num_Children_Dependent']==0),'1Adult-W',np.where((df_p['Num_Family_Adults']==1)&(df_p['Num_Working_Family_Members']==1)&(df_p['Num_Children_Dependent']==1),'1Adult_1Child-W',np.where((df_p['Num_Family_Adults']==1)&(df_p['Num_Working_Family_Members']==1)&(df_p['Num_Children_Dependent']==2),'1Adult_2Child-W',np.where((df_p['Num_Family_Adults']==1)&(df_p['Num_Working_Family_Members']==1)&(df_p['Num_Children_Dependent']>=3),'1Adult_3Child-W',np.where((df_p['Num_Family_Adults']>=2)&(df_p['Num_Working_Family_Members']==1)&(df_p['Num_Children_Dependent']==0),'2Adult1W-W',np.where((df_p['Num_Family_Adults']>=2)&(df_p['Num_Working_Family_Members']==1)&(df_p['Num_Children_Dependent']==1),'2Adult1W_1Child-W',np.where((df_p['Num_Family_Adults']>=2)&(df_p['Num_Working_Family_Members']==1)&(df_p['Num_Children_Dependent']==2),'2Adult1W_2Child-W',np.where((df_p['Num_Family_Adults']>=2)&(df_p['Num_Working_Family_Members']==1)&(df_p['Num_Children_Dependent']>=3),'2Adult1W_3Child-W',np.where((df_p['Num_Family_Adults']>=2)&(df_p['Num_Working_Family_Members']>=2)&(df_p['Num_Children_Dependent']==0),'2Adult-W',np.where((df_p['Num_Family_Adults']>=2)&(df_p['Num_Working_Family_Members']>=2)&(df_p['Num_Children_Dependent']==1),'2Adult_1Child-W',np.where((df_p['Num_Family_Adults']>=2)&(df_p['Num_Working_Family_Members']>=2)&(df_p['Num_Children_Dependent']==2),'2Adult_2Child-W',np.where((df_p['Num_Family_Adults']>=2)&(df_p['Num_Working_Family_Members']>=2)&(df_p['Num_Children_Dependent']>=3),'2Adult_3Child-W',np.where(df_p['Employed']==1,'1Adult-W','NA')))))))))))))

    #########################################################################################################################


    df_p = df_p.merge(df_wage,on=['Metro_Code','Year','LivingWage_Category'],how='left')

    df_p.loc[(df_p['LivingWage_Category']!='NA')&(df_p['LivingWage'].isnull()),'LivingWage'] = df_p.merge(df_wage_state,on=['State_FIPS','Year','LivingWage_Category'],how='left')[(df_p['LivingWage_Category']!='NA')&(df_p['LivingWage'].isnull())]['LivingWage_y']
    
    #Next:
    #Extrapolate %hourly wages below living wage level to all employed w/in data?
    
    #Divide Weekly Earnings by hours per week to get hourly earnings. Extrapolate to rest of employed thats missing data

    df_comp = pd.DataFrame()
    df_comp['Weekly_Earnings'] = df_p[(df_p['Weekly_Earnings'].isin([-1,0])==False)&(df_p['Employed']==1)&(df_p['Hours_per_week'].isin([-1,-4])==False)&(df_p['LivingWage'].isnull()==False)]['Weekly_Earnings']/100
    df_comp['Hours_per_week'] = df_p[(df_p['Weekly_Earnings'].isin([-1,0])==False)&(df_p['Employed']==1)&(df_p['Hours_per_week'].isin([-1,-4])==False)&(df_p['LivingWage'].isnull()==False)]['Hours_per_week']
    df_comp['Hourly_Earnings'] = df_comp['Weekly_Earnings']/df_comp['Hours_per_week']
    df_comp['LivingWage'] = df_p[(df_p['Weekly_Earnings'].isin([-1,0])==False)&(df_p['Employed']==1)&(df_p['Hours_per_week'].isin([-1,-4])==False)&(df_p['LivingWage'].isnull()==False)]['LivingWage']
    df_comp['Wage_Ratio'] = (df_comp['LivingWage']-df_comp['Hourly_Earnings'])/df_comp['LivingWage']

    df_p = df_p.merge(df_comp[['Hourly_Earnings','Wage_Ratio']],how='left',left_index=True,right_index=True)

    '''
    #Partial wage unemployment
    comp_unemployment = (1-(df_comp[df_comp['Hourly_Earnings']<df_comp['LivingWage']]['Hourly_Earnings']/df_comp[df_comp['Hourly_Earnings']<df_comp['LivingWage']]['LivingWage'])).values.sum()
    # comp2_unemployment = (1-(df_comp2[df_comp2['Hourly_Earnings']<df_comp2['1Adult-W']]['Hourly_Earnings']/df_comp2['1Adult-W'])).values.sum()

    #Scaled to full employed list
    wage_unemployed = (comp_unemployment/len(df_comp))*len(df_p[df_p['Employed']==1])

    u3_unemployed = len(df_p[df_p['LF_recode'].isin([' 3',' 4'])])

    u3_employed = len(df_p[df_p['Employed']==1])

    #Almost double the u3 rate
    wage_adjusted_rate = (u3_unemployed+wage_unemployed)/(u3_unemployed+u3_employed)
    u3rate = (u3_unemployed)/(u3_unemployed+u3_employed)
    '''
    ######################################################################################################################################

    df_p = df_p.drop(columns=['Ref_person','Person_line_num','HHID2','Mother_Country_of_birth','Type_of_mother','Father_Country_of_birth','Type_of_father','Citizenship_Status','Immigrant_Year_Entry','When_Serve','High_School_Diploma','Highest_Grade_GED','Family_Income','Household_Members','Family_Recode','Periodicity','Hourly_Status','Usual_Hours','Hourly_Pay_Main','Hourly_Rate_Out','Hourly_Rate_Recode','Weekly_Earnings','Overtime_Weekly','Overtime_Weekly2','Weeks_Paid','Num_Children_Dependent','Num_Family_Adults','Employed','Num_Working_Family_Members','Working_Individual','Num_Working_Individual']).reset_index(drop=True)

    return df_p



def livingwage(df_lw,col):
    df_lw = df_lw[['Year',col,'1Adult-W', '1Adult_1Child-W','1Adult_2Child-W', '1Adult_3Child-W', '2Adult1W-W', '2Adult1W_1Child-W','2Adult1W_2Child-W', '2Adult1W_3Child-W', '2Adult-W', '2Adult_1Child-W','2Adult_2Child-W', '2Adult_3Child-W']]

    if col == 'Metro_Code':
        #Get metro codes where 1 years of data is missing
        fours = df_lw['Metro_Code'].value_counts()[df_lw['Metro_Code'].value_counts()==4].index

        for f in fours:
            four = df_lw[df_lw['Metro_Code']==f]
            years = four['Year'].values
            missing = set([2016,2017,2018,2019,2020])-set(years)
            missing = missing.pop()
            year_low = missing-1
            year_hi = missing+1
            row = (four[four['Year']==year_low].values+four[four['Year']==year_hi].values)/2
            df_lw.loc[len(df_lw)] = row[0]
        
        df_lw = df_lw.astype({'Year':int, 'Metro_Code':int})

        #keep only metro codes with 4 or 5 values between 20016 and 2020
        metro_list = list(df_lw['Metro_Code'].value_counts()[df_lw['Metro_Code'].value_counts()==5].index)
        df_lw = df_lw[df_lw['Metro_Code'].isin(metro_list)]


    df_lw = df_lw.sort_values('Year').reset_index(drop=True)
    df_lw = pd.melt(df_lw,id_vars=['Year', col],value_vars=['1Adult-W', '1Adult_1Child-W', '1Adult_2Child-W','1Adult_3Child-W', '2Adult1W-W', '2Adult1W_1Child-W','2Adult1W_2Child-W', '2Adult1W_3Child-W', '2Adult-W', '2Adult_1Child-W','2Adult_2Child-W', '2Adult_3Child-W']).rename(columns={'variable':'LivingWage_Category','value':'LivingWage'})

    #Get average yearly growth of wage variables per metro code
    df_wage = df_lw.copy()
    df_wage['Yearly_Growth'] = df_wage.groupby([col,'LivingWage_Category'])['LivingWage'].pct_change()
    df_growth = pd.DataFrame(df_wage.groupby([col,'LivingWage_Category'])['Yearly_Growth'].mean()).reset_index()

    cats = list(df_wage['LivingWage_Category'].unique())

    #Setting 2016 Growth rate values as average of 2017,2018,2019 and 2020 gorwth rates
    for cat in cats:
        df_wage.loc[(df_wage.Year==2016)&(df_wage['LivingWage_Category']==cat),'Yearly_Growth'] = df_wage[(df_wage.Year==2016)&(df_wage['LivingWage_Category']==cat)].merge(df_growth,on=[col,'LivingWage_Category'])['Yearly_Growth_y'].values

    #Setting 1995-2015 living wage values
    for year in range(2015,1994,-1):
        for cat in cats:
            df_new = df_wage[(df_wage.Year==(year+1))&(df_wage['LivingWage_Category']==cat)][[col,'Yearly_Growth']]
            df_new['Year'] = year
            df_new['LivingWage_Category'] = cat
            df_new['LivingWage'] = df_wage[(df_wage.Year==(year+1))&(df_wage['LivingWage_Category']==cat)]['LivingWage']/(1+df_wage[(df_wage.Year==(year+1))&(df_wage['LivingWage_Category']==cat)]['Yearly_Growth'])
            df_wage = df_wage.append(df_new).sort_values('Year').reset_index(drop=True)    

    df_wage['Year'] = df_wage['Year'].astype(str)
    df_wage = df_wage[['Year',col,'LivingWage_Category','LivingWage']]

    # df_p = df_p.merge(df_wage,on=[col,'Year','LivingWage_Category'],how='left')

    return df_wage

if __name__=='__main__':
    #Where raw files (.dat) are stored on local computer/external drive
    fp = '/media/dhense/Elements/PublicData/cps_files/'
    # fp = '/home/dhense/PublicData/cps_files/'

    #Where you would like to save preprocessed csv's on local computer/external drive
    export_path = '/media/dhense/Elements/PublicData/cps_csv_files/'
    # export_path = '/home/dhense/PublicData/cps_files/'

    #Living Wage data
    df_lw = pd.read_csv('/home/dhense/PublicData/Economic_analysis/intermediate_files/livingwagemetro.csv')
    df_lw_state = pd.read_csv('/home/dhense/PublicData/Economic_analysis/intermediate_files/livingwage.csv',converters={'State_FIPS': lambda x: str(x).zfill(2)})

    df_wage = livingwage(df_lw,'Metro_Code')
    df_wage_state = livingwage(df_lw_state,'State_FIPS')

    months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    # months = ['jan']
    months_dict = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}


    tic = time.perf_counter()
    for year in range(1999,2021):
        for m in months:
            if year==2020 and m=='sep':
                break
            df_p = pd.read_csv(fp+str(year)+'/'+m+str(year)[-2:]+'pub.dat')
            df_p = extract_record(df_p,str(year),str(months_dict[m]),df_wage,df_wage_state)
            df_p.drop_duplicates(subset='PID',keep=False, inplace=True)
            df_p.to_csv(export_path+'cps_'+m+str(year)+'.csv',index=False)
    toc = time.perf_counter()
    print(f"Exports took {toc - tic:0.1f} seconds")