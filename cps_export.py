import os, pickle, time
import pandas as pd
import numpy as np




def extract_record(df,yr,m):
    '''
        Preprocessing:
            - Rows begin as single string of numerical values
            - Breaks them up into unique fields and inserts those values into Pandas DataFrame columns 
    '''
    col_dict = {'HHID':(0,15),'Ref_person':(117,119),'Person_line_num':(146,148),'Person_type':(160,162),'Age':(121,123),'Sex':(128,130),'Race':(138,140),'Hispanic':(140,142),'Marital_status':(124,126),'Country_of_birth':(162,165),'School_completed':(136,138),'Ever_active_duty':(130,132),'LF_recode':(179,181),'LF_recode2':(392,394),'Civilian_LF':(386,388),'Employed_nonfarm':(479,481),'Have_job':(205,207),'Unpaid_family_work':(183,185),'Recall_return':(276,278),'Recall_look':(280,282),'Job_offered':(331,333),'Job_offered_week':(358,360),'Available_ft':(249,251),'Job_search':(400,402),'Look_last_month':(293,295),'Look_last_year':(350,352),'Last_work':(564,566),'Discouraged':(388,390),'Retired':(566,568),'Disabled':(203,205),'Situation':(568,570),'FT_PT':(396,398),'FT_PT_status':(415,417),'Detailed_reason_part_time':(404,406),'Main_reason_part_time':(228,230),'Main_reason_not_full_time':(230,232),'Want_job':(346,348),'Want_job_ft':(226,228),'Want_job_ft_pt':(199,201),'Want_job_nilf':(417,419),'Reason_unemployment':(411,413),'Reason_not_looking':(348,350),'Hours_per_week':(217,219),'Hours_per_week_last':(242,244),'In_school':(574,576),'In_school_ft_pt':(576,578),'School_type':(578,580),'In_school_nilf':(580,582),'State_FIPS':(92,94),'County_FIPS':(100,103),'Metro_Code':(94,100),'Metro_Size':(106,107),'Metro_Status':(104,105),'Region':(88,90),'Division':(90,91)}

    df_p = pd.DataFrame()

    #Deals with index differences between files in different years for certain variables
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

    df_p = df_p.drop(columns=['Ref_person','Person_line_num']).reset_index(drop=True)

    return df_p

if __name__=='__main__':
    #Where raw files (.dat) are stored on local computer/external drive
    fp = '/media/dhense/Elements/PublicData/cps_files/'

    #Where you would like to save preprocessed csv's on local computer/external drive
    export_path = '/media/dhense/Elements/PublicData/cps_csv_files/'

    months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    # months = ['jan']
    months_dict = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}


    tic = time.perf_counter()
    for year in range(1995,2021):
        for m in months:
            if year==2020 and m=='sep':
                break
            df_cps = pd.read_csv(fp+str(year)+'/'+m+str(year)[-2:]+'pub.dat')
            df_cps = extract_record(df_cps,str(year),str(months_dict[m]))
            df_cps.drop_duplicates(subset='PID',keep=False, inplace=True)
            df_cps.to_csv(export_path+'cps_'+m+str(year)+'.csv',index=False)
    toc = time.perf_counter()
    print(f"Exports took {toc - tic:0.1f} seconds") 