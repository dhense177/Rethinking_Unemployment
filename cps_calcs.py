import os, pickle, time, math
import pandas as pd
import numpy as np
from cps_parser import refine_df_state



def assign_groups(df):
    df['Unemployed_U3'] = np.where((df['LF_recode'].isin(['Unemployed - on layoff','Unemployed - looking'])),'Yes','No')

    df['Unemployed_U6'] = np.where(((df['LF_recode'].isin(['Unemployed - on layoff','Unemployed - looking']))|(df['Look_last_year']=='Yes')|(df['Look_last_month']=='Yes')|(df['Job_offered_week']=='Yes')|(df['Last_work']=='Within Last 12 Months')|(df['Reason_not_looking'].isin(['Believes No Work Available In Area Of Expertise','Couldnt Find Any Work','Lacks Necessary Schooling/Training']))|(df['Discouraged']=='Yes')|(df['FT_PT_status'].isin(['PT Hrs, Usually Pt For Economic Reasons','PT for Economic Reasons, Usually Ft']))),'Yes','No')

    df['Labor_Force_U3'] = np.where((df['LF_recode'].isin(['Employed - at work','Employed - absent','Unemployed - on layoff','Unemployed - looking'])),'Yes','No')

    df['Labor_Force_U6'] = np.where(((df['LF_recode'].isin(['Employed - at work','Employed - absent','Unemployed - on layoff','Unemployed - looking']))|(df['Unemployed_U6']=='Yes')),'Yes','No')

    return df

def calc_rates(df):
    U3Rate = df[df['Unemployed_U3']=='Yes']['Weight'].sum()/df[df['Labor_Force_U3']=='Yes']['Weight'].sum()
    U6Rate = df[df['Unemployed_U6']=='Yes']['Weight'].sum()/df[df['Labor_Force_U6']=='Yes']['Weight'].sum()

    U3LFPR = df[df['Labor_Force_U3']=='Yes']['Weight'].sum()/df['Weight'].sum()
    U6LFPR = df[df['Labor_Force_U6']=='Yes']['Weight'].sum()/df['Weight'].sum()

    ################################# #Self rate calc #####################################################

    #Wage unemployment
    weight_ratio = df[df['Wage_Ratio']>0]['Weight'].sum()/len(df[df['Wage_Ratio']>0])
    wagerate = (df[df['Wage_Ratio']>0]['Wage_Ratio'].sum()*weight_ratio)/len(df[df['Wage_Ratio'].isnull()==False])

    #Part time unemployment
    reasons_pt = df[df['Main_reason_part_time'].isin(['Slack Work/Business Conditions', 'Could Only Find Part-time Work', 'Seasonal Work', 'Child Care Problems', 'Health/Medical Limitations', 'Full-Time Workweek Is Less Than 35 Hrs','Retired/Social Security Limit On Earnings'])]['Weight'].sum()

    avg_pt_hrs = df[df['Main_reason_part_time'].isin(['Slack Work/Business Conditions', 'Could Only Find Part-time Work', 'Seasonal Work', 'Child Care Problems', 'Health/Medical Limitations', 'Full-Time Workweek Is Less Than 35 Hrs','Retired/Social Security Limit On Earnings'])]['Hours_per_week_last'].mean()

    pt_timers = df[df['FT_PT']=='Part_time']['Weight'].sum()

    ptrate = (reasons_pt/pt_timers)*((35-avg_pt_hrs)/35)


    # Unemployed main
    unemployed_self = df[(df['LF_recode'].isin(['Employed - at work','Employed - absent'])==False)&((df['LF_recode'].isin(['Unemployed - on layoff','Unemployed - looking']))|(df['Look_last_year']=='Yes')|(df['Look_last_month']=='Yes')|(df['Job_offered_week']=='Yes')|(df['Last_work']=='Within Last 12 Months')|(df['Reason_not_looking'].isin(['Believes No Work Available In Area Of Expertise','Couldnt Find Any Work','Lacks Necessary Schooling/Training','Employers Think Too Young Or Too Old','Other Types Of Discrimination','Cant Arrange Child Care']))|(df['Discouraged']=='Yes')|(df['Disabled']=='Yes'))]['Weight'].sum()

    #Get rid of 71.4% of disabled who dont meet other criteria given that 28.6% of disabled are due to work-related matters
    only_disabled = df[(df['Disabled']=='Yes')&((df['LF_recode'].isin(['Employed - at work','Employed - absent'])==False)&(df['LF_recode'].isin(['Unemployed - on layoff','Unemployed - looking'])==False)&(df['Look_last_year']!='Yes')&(df['Look_last_month']!='Yes')&(df['Job_offered_week']!='Yes')&(df['Last_work']!='Within Last 12 Months')&(df['Reason_not_looking'].isin(['Believes No Work Available In Area Of Expertise','Couldnt Find Any Work','Lacks Necessary Schooling/Training','Employers Think Too Young Or Too Old','Other Types Of Discrimination','Cant Arrange Child Care'])==False)&(df['Discouraged']!='Yes'))]['Weight'].sum()

    unemployed_self = unemployed_self-(.714*only_disabled)
    employed_self = df[df['LF_recode'].isin(['Employed - at work','Employed - absent'])]['Weight'].sum()
    unemployment_self = unemployed_self/(unemployed_self+employed_self)

    #Final calc for self_calc rate
    SelfRate = wagerate+ptrate+unemployment_self

    return U3Rate, U6Rate, U3LFPR, U6LFPR, SelfRate, wagerate, ptrate


def calc_percentages(df_cps, rate):
    ##### Total #####
    if rate=='U3':
        var = 'Labor_Force_U3'
    else:
        var = 'Labor_Force_U6'

    nilf = df_cps[df_cps[var]=='No']   
    
    perc_nilf = nilf['Weight'].sum()/len(df_cps)
    perc_retired_nilf = nilf[nilf['Retired']=='Yes']['Weight'].sum()/len(df_cps)
    perc_disabled_nilf = nilf[nilf['Disabled']=='Yes']['Weight'].sum()/len(df_cps)
    perc_student_nilf = nilf[nilf['In_school']=='Yes']['Weight'].sum()/len(df_cps)
    perc_other_nilf = nilf[(nilf['Retired']!='Yes')&(nilf['Disabled']!='Yes')&(nilf['In_school']!='Yes')]['Weight'].sum()/len(df_cps)

    perc_looking = df_cps[df_cps['LF_recode']=='Unemployed - looking']['Weight'].sum()/len(df_cps)
    perc_layoff = df_cps[df_cps['LF_recode']=='Unemployed - on layoff']['Weight'].sum()/len(df_cps)
    perc_unemployed = perc_looking+perc_layoff

    perc_employed = df_cps[df_cps['LF_recode'].isin(['Employed - at work','Employed - absent'])]['Weight'].sum()/len(df_cps)
    perc_employed_ft = df_cps[df_cps['FT_PT']=='Full_time']['Weight'].sum()/len(df_cps)
    perc_employed_pt = df_cps[df_cps['FT_PT']=='Part_time']['Weight'].sum()/len(df_cps)


    ##### Labor Force #######
    lf = df_cps[df_cps[var]=='Yes']['Weight'].sum()

    looking_lf = df_cps[df_cps['LF_recode']=='Unemployed - looking']['Weight'].sum()/lf
    layoff_lf = df_cps[df_cps['LF_recode']=='Unemployed - on layoff']['Weight'].sum()/lf
    unemployed_lf = looking_lf+layoff_lf

    employed_ft_lf = df_cps[df_cps['FT_PT']=='Full_time']['Weight'].sum()/lf
    employed_pt_lf = df_cps[df_cps['FT_PT']=='Part_time']['Weight'].sum()/lf
    employed_lf = employed_ft_lf+employed_pt_lf

    employed_pt_econ_lf = df_cps[(df_cps['FT_PT']=='Part_time')&(df_cps['FT_PT_status'].isin(['PT Hrs, Usually Pt For Economic Reasons','FT_Hours, Usually PT For Economic Reasons']))]['Weight'].sum()/lf

    employed_pt_nonecon_lf = df_cps[(df_cps['FT_PT']=='Part_time')&(df_cps['FT_PT_status'].isin(['PT Hrs, Usually Pt For Economic Reasons','FT_Hours, Usually PT For Economic Reasons'])==False)]['Weight'].sum()/lf


    return [perc_nilf,perc_retired_nilf,perc_disabled_nilf,perc_student_nilf,perc_other_nilf,perc_unemployed,perc_looking,perc_layoff,perc_employed,perc_employed_ft,perc_employed_pt,looking_lf,layoff_lf,unemployed_lf,employed_ft_lf,employed_pt_lf,employed_lf,employed_pt_econ_lf,employed_pt_nonecon_lf]




if __name__=='__main__':
    import_path = '/home/dhense/PublicData/Economic_analysis/intermediate_files/cps_csv/'
    export_path = '/home/dhense/PublicData/Economic_analysis/'


    state_pop_pickle = 'state_pop.pickle'
    pickle_path = '/home/dhense/PublicData/Economic_analysis/intermediate_files/'

    print("...loading pickle")
    tmp = open(pickle_path+state_pop_pickle,'rb')
    df_state = pickle.load(tmp)
    tmp.close()


    df_state = refine_df_state(df_state)

    
    # months = ['jan']
    months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']

    df_data = pd.DataFrame()
    df_self = pd.DataFrame()
    df_percentages = pd.DataFrame()

    tic = time.perf_counter()
    for year in range(1999,2021):
        for m in months:
            if year==2020 and m=='sep':
                break
            df = pd.read_csv(import_path+'cps_'+m+str(year)+'.csv')
            df = assign_groups(df)
            U3Rate, U6Rate, U3LFPR, U6LFPR, SelfRate, wagerate, ptrate = calc_rates(df)
            df.to_csv(import_path+'cps_'+m+str(year)+'.csv',index=False)

            data = pd.DataFrame([{'Year':year,'Month':m,'U3_Rate':U3Rate,'U3_LFPR':U3LFPR,'U6_Rate':U6Rate,'U6_LFPR':U6LFPR,'Self_Rate':SelfRate}])
            df_data = df_data.append(data).reset_index(drop=True)

            df_s = pd.DataFrame([{'Year':year,'Month':m,'Self_Rate':SelfRate,'Wage_Component':wagerate,'PT_Component':ptrate}])
            df_self = df_self.append(df_s).reset_index(drop=True)

            percentages = calc_percentages(df,'U3')
            percentage_list = [{'Year':year,'Month':m,'perc_nilf':percentages[0],'perc_retired_nilf':percentages[1],'perc_disabled_nilf':percentages[2],'perc_student_nilf':percentages[3],'perc_other_nilf':percentages[4],'perc_unemployed':percentages[5],'perc_looking':percentages[6],'perc_layoff':percentages[7],'perc_employed':percentages[8],'perc_employed_ft':percentages[9],'perc_employed_pt':percentages[10],'looking_lf':percentages[11],'layoff_lf':percentages[12],'unemployed_lf':percentages[13],'employed_ft_lf':percentages[14],'employed_pt_lf':percentages[15],'employed_lf':percentages[16],'employed_pt_econ_lf':percentages[17],'employed_pt_nonecon_lf':percentages[18]}]

            df_perc = pd.DataFrame(percentage_list)
            df_percentages = df_percentages.append(df_perc)
    df_data.to_csv(export_path+'unemployment_stats.csv',index=False)
    df_self.to_csv(export_path+'self_stats.csv',index=False) 
    df_percentages.to_csv(export_path+'perc_stats.csv',index=False) 
    toc = time.perf_counter()
    print(f"Process took {toc - tic:0.1f} seconds")

    print(df_data.head())
    