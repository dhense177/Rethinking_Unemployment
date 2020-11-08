import os, pickle
import pandas as pd
import numpy as np


def extract(df,yr):
    '''
        Extract relevant household columns and convert data types
    '''
    df_hh = pd.DataFrame()
    df_hh['Record_type'] = [int(i[0][0]) for i in df.values]
    df_hh['HHID'] = [int(i[0][1:6]) for i in df.values]
    df_hh['Interview_type'] = [int(i[0][19:20]) for i in df.values]
    df_hh['HHINC'] = [int(i[0][271:273]) for i in df.values]
    df_hh['Year'] = yr

    #Household-level variables
    col_dict = {'HH_Income':(247,255),'Self_Employment':(98,105),'Wages_Salaries':(90,97),'Unemployment_comp':(114,121),'Social_Security':(130,137),'Public_Assistance':(145,151),'Workers_Comp':(122,129),'Supplemental_Security':(138,144),'Veterans_Benefits':(152,159),'Survivors_Income':(160,167),'Disability':(168,175),'Retirement_Income':(176,183),'Interest':(184,191),'Dividends':(192,199),'Rents':(200,207),'Education':(208,215),'Child_Support':(216,223),'Financial_Assistance':(232,239),'Other_Income':(240,247)}

    for k,v in col_dict.items():
        df_hh[k] = [i[0][v[0]:v[1]] for i in df.values]


    #Flags - Household
    flag_dict = {'HINC_SE':97,'HINC_WS':89,'HINC_UC':113,'HSS_YN':129,'HPAW_YN':144,'HINC_WC':121,'HSSI_YN':137,'HVET_YN':151,'HSUR_YN':159,'HDIS_YN':167,'HRET_YN':175,'HINT_YN':183,'HDIV_YN':191,'HRNT_YN':199,'HED_YN':207,'HCSP_YN':215,'HFIN_YN':231,'HOI_YN':239}

    for k,v in flag_dict.items():
        df_hh[k] = [i[0][v] for i in df.values]


    #Restrict to households, successful interviews and rows with reported total income figures
    df_hh = df_hh[df_hh['Record_type']==1]
    df_hh = df_hh[df_hh['Interview_type']==1]
    df_hh = df_hh[df_hh['HHINC']!=0]


    df_hh = df_hh.astype({'HH_Income': 'int', 'Self_Employment': 'int','Wages_Salaries': 'int','Unemployment_comp': 'int','Social_Security': 'int','Public_Assistance': 'int','Workers_Comp': 'int','Supplemental_Security': 'int','Veterans_Benefits': 'int','Survivors_Income': 'int','Disability': 'int','Retirement_Income': 'int','Interest': 'int','Dividends': 'int','Rents': 'int','Education': 'int','Child_Support': 'int','Financial_Assistance': 'int','Other_Income': 'int','HINC_SE': 'int','HINC_WS': 'int','HINC_UC': 'int','HSS_YN': 'int','HPAW_YN': 'int','HINC_WC': 'int','HSSI_YN': 'int','HVET_YN': 'int','HSUR_YN': 'int','HDIS_YN': 'int','HRET_YN': 'int','HINT_YN': 'int','HDIV_YN': 'int','HRNT_YN': 'int','HED_YN': 'int','HCSP_YN': 'int','HFIN_YN': 'int','HOI_YN': 'int'})


    return df_hh


def extract19(df):
    '''
        Extract relevant household columns and convert data types for 2019 file
    '''
    df_hh = pd.DataFrame()
    col_names = {'Record_type':'HRECORD','Interview_type':'H_HHTYPE','Year':'H_YEAR','HHINC':'HTOTVAL','Self_Employment':'HSEVAL','Wages_Salaries':'HWSVAL','Unemployment_comp':'HUCVAL','Social_Security':'HSSVAL','Public_Assistance':'HPAWVAL','Workers_Comp':'HWCVAL','Supplemental_Security':'HSSIVAL','Veterans_Benefits':'HVETVAL','Survivors_Income':'HSURVAL','Disability':'HDISVAL','Retirement_Income':'HDSTVAL','Interest':'HINTVAL','Dividends':'HDIVVAL','Rents':'HRNTVAL','Education':'HEDVAL','Child_Support':'HCSPVAL','Financial_Assistance':'HFINVAL','Other_Income':'HOIVAL','HINC_WS':'HINC_WS','HINC_UC':'HINC_UC','HSS_YN':'HSS_YN','HPAW_YN':'HPAW_YN','HINC_WC':'HINC_WC','HSSI_YN':'HSSI_YN','HVET_YN':'HVET_YN','HSUR_YN':'HSUR_YN','HDIS_YN':'HDIS_YN','HRET_YN':'HDST_YN','HINT_YN':'HINT_YN','HDIV_YN':'HDIV_YN','HRNT_YN':'HRNT_YN','HED_YN':'HED_YN','HCSP_YN':'HCSP_YN','HFIN_YN':'HFIN_YN','HOI_YN':'HOI_YN'}

    for k,v in col_names.items():
        df_hh[k] = df[v]


    #Restrict to households, successful interviews and rows with reported total income figures
    df_hh = df_hh[df_hh['Record_type']==1]
    df_hh = df_hh[df_hh['Interview_type']==1]
    df_hh = df_hh[df_hh['HHINC']!=0]

    return df_hh


def extract19p(df,yr):
    '''
        Extract relevant person columns and convert data types for 2019 file
    '''
    df_p = pd.DataFrame()
    df_p['Hourly_Pay'] = df['A_HRSPAY']/100
    df_p['Weekly_hrs_worked'] = df['A_USLHRS']
    df_p['Hourly_Worker?'] = df['A_HRLYWK']
    df_p['Year'] = str(yr)
    return df_p



def extract_person(df,yr):
    '''
        Extract relevant person columns and convert data types for 2019 file
    '''
    df_p = pd.DataFrame()
    df_p['Record_type'] = [int(i[0][0]) for i in df.values]
    df_p['Hourly_Pay'] = [int(i[0][186:190]) if int(i[0][186:190]) < 100 else int(i[0][186:190])/100 for i in df.values]
    df_p['Person_type'] = [int(i[0][29]) for i in df.values]


    df_p['LF_recode'] = [int(i[0][217:218]) for i in df.values]
    # df_p['Total_Earnings'] = [(i[0][382:390]) for i in df.values]
    df_p['Total_Wage_Salary'] = [int(i[0][363:370]) for i in df.values]
    df_p['Full/Part_Year_Worker'] = [int(i[0][276]) for i in df.values]
    df_p['Year'] = str(yr)

    df_p = df_p[df_p['Record_type']==3]
    df_p = df_p[df_p['Person_type']==2]

    # df_p = df_p.astype({'Total_Earnings':'int'})

    #Flags
    # df_p['Hourly_Worker?'] = [int(i[0][185:186]) for i in df.values]


    # df_p = df_p[['Year','Hourly_Pay','Weekly_hrs_worked','Hourly_Worker?']]

    return df_p




if __name__=='__main__':
    fp = '/home/dhense/PublicData/cps_files/'
    filepath = '/home/dhense/PublicData/Economic_analysis/'

    # cps_pickle = 'cps.pickle'
    # cpsp_pickle = 'cpsp.pickle'
    # if not os.path.isfile(filepath+'intermediate_files/'+cps_pickle):
    yr=2016
    f = fp+'2016/asec2016_pubuse_v3.dat'
    df = pd.read_csv(f,header=None)
    df_p = extract_person(df,yr)
    df_h = extract(df,yr)

    # df_dict = {}
    # df_dictp = {}
    # for yr in range(2003,2019):
    #     files = os.listdir(fp+str(yr)+'/')
    #     if yr==2014:
    #         df14a = pd.read_csv(fp+str(2014)+'/asec2014_pubuse_3x8_rerun_v2.dat')
    #         dfa_new = extract(df14a,yr)
    #         dfap_new = extract_person(df14a,yr)
    #         df14b = pd.read_csv(fp+str(2014)+'/asec2014_pubuse_tax_fix_5x8_2017.dat')
    #         dfb_new = extract(df14b,yr)
    #         dfbp_new = extract_person(df14b,yr)
    #         df_new = pd.concat([dfa_new,dfb_new])
    #         df_newp = pd.concat([dfap_new,dfbp_new])
    #     else:
    #         f = [fp+str(yr)+'/'+f for f in files if f[:8]=='asec'+str(yr)][0]
    #         df = pd.read_csv(f,header=None)
    #         df_new = extract(df,yr)
    #         df_newp = extract_person(df,yr)
    #     df_dict['asec'+str(yr)] = df_new
    #     df_dictp['asec'+str(yr)] = df_newp

    # df19 = pd.read_csv(fp+'2019/hhpub19.csv')
    # df_dict['asec2019'] = extract19(df19)
    # df = pd.concat(df_dict.values(),ignore_index=True)

    # df19p = pd.read_csv(fp+'2019/pppub19.csv')
    # df_dictp['asec2019'] = extract19p(df19p,2019)
    # dfp = pd.concat(df_dictp.values(),ignore_index=True)



    #     print("...saving pickle")
    #     tmp = open(filepath+'intermediate_files/'+cps_pickle,'wb')
    #     pickle.dump(df,tmp)
    #     tmp.close()
    #     print("...saving pickle")
    #     tmp = open(filepath+'intermediate_files/'+cpsp_pickle,'wb')
    #     pickle.dump(dfp,tmp)
    #     tmp.close()
    # else:
    #     print("...loading pickle")
    #     tmp = open(filepath+'intermediate_files/'+cps_pickle,'rb')
    #     df = pickle.load(tmp)
    #     tmp.close()
    #     print("...loading pickle")
    #     tmp = open(filepath+'intermediate_files/'+cpsp_pickle,'rb')
    #     dfp = pickle.load(tmp)
    #     tmp.close()
