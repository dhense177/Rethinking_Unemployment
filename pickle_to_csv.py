import pickle, csv





if __name__=='__main__':
    import_path = '/home/dhense/PublicData/Economic_analysis/intermediate_files/cps/'
    export_path = '/home/dhense/PublicData/Economic_analysis/intermediate_files/cps_csv/'

    months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    for year in range(1999,2021):
        for m in months:

            if year==2020 and m=='sep':
                break

            print("...loading pickle")
            tmp = open(import_path+'cps_'+m+str(year)+'.pickle','rb')
            df = pickle.load(tmp)
            tmp.close()

            df.to_csv(export_path+'cps_'+m+str(year)+'.csv',index=False)
        