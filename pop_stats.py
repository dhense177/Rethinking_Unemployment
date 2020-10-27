import numpy as np
import pandas as pd




def calc_pop_stats(df_state):
    df_stats = df_state.groupby(['Age_group','Sex','Hispanic','Race']).sum().reset_index()

    female_growth = calc_growth('Sex','Female')
    male_growth = calc_growth('Sex','Male')
    white_growth = calc_growth('Race','White Only')
    black_growth = calc_growth('Race','Black Only')
    hispanic_growth = calc_growth('Hispanic','Hispanic')
    white_male_growth = calc_growth_mult('Sex','Male','Race','White Only')
    white_female_growth = calc_growth_mult('Sex','Female','Race','White Only')
    black_male_growth = calc_growth_mult('Sex','Male','Race','Black Only')
    black_female_growth = calc_growth_mult('Sex','Female','Race','Black Only')
    hispanic_male_growth = calc_growth_mult('Sex','Male','Hispanic','Hispanic')
    hispanic_female_growth = calc_growth_mult('Sex','Female','Hispanic','Hispanic')


    # ages = pd.DataFrame()
    # ages['Year'] = [i for i in range(1991,2019)]
    # for a in df_stats.Age_group.unique():
    #     df_ages = df_stats[df_stats['Age_group']==a][['POPESTIMATE1990','POPESTIMATE1991','POPESTIMATE1992','POPESTIMATE1993','POPESTIMATE1994','POPESTIMATE1995','POPESTIMATE1996','POPESTIMATE1997','POPESTIMATE1998','POPESTIMATE1999','POPESTIMATE2000','POPESTIMATE2001','POPESTIMATE2002','POPESTIMATE2003','POPESTIMATE2004','POPESTIMATE2005','POPESTIMATE2006','POPESTIMATE2007','POPESTIMATE2008','POPESTIMATE2009','POPESTIMATE2010','POPESTIMATE2011','POPESTIMATE2012','POPESTIMATE2013','POPESTIMATE2014','POPESTIMATE2015','POPESTIMATE2016','POPESTIMATE2017','POPESTIMATE2018']].sum()
    #
    #     ages['Age_group_'+str(a)] = [((df_ages['POPESTIMATE'+str(y)]/df_ages['POPESTIMATE'+str(y-1)])-1)*100 for y in range(1991,2019)]
    return female_growth





def calc_growth(df_stats,column,value):
    aggs = df_stats[df_stats[column]==value][['POPESTIMATE1990','POPESTIMATE1991','POPESTIMATE1992','POPESTIMATE1993','POPESTIMATE1994','POPESTIMATE1995','POPESTIMATE1996','POPESTIMATE1997','POPESTIMATE1998','POPESTIMATE1999','POPESTIMATE2000','POPESTIMATE2001','POPESTIMATE2002','POPESTIMATE2003','POPESTIMATE2004','POPESTIMATE2005','POPESTIMATE2006','POPESTIMATE2007','POPESTIMATE2008','POPESTIMATE2009','POPESTIMATE2010','POPESTIMATE2011','POPESTIMATE2012','POPESTIMATE2013','POPESTIMATE2014','POPESTIMATE2015','POPESTIMATE2016','POPESTIMATE2017','POPESTIMATE2018']].sum()

    growth = [((aggs['POPESTIMATE'+str(y)]/aggs['POPESTIMATE'+str(y-1)])-1)*100 for y in range(1991,2019)]

    return growth


def calc_growth_mult(df_stats,column1,value1,column2,value2):
    aggs = df_stats[(df_stats[column1]==value1)&(df_stats[column2]==value2)][['POPESTIMATE1990','POPESTIMATE1991','POPESTIMATE1992','POPESTIMATE1993','POPESTIMATE1994','POPESTIMATE1995','POPESTIMATE1996','POPESTIMATE1997','POPESTIMATE1998','POPESTIMATE1999','POPESTIMATE2000','POPESTIMATE2001','POPESTIMATE2002','POPESTIMATE2003','POPESTIMATE2004','POPESTIMATE2005','POPESTIMATE2006','POPESTIMATE2007','POPESTIMATE2008','POPESTIMATE2009','POPESTIMATE2010','POPESTIMATE2011','POPESTIMATE2012','POPESTIMATE2013','POPESTIMATE2014','POPESTIMATE2015','POPESTIMATE2016','POPESTIMATE2017','POPESTIMATE2018']].sum()

    growth = [((aggs['POPESTIMATE'+str(y)]/aggs['POPESTIMATE'+str(y-1)])-1)*100 for y in range(1991,2019)]

    return growth
