from itertools import combinations, product, chain















if __name__=='__main__':
    var_list = ['Sex','Race','Hispanic','Age_group','Marital_Status','School_completed','State_FIPS','Disabled','Ever_active_duty']

    sex_list = [{'Sex': 'Female'}, {'Sex': 'Male'}]

    race_list = [{'Race': 'White Only'}, {'Race': 'Black Only'}]

    hispanic_list = [{'Hispanic': 'Hispanic'}, {'Hispanic': 'Not Hispanic'}]

    #change age grouping from number to string range (e.g. 1 = '16-19'); limit ages 20-65 for subpops
    age_list = [{'Age_group': (2,3)},{'Age_group': (3,4)},{'Age_group': (4,5)},{'Age_group': (5,6)},{'Age_group': (6,7)},{'Age_group': (7,8)},{'Age_group': (8,9)},{'Age_group': (9,10)}]

    marital_list = [{'Marital_status': 'Married'},{'Marital_status': 'Never married'},{'Marital_status': 'Divorced'},{'Marital_status': 'Widowed'},{'Marital_status': 'Separated'}]

    school_list = [{'School_completed': 'High school degree'},{'School_completed': 'No high school degree'},{'School_completed': 'Bachelors degree'},{'School_completed': 'Masters degree'},{'School_completed': 'Associate degree'},{'School_completed': 'Professional school degree'},{'School_completed': 'Doctorate degree'}]

    division_list = [{'Division': 'South Atlantic'},{'Division': 'East North Central'},{'Division': 'Mid-Atlantic'},{'Division': 'Pacific'},{'Division': 'Mountain'},{'Division': 'West South Central'},{'Division': 'West North Central'},{'Division': 'New England'},{'Division': 'East South Central'}]

    state_list = [{'State_FIPS': '06'},{'State_FIPS': '36'},{'State_FIPS': '12'},{'State_FIPS': '48'},{'State_FIPS': '42'},{'State_FIPS': '17'},{'State_FIPS': '39'},{'State_FIPS': '26'},{'State_FIPS': '34'},{'State_FIPS': '37'},{'State_FIPS': '25'},{'State_FIPS': '13'},{'State_FIPS': '04'},{'State_FIPS': '51'},{'State_FIPS': '40'},{'State_FIPS': '01'},{'State_FIPS': '54'},{'State_FIPS': '16'},{'State_FIPS': '32'},{'State_FIPS': '27'},{'State_FIPS': '22'},{'State_FIPS': '08'},{'State_FIPS': '55'},{'State_FIPS': '47'},{'State_FIPS': '30'},{'State_FIPS': '05'},{'State_FIPS': '35'},{'State_FIPS': '31'},{'State_FIPS': '21'},{'State_FIPS': '20'},{'State_FIPS': '49'},{'State_FIPS': '46'},{'State_FIPS': '18'},{'State_FIPS': '19'},{'State_FIPS': '53'},{'State_FIPS': '38'},{'State_FIPS': '56'},{'State_FIPS': '41'},{'State_FIPS': '29'},{'State_FIPS': '45'},{'State_FIPS': '02'},{'State_FIPS': '28'},{'State_FIPS': '24'},{'State_FIPS': '23'},{'State_FIPS': '33'},{'State_FIPS': '44'},{'State_FIPS': '09'},{'State_FIPS': '50'},{'State_FIPS': '10'},{'State_FIPS': '11'},{'State_FIPS': '15'}]

    #disabled_list = [{'Disabled':i} for i in list(df['Disabled'].value_counts().index)]
    #duty_list = list(df['Ever_active_duty'].value_counts().index)

    all_list = [sex_list,race_list,hispanic_list,age_list,marital_list,school_list,division_list]


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