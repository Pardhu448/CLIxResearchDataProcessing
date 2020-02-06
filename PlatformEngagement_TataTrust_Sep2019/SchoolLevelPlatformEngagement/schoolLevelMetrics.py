'''
To get following metrics of school engagement for Tata Trust presentation:
1. Number of module days in a school
2. Number of modules attempted in a school
3. Number of module days with breakdown by domain
4. Number of modules attempted with breakdown by domain
5. Number of lessons attempted in a school
6. Number of lessons attempted with a breakdown by domain
7. Time spent on tools in a school
8. Time spent on tools with breakdown by domain
'''
import numpy as np
from datetime import datetime
import pandas
path = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest/district_level/lab_usage_csvs'
#Rajasthan
#jaipur_labusage_data = pandas.read_csv(path + '/jaipur_lab_usage_March31st2019_monthly.csv')
#sirohi_labusage_data = pandas.read_csv(path + '/sirohi_lab_usage_March31st2019_monthly.csv')
#labusage_data = pandas.concat([jaipur_labusage_data, sirohi_labusage_data])
#Telangana
#warangal_labusage_data = pandas.read_csv(path + '/warangal_lab_usage_March31st2019_monthly.csv')
#karimnagar_labusage_data = pandas.read_csv(path + '/karimnagar_lab_usage_March31st2019_monthly.csv')
#rangareddy_labusage_data = pandas.read_csv(path + '/rangareddy_lab_usage_March31st2019_monthly.csv')
#labusage_data = pandas.concat([warangal_labusage_data, karimnagar_labusage_data, rangareddy_labusage_data])

#labusage_data = pandas.read_csv(path + '/chattisgarh_lab_usage_March31st2019_monthly.csv')
labusage_data = pandas.read_csv(path + '/mizoram_lab_usage_March31st2019_monthly.csv')
modules_domain_map = {"e" : ["[u'English Beginner']", "[u'English Elementary']"],
                      "m" : ["[u'Geometric Reasoning Part I']", "[u'Geometric Reasoning Part II']", "[u'Linear Equations']",
                             "[u'Proportional Reasoning']"],
                      "s" : ["[u'Atomic Structure']", "[u'Sound']", "[u'Understanding Motion']", "[u'Basic Astronomy']",
                             "[u'Health and Disease']", "[u'Ecosystem']", "[u'Reflecting on Values']"]
                      }

domain_module_map = {each: key for key, value in modules_domain_map.items() for each in value}

# To get number of module with and without breakdown by domain
def get_num_mod(school_df):

    try:
        # To get total modules done by students in a school
        school_df = school_df[school_df['module_name'] != "[u'Pre-CLIx Survey']"]
        school_df = school_df[school_df['module_name'] != "[u'Post-CLIx Survey']"]
        school_df['total_num_mod'] = school_df.groupby(['user_id'])['module_name'].apply(lambda x: len(x.unique())).sum()

        school_df['domain'] = school_df['module_name']
        school_df = school_df.replace({'domain': domain_module_map})

        domain_df = school_df.groupby(['user_id', 'domain'])['module_name'].apply(lambda x: len(x.unique())).reset_index()
        domainwise_df  = domain_df.groupby(['domain'])['module_name'].sum().reset_index()

        for each_dom in ['e', 'm', 's']:
           if not domainwise_df[domainwise_df['domain'] == each_dom]['module_name'].empty:
            school_df[each_dom + '_num_modules'] = domainwise_df[domainwise_df['domain'] == each_dom]['module_name'].iloc[0]
           else:
            school_df[each_dom + '_num_modules'] = 0

        school_df = school_df.reset_index()
        final_df = school_df.loc[:, ['total_num_mod', 'e_num_modules', 'm_num_modules', 's_num_modules']]

    except Exception as e:
        print(e)
        import pdb
        pdb.set_trace()

    return final_df.drop_duplicates()

impl_dframe1 = labusage_data.groupby(['school_server_code']).apply(lambda x: get_num_mod(x)).reset_index()
impl_dframe1= impl_dframe1.drop(['level_1'], axis =1)

# To get number of module days in each school with and without breakdown by domain
def get_mod_days(school_df):
    try:
        # To get total module days in a school
        school_df = school_df[school_df['module_name'] != "[u'Pre-CLIx Survey']"]
        school_df = school_df[school_df['module_name'] != "[u'Post-CLIx Survey']"]

        school_df['total_mod_days'] = len(school_df['date_created'].unique())

        school_df['domain'] = school_df['module_name']
        school_df = school_df.replace({'domain': domain_module_map})

        domain_df = school_df.groupby(['domain'])['date_created'].apply(lambda x: len(x.unique())).reset_index()

        for each_dom in ['e', 'm', 's']:
           if not domain_df[domain_df['domain'] == each_dom]['date_created'].empty:
            school_df[each_dom + '_num_days'] = domain_df[domain_df['domain'] == each_dom]['date_created'].reset_index()['date_created'].iloc[0]
           else:
            school_df[each_dom + '_num_days'] = 0
        school_df = school_df.reset_index()
        final_df = school_df.loc[:, ['total_mod_days', 'e_num_days', 'm_num_days', 's_num_days']]

    except Exception as e:
        print(e)
        import pdb
        pdb.set_trace()

    return final_df.drop_duplicates()

impl_dframe2 = labusage_data.groupby(['school_server_code']).apply(lambda x: get_mod_days(x)).reset_index()
impl_dframe2= impl_dframe2.drop(['level_1'], axis =1)

# To get number of lessons done by all students in a school
def get_num_lessons(school_df):
    try:
        # To get total module days in a school
        school_df = school_df[school_df['module_name'] != "[u'Pre-CLIx Survey']"]
        school_df = school_df[school_df['module_name'] != "[u'Post-CLIx Survey']"]

        school_df_lessons = school_df.groupby(['user_id', 'module_name'])['lessons_visited'].max().reset_index()
        school_df['total_num_lessons'] = school_df_lessons['lessons_visited'].sum()
        school_df['total_users'] = len(school_df['user_id'].unique())

        school_df['domain'] = school_df['module_name']
        school_df = school_df.replace({'domain': domain_module_map})

        domain_df = school_df.groupby(['user_id', 'domain', 'module_name'])['lessons_visited'].max().reset_index()
        domainwise_df = domain_df.groupby(['domain'])['lessons_visited'].sum().reset_index()

        for each_dom in ['e', 'm', 's']:
           if not domainwise_df[domainwise_df['domain'] == each_dom]['lessons_visited'].empty:
            school_df[each_dom + '_num_lessons'] = domainwise_df[domainwise_df['domain'] == each_dom]['lessons_visited'].iloc[0]
           else:
            school_df[each_dom + '_num_lessons'] = 0

        school_df = school_df.reset_index()
        final_df = school_df.loc[:, ['total_num_lessons', 'total_users', 'e_num_lessons', 'm_num_lessons', 's_num_lessons']]

    except Exception as e:
        print(e)
        import pdb
        pdb.set_trace()

    return final_df.drop_duplicates()

impl_dframe3 = labusage_data.groupby(['school_server_code']).apply(lambda x: get_num_lessons(x)).reset_index()
impl_dframe3= impl_dframe3.drop(['level_1'], axis =1)

impl_dframe = impl_dframe1.merge(impl_dframe2, left_on = 'school_server_code', right_on = 'school_server_code').merge(impl_dframe3, left_on='school_server_code', right_on = 'school_server_code')

pathfinal = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest/platform_engagement_schoollevel'
impl_dframe.to_csv(pathfinal + '/mz_platform_engagement_schoollevel.csv')
