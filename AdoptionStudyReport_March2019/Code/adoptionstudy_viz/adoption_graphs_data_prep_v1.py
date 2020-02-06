'''
Code to get graphs for adoption study report.
Please refer to this doc for more details:
https://docs.google.com/document/d/1tx-MIh1VFACSSA9oVvb5VP_IF2pTl10hLf656yBrRQ4/edit#heading=h.e6i45hyx8uwl

This includes code to get monthly variation of lab functionality also.
'''

import pandas
import json
from datetime import datetime

tools_domain_map = {
        'e': [],
        'm': ['ice', 'factorisation', 'coins_puzzle','rationpatterns', 'food_sharing_tool', 'ages_puzzle', 'policesquad'],
        's': ['astroamer_element_hunt_activity', 'astroamer_moon_track', 'astroamer_planet_trek_activity']
    }


modules_domain_map = {"e" : ["[u'English Beginner']", "[u'English Elementary']"],
                      "m" : ["[u'Geometric Reasoning Part I']", "[u'Geometric Reasoning Part II']", "[u'Linear Equations']",
                             "[u'Proportional Reasoning']"],
                      "s" : ["[u'Atomic Structure']", "[u'Sound']", "[u'Understanding Motion']", "[u'Basic Astronomy']",
                             "[u'Health and Disease']", "[u'Ecosystem']", "[u'Reflecting on Values']"]
                      }
def clean_code(x):
    if x.split('-')[0] == 'nan':
        return '-' + x.split('-')[1]
    else:
        return x

def get_domain_level_numbers(df, tools_module_flag):

    if (tools_module_flag == 'tools'):
        domain_map = tools_domain_map
        modul_column = 'tool_name'

    elif (tools_module_flag == 'modules'):
        domain_map = modules_domain_map
        modul_column = 'module_name'

    domains = ['e', 'm', 's']
    df['month'] = pandas.to_datetime(df['date_created'], format="%Y-%m-%d").apply(lambda x: x.month)

    def get_num_stud(df_month, dom, modul_column):
        df_month['stud_per_month_' + dom] = len(df_month.loc[df_month[modul_column].isin(domain_map[dom])]['user_id'].unique())
        return df_month

    def get_num_days(df_month, dom, modul_column):
        df_month['days_per_month_' + dom] = len(df_month.loc[df_month[modul_column].isin(domain_map[dom])]['date_created'].unique())
        return df_month

    if tools_module_flag == 'tools':
        def get_time_spent_day(df_month, dom):
            # Trying to capture range of average daily time spent
            # in a month across all schools in a state.
            domain_logs = df_month.loc[df_month[modul_column].isin(domain_map[dom])]
            if not domain_logs.empty:
                def get_daily_time(x):
                    return x.groupby(['session_id', 'user_id']).apply(lambda x: x['time_spent'].unique()[0]).sum()
                daily_time_spent = domain_logs.groupby(['date_created']).apply(lambda x: get_daily_time(x))
                df_month['avg_time_spent_month_' + dom] = daily_time_spent.mean()
                return df_month
            else:
                df_month['avg_time_spent_month_' + each_dom] = 0
                return df_month

        for each_dom in domains:
            #To get total time spent (averaged to per day) by all students in a month for a particular domain
            df = df.groupby(['month']).apply(lambda x: get_time_spent_day(x, each_dom)).reset_index(level=None, drop=True)
            df['avg_timesp_year_' + each_dom] = df.groupby(['month']).apply(lambda x: x['avg_time_spent_month_' + each_dom]).mean()

    if tools_module_flag == 'modules':

        def get_stud_visit_activit(df_month, dom, modul_column):
            modul_dframe = df_month.loc[df_month[modul_column].isin(domain_map[dom])]
            df_month['stud_visit_activ_month_' + dom] = len(modul_dframe[modul_dframe['percentage_activities_visited'] >= 50]['user_id'].unique())
            return df_month

        #To get total number of students who visited atleast 50% of the activities
        for each_dom in domains:
            df = df.groupby(['month']).apply(lambda x: get_stud_visit_activit(x, each_dom, modul_column)).reset_index(level=None, drop=True)
            agg_modul_df = df.loc[df[modul_column].isin(domain_map[each_dom])]
            df['stud_vist_activit_year_' + each_dom] = len(agg_modul_df[agg_modul_df['percentage_activities_visited'] >= 50]['user_id'].unique())

    for each_dom in domains:
        df = df.groupby(['month']).apply(lambda x: get_num_stud(x, each_dom, modul_column)).reset_index(level=None, drop=True)
        df = df.groupby(['month']).apply(lambda x: get_num_days(x, each_dom, modul_column)).reset_index(level=None, drop=True)
        df['total_stud_' + each_dom] = len(df.loc[df[modul_column].isin(tools_domain_map[each_dom])]['user_id'].unique())
        df['total_days_' + each_dom] = len(df.loc[df[modul_column].isin(tools_domain_map[each_dom])]['date_created'].unique())

    return df

# Tool usage - Snapshot View
# Number of students in each domain and number of days on each domain
# Average daily time spent on each domain
# These above metrics are calculated monthly
data_path = '/home/parthae/Documents/Projects/TISS_Git/projects/Visualisations/data'

#ct_data = pandas.read_csv(data_path + 'data/ct_metrics_tools_March31st2019.csv')
#rj_data = pandas.read_csv(data_path + 'data/rj_metrics_tools_March31st2019.csv')
#mz_data = pandas.read_csv(data_path + 'data/mz_metrics_tools_March31st2019.csv')
#tg_data = pandas.read_csv(data_path + 'data/tg_metrics_tools_March31st2019.csv')
#cumul_data = pandas.concat([ct_data, rj_data, mz_data, tg_data], ignore_index=True)
rj_data = pandas.read_csv(data_path + '/tools_data/rj_metrics_tools_March31st2019.csv')
sirohi_tools_data = rj_data.loc[rj_data['school_server_code'].apply(lambda x: x[1:3] == '07')]
jaipur_tools_data =  rj_data.loc[rj_data['school_server_code'].apply(lambda x: x[1:3] == '05')]

tg_data = pandas.read_csv(data_path + '/tools_data/tg_metrics_tools_March31st2019.csv')
karimnagar_tools_data = tg_data.loc[tg_data['school_server_code'].apply(lambda x: x[3:5] == '20')]
warangal_tools_data = tg_data.loc[tg_data['school_server_code'].apply(lambda x: x[3:5] == '10')]
rangareddy_tools_data = tg_data.loc[tg_data['school_server_code'].apply(lambda x: x[3:5] == '30')]

list_of_dfs = [(sirohi_tools_data, 'sirohi'), (jaipur_tools_data, 'jaipur'),
               (karimnagar_tools_data, 'karimnagar'), (warangal_tools_data, 'warangal'),
               (rangareddy_tools_data, 'rangareddy')]

list_of_sv_tool_usage = []

for each_state in list_of_dfs:

    each_state[0]['date_created_mask'] = pandas.to_datetime(each_state[0]['date_created'], format = "%Y-%m-%d").apply(lambda x: x.date())

    each_state_dframe = each_state[0][each_state[0]['date_created_mask'] >= datetime.date(datetime.strptime('2018-07-01', "%Y-%m-%d"))]
    each_state_dframe = each_state_dframe[each_state_dframe['date_created_mask'] <= datetime.date(datetime.strptime('2018-12-31', "%Y-%m-%d"))]

    each_state_dframe = each_state_dframe[~((each_state_dframe['tool_name'] == 'policesquad') & (each_state_dframe['time_spent'] >= 120))]
    each_state_dframe = each_state_dframe[each_state_dframe['time_spent'] <= 200]

    each_state_dframe = each_state_dframe.groupby(["school_server_code"]).apply(lambda x: get_domain_level_numbers(x, 'tools')).reset_index(level=None, drop=True)
    print('Done with tools data for state - {}'.format(each_state[1]))
    list_of_sv_tool_usage.append((each_state[1], each_state_dframe))

## To get SV tables
for eachdf in list_of_sv_tool_usage:

    sv_tool_total_stud = ['school_server_code', 'total_stud_e', 'total_stud_m', 'total_stud_s']
    domain_stud_sv = eachdf[1][sv_tool_total_stud].drop_duplicates(['school_server_code'])
    domain_stud_sv.to_csv(data_path + '/data_for_vis/' + eachdf[0] + '_tools_stud_domainwise.csv', index=False)

    sv_tool_total_days = ['school_server_code', 'total_days_e', 'total_days_m', 'total_days_s']
    domain_days_sv = eachdf[1][sv_tool_total_days].drop_duplicates(['school_server_code'])
    domain_days_sv.to_csv(data_path + '/data_for_vis/' + eachdf[0] + '_tools_days_domainwise.csv', index=False)

    sv_tool_avg_timesp = ['school_server_code', 'avg_timesp_year_e', 'avg_timesp_year_m', 'avg_timesp_year_s']
    domain_timesp_sv = eachdf[1][sv_tool_avg_timesp].drop_duplicates(['school_server_code'])
    domain_timesp_sv.to_csv(data_path + '/data_for_vis/' + eachdf[0] + '_tools_timesp_domainwise.csv', index=False)

# Module usage - Snapshot View
# Number of students in each domain and number of days on each domain
# Number of students who visited atleast 50% of the activities in course module
# Number of days modules was used by 50% of the students (cut 2).
# These above metrics are calculated monthly

#ct_progress_data = pandas.read_csv(data_path + 'data/ct_progress_collated_data_March31st2019.csv')
#rj_progress_data = pandas.read_csv(data_path + 'data/rj_progress_collated_data_March31st2019.csv')
#mz_progress_data = pandas.read_csv(data_path + 'data/mz_progress_collated_data_March31st2019.csv')
#tg_progress_data = pandas.read_csv(data_path + 'data/tg_progress_collated_data_March31st2019.csv')

#mz_progress_data['date_created'] = pandas.to_datetime(mz_progress_data['timestamp'], format = "%Y-%m-%d %H:%M:%S").apply(lambda x: x.date())
#mz_dframe = mz_progress_data[mz_progress_data['date_created'] >= datetime.date(datetime.strptime('2018-07-01', "%Y-%m-%d"))]
#mz_dframe = mz_dframe[mz_dframe['date_created'] <= datetime.date(datetime.strptime('2018-12-31', "%Y-%m-%d"))]

'''
def get_domwise_activities(df):

    def get_modul_activities(df_stud, dom):
        df_stud = df_stud.loc[df_stud['module_name'].isin(modules_domain_map[dom])]
        if df_stud.empty:
            return 0
        else:
            return df_stud.groupby(['module_name']).apply(lambda x: max(x['activities_visited'])).sum()
    df['e_avg_activities'] = df.groupby(['user_id']).apply(lambda x: get_modul_activities(x, 'e')).mean()
    df['m_avg_activities'] = df.groupby(['user_id']).apply(lambda x: get_modul_activities(x, 'm')).mean()
    df['s_avg_activities'] = df.groupby(['user_id']).apply(lambda x: get_modul_activities(x, 's')).mean()
    return df

mz_dframe = mz_dframe.groupby(['server_id']).apply(lambda x: get_domwise_activities(x)).reset_index(level=None, drop=True)
keep_cols = ['server_id', 'e_avg_activities', 'm_avg_activities', 's_avg_activities']
mz_dframe_new = mz_dframe.drop_duplicates(subset=keep_cols)[keep_cols]
mz_dframe_new.to_csv('mz_school_actvities_profile.csv')

def get_domainwise_moduledays(df):
    df['e_moduldays'] = len(df.loc[df['module_name'].isin(modules_domain_map['e'])]['date_created'].unique())
    df['m_moduldays'] = len(df.loc[df['module_name'].isin(modules_domain_map['m'])]['date_created'].unique())
    df['s_moduldays'] = len(df.loc[df['module_name'].isin(modules_domain_map['s'])]['date_created'].unique())
    return df

def get_domainwise_modulevisits(df):
    df['e_modulvisits'] = len(df.loc[df['module_name'].isin(modules_domain_map['e'])]['module_name'].unique())
    df['m_modulvisits'] = len(df.loc[df['module_name'].isin(modules_domain_map['m'])]['module_name'].unique())
    df['s_modulvisits'] = len(df.loc[df['module_name'].isin(modules_domain_map['s'])]['module_name'].unique())
    return df

mz_dframe = mz_dframe.groupby(['server_id']).apply(lambda x: get_domainwise_moduledays(x)).reset_index(level=None, drop=True)
mz_dframe = mz_dframe.groupby(['server_id']).apply(lambda x: get_domainwise_modulevisits(x)).reset_index(level=None, drop=True)
keep_cols = ['server_id', 'e_moduldays', 'm_moduldays', 's_moduldays', 'e_modulvisits', 'm_modulvisits', 's_modulvisits']

mz_dframe_new = mz_dframe.drop_duplicates(subset = keep_cols)[keep_cols]
mz_dframe_new['temp'] = mz_dframe_new['server_id'].apply(lambda x: int(x[2:]))
mz_dframe_new = mz_dframe_new.sort_values('temp')
mz_dframe_new.to_csv('mz_LA_domainwise_modul_days_n_visits.csv')


mz_dframe_moduledays = mz_dframe.groupby(['server_id']).apply(lambda x: len(x['date_created'].unique())).reset_index()
mz_dframe_moduledays['temp'] = mz_dframe_moduledays['server_id'].apply(lambda x: int(x[2:]))
mz_dframe_moduledays = mz_dframe_moduledays.sort_values('temp')
mz_dframe_moduledays.to_csv('mz_school_module_days.csv')

mz_dframe_modulesvisited = mz_dframe.groupby(['server_id']).apply(lambda x: len(x['module_name'].unique())).reset_index()
mz_dframe_modulesvisited['temp'] = mz_dframe_modulesvisited['server_id'].apply(lambda x: int(x[2:]))
mz_dframe_modulesvisited = mz_dframe_modulesvisited.sort_values('temp')
mz_dframe_modulesvisited.to_csv('mz_school_modules_visited.csv')


len(ct_dframe_moduledays['server_id'].unique())
'''
sirohi_progress = pandas.read_csv(data_path + '/progress_csv_data/' + 'sirohi_ProgressCsv_May2019.csv')
jaipur_progress = pandas.read_csv(data_path + '/progress_csv_data/' + 'jaipur_ProgressCsv_May2019.csv')

warangal_progress = pandas.read_csv(data_path + '/progress_csv_data/' + 'warangal_ProgressCsv_May2019.csv')
karimnagar_progress = pandas.read_csv(data_path + '/progress_csv_data/' + 'karimnagar_ProgressCsv_May2019.csv')
rangareddy_progress = pandas.read_csv(data_path + '/progress_csv_data/' + 'rangareddy_ProgressCsv_May2019.csv')

#cumul_progress_data = pandas.concat([ct_progress_data, rj_progress_data, mz_progress_data, tg_progress_data], ignore_index=True)

list_of_prog_dfs = [(sirohi_progress, 'sirohi'), (jaipur_progress, 'jaipur'),
                    (warangal_progress, 'warangal'), (karimnagar_progress, 'karimnagar'),
                    (rangareddy_progress, 'rangareddy')]

list_of_sv_module_usage = []
for each_state in list_of_prog_dfs:

    each_state[0]['date_created'] = pandas.to_datetime(each_state[0]['timestamp'], format = "%Y-%m-%d %H:%M:%S").apply(lambda x: x.date())

    each_state_dframe = each_state[0][each_state[0]['date_created'] >= datetime.date(datetime.strptime('2018-07-01', "%Y-%m-%d"))]
    each_state_dframe = each_state_dframe[each_state_dframe['date_created'] <= datetime.date(datetime.strptime('2018-12-31', "%Y-%m-%d"))]

    school_server_code = [each[0] + each[1] for each in zip(each_state_dframe['school_code'].apply(str).apply(lambda x: x.split('.')[0]).tolist(),
                              each_state_dframe['server_id'].apply(lambda x: '-' + x).tolist())]
    each_state_dframe['school_server_code'] = pandas.Series(school_server_code, index=each_state_dframe.index).apply(lambda x: clean_code(x))

    each_state_df = each_state_dframe.groupby(["school_server_code"]).apply(lambda x: get_domain_level_numbers(x, 'modules')).reset_index(level=None, drop=True)
    print('Done with progress csv data for state - {0}'.format(each_state[1]))
    list_of_sv_module_usage.append((each_state[1], each_state_df))

## To get SV tables
for eachdf in list_of_sv_module_usage:

    sv_module_total_stud = ['school_server_code', 'total_stud_e', 'total_stud_m', 'total_stud_s']
    mod_domain_stud_sv = eachdf[1][sv_module_total_stud].drop_duplicates(['school_server_code'])
    mod_domain_stud_sv.to_csv(data_path + '/data_for_vis/' + eachdf[0] + '_modules_stud_domainwise.csv', index=False)

    sv_module_total_days = ['school_server_code', 'total_days_e', 'total_days_m', 'total_days_s']
    mod_domain_days_sv = eachdf[1][sv_module_total_days].drop_duplicates(['school_server_code'])
    mod_domain_days_sv.to_csv(data_path + '/data_for_vis/' + eachdf[0] + '_modules_days_domainwise.csv', index=False)

    sv_module_visit_activ = ['school_server_code', 'stud_vist_activit_year_e', 'stud_vist_activit_year_m', 'stud_vist_activit_year_s']
    domain_activ_visit_sv = eachdf[1][sv_module_visit_activ].drop_duplicates(['school_server_code'])
    domain_activ_visit_sv.to_csv(data_path + '/data_for_vis/' + eachdf[0] + '_modules_activ_visit_domainwise.csv', index=False)

# Tools usage - Time Variation
# Here we get monthly variation of number of days, number of students and time spent in a day across
# all schools in a given state.
domains = ['e', 'm', 's']
no_usage_min = 0.25
def get_monthly_range(state_df, metric):

    domains = ['e', 'm', 's']

    if metric == 'domain_stud':
        for each_dom in domains:
            state_df['stud_per_month_' + each_dom] = state_df['stud_per_month_' + each_dom].fillna(value=0)
            state_df_temp = state_df[state_df['stud_per_month_' + each_dom] > 0]
            try:
                if not state_df_temp.empty:
                    #There is some domain activity
                    if (min(state_df_temp['stud_per_month_' + each_dom]) == max(state_df_temp['stud_per_month_' + each_dom])):
                        # Single instance of domain activity
                        state_df['stud_monthly_min_' + each_dom] = 0
                        state_df['stud_monthly_max_' + each_dom] = max(state_df_temp['stud_per_month_' + each_dom])
                    else:
                        state_df['stud_monthly_min_' + each_dom] = min(state_df_temp['stud_per_month_' + each_dom])
                        state_df['stud_monthly_max_' + each_dom] = max(state_df_temp['stud_per_month_' + each_dom])
                else:
                    #No domain activity
                    state_df['stud_monthly_min_' + each_dom] = 0
                    state_df['stud_monthly_max_' + each_dom] = no_usage_min
            except Exception as e:
                import pdb
                pdb.set_trace()

    if metric == 'domain_days':
        for each_dom in domains:
            state_df['days_per_month_' + each_dom] = state_df['days_per_month_' + each_dom].fillna(value=0)
            state_df_temp = state_df[state_df['days_per_month_' + each_dom] > 0]
            try:
                if not state_df_temp.empty:
                    if (min(state_df_temp['days_per_month_' + each_dom]) == max(
                            state_df_temp['days_per_month_' + each_dom])):
                        state_df['days_monthly_min_' + each_dom] = 0
                        state_df['days_monthly_max_' + each_dom] = max(state_df_temp['days_per_month_' + each_dom])
                    else:
                        state_df['days_monthly_min_' + each_dom] = min(state_df_temp['days_per_month_' + each_dom])
                        state_df['days_monthly_max_' + each_dom] = max(state_df_temp['days_per_month_' + each_dom])
                else:
                    state_df['days_monthly_min_' + each_dom] = 0
                    state_df['days_monthly_max_' + each_dom] = no_usage_min
            except Exception as e:
                import pdb
                pdb.set_trace()

    if metric == 'domain_timesp':
        for each_dom in domains:
            state_df['avg_time_spent_month_' + each_dom] = state_df['avg_time_spent_month_' + each_dom].fillna(value=0)
            state_df_temp = state_df[state_df['avg_time_spent_month_' + each_dom] > 0]
            try:
                if not state_df_temp.empty:
                    if (min(state_df_temp['avg_time_spent_month_' + each_dom]) == max(
                            state_df_temp['avg_time_spent_month_' + each_dom])):
                        state_df['timesp_monthly_min_' + each_dom] = 0
                        state_df['timesp_monthly_max_' + each_dom] = max(state_df_temp['avg_time_spent_month_' + each_dom])
                    else:
                        state_df['timesp_monthly_min_' + each_dom] = min(state_df_temp['avg_time_spent_month_' + each_dom])
                        state_df['timesp_monthly_max_' + each_dom] = max(state_df_temp['avg_time_spent_month_' + each_dom])
                else:
                    state_df['timesp_monthly_min_' + each_dom] = 0
                    state_df['timesp_monthly_max_' + each_dom] = no_usage_min
            except Exception as e:
                import pdb
                pdb.set_trace()

    if metric == 'domain_percn_activ':
        for each_dom in domains:

            state_df['stud_visit_activ_month_' + each_dom] = state_df['stud_visit_activ_month_' + each_dom].fillna(value=0)
            state_df_temp = state_df[state_df['stud_visit_activ_month_' + each_dom] > 0]
            try:
                if not state_df_temp.empty:
                    if (min(state_df_temp['stud_visit_activ_month_' + each_dom]) == max(
                            state_df_temp['stud_visit_activ_month_' + each_dom])):
                        state_df['percn_activ_monthly_min_' + each_dom] = 0
                        state_df['percn_activ_monthly_max_' + each_dom] = max(state_df_temp['stud_visit_activ_month_' + each_dom])
                    else:
                        state_df['percn_activ_monthly_min_' + each_dom] = min(state_df_temp['stud_visit_activ_month_' + each_dom])
                        state_df['percn_activ_monthly_max_' + each_dom] = max(state_df_temp['stud_visit_activ_month_' + each_dom])
                else:
                    state_df['percn_activ_monthly_min_' + each_dom] = 0
                    state_df['percn_activ_monthly_max_' + each_dom] = no_usage_min
            except Exception as e:
                import pdb
                pdb.set_trace()

    return state_df

for each_tool_n_modul in [('tool', list_of_sv_tool_usage), ('module', list_of_sv_module_usage)]:

   tool_modul_flag = each_tool_n_modul[0]
   for eachdf in each_tool_n_modul[1]:

    eachdf[1]['month_year'] = pandas.to_datetime(eachdf[1]['date_created'], format="%Y-%m-%d").apply(lambda d: d.month_name() +'_'+ str(d.year))
    df_domain = eachdf[1].drop_duplicates(['month_year', 'school_server_code', 'date_created'])

    domain_stud_tv = df_domain.groupby(['month_year']).apply(lambda x: get_monthly_range(x, metric='domain_stud')).reset_index(level=None, drop=True)
    domain_days_tv = df_domain.groupby(['month_year']).apply(lambda x: get_monthly_range(x, metric='domain_days')).reset_index(level=None, drop=True)

    if tool_modul_flag == 'tool':
      domain_timesp_tv = df_domain.groupby(['month_year']).apply(lambda x: get_monthly_range(x, metric='domain_timesp')).reset_index(level=None, drop=True)
      domain_metric_list = ['stud', 'days', 'timesp']

    elif tool_modul_flag == 'module':
      domain_perc_activ_tv = df_domain.groupby(['month_year']).apply(lambda x: get_monthly_range(x, metric='domain_percn_activ')).reset_index(level=None, drop=True)
      domain_metric_list = ['stud', 'days', 'percnactiv']

    domains = ['e', 'm', 's']
    for each_metric in domain_metric_list:
       if each_metric == 'stud':
           domain_df = domain_stud_tv
           metric_col = 'stud_monthly'
       if each_metric == 'days':
           domain_df = domain_days_tv
           metric_col = 'days_monthly'
       if each_metric == 'timesp':
           domain_df = domain_timesp_tv
           metric_col = 'timesp_monthly'
       if each_metric == 'percnactiv':
           domain_df = domain_perc_activ_tv
           metric_col = 'percn_activ_monthly'

       domain_df_list = []
       domain_df = domain_df.sort_values(['date_created'])

       for each_dom in domains:
           cols = ['month_year', metric_col + '_min_' + each_dom, metric_col + '_max_' + each_dom]
           domain_df_temp = domain_df.drop_duplicates(cols)
           tempdf = domain_df_temp[cols].rename(columns={metric_col + '_min_' + each_dom : 'y0', metric_col + '_max_' + each_dom : 'y1'})
           domain_df_list.append(tempdf.to_dict(orient='records'))

       month_range = ['July_2018', 'August_2018', 'September_2018', 'October_2018',
                      'November_2018', 'December_2018']
       domain_vis_data = {'categories': month_range,
                        'series': ["English Module", "Science Module", "Math Module"],
                        'colors': ["#87CEFA", "#F0E68C", "#C0C0C0"],
                        'layers': domain_df_list
                        }

       with open(data_path + '/data_for_vis/' + eachdf[0] + '_' + tool_modul_flag + '_' + metric_col + '.json', 'w') as write_file:
        json.dump(domain_vis_data, write_file)

##########################################################################################
#To generate data for Lab functionality/usage graphs as monthly variation

#ct_lab_func = pandas.read_csv(data_path + 'data/' + 'ct_progress_lab_func_March31st2019_tv.csv')
#rj_lab_func = pandas.read_csv(data_path + 'data/' + 'rj_progress_lab_func_March31st2019.csv')
#mz_lab_func = pandas.read_csv(data_path + 'data/' + 'mz_progress_lab_func_March31st2019.csv')
#tg_lab_func = pandas.read_csv(data_path + 'data/' + 'tg_progress_lab_func_March31st2019.csv')

'''
This part of code is to generate data of lab func for dynamic visualisation with 
transitions and durations

cumul_lab_func = pandas.concat([ct_lab_func, rj_lab_func, mz_lab_func, tg_lab_func], ignore_index=True)
col_required = ['school_server_code', 'state_code', 'days_server_wo_activity', 'tools_only_activity', 'module_only_activity',
                'tool_with_module_activity', 'total_days_server_on']

lab_func_data_list = [('ct', ct_lab_func), ('rj', rj_lab_func), ('mz', mz_lab_func), ('tg', tg_lab_func), ('all_states', cumul_lab_func)]
for each_state in lab_func_data_list:
    state_progress_data = each_state[1][col_required]
    state_lab_func_data = state_progress_data.drop_duplicates(subset=["school_server_code"])
    # To create a json file structure with our data
    #columns_required = [each_col for each_col in all_states_lab_func][2:]
    columns_required = ['module_only_activity', 'tool_with_module_activity', 'days_server_wo_activity', 'tools_only_activity']
    #data_state_codes = [(state_lab_func_data, 'all_states'), (ct_lab_func, 'ct'), (rj_lab_func, 'rj'), (mz_lab_func, 'mz'), (tg_lab_func, 'tg')]
    data_json = list()
    for each_col in columns_required:
       data_stack = state_lab_func_data[['school_server_code', each_col]]
       data_stack = data_stack.rename(columns={each_col: 'y'}).to_dict(orient='records')
       data_json.append(data_stack)
    with open(data_path + 'data/' + each_state[0] + "_lab_func.json", 'w') as write_file:
       json.dump(data_json, write_file)
'''

data_path = '/home/parthae/Documents/Projects/TISS_Git/projects/Visualisations/data'
cols_lab_func = ['school_server_code', 'days_server_wo_activity', 'tools_only_activity',
                 'module_only_activity', 'tool_with_module_activity', 'total_days_server_on']

#sirohi_lab_func = pandas.read_csv(data_path + '/progress_csv_data/' + 'sirohi_lab_usage_March31st2019.csv')
#jaipur_lab_func = pandas.read_csv(data_path + '/progress_csv_data/' + 'jaipur_lab_usage_March31st2019.csv')

#karimnagar_lab_func = pandas.read_csv(data_path + '/progress_csv_data/' + 'karimnagar_lab_usage_March31st2019.csv')
#warangal_lab_func = pandas.read_csv(data_path + '/progress_csv_data/' + 'warangal_lab_usage_March31st2019.csv')
#rangareddy_lab_func = pandas.read_csv(data_path + '/progress_csv_data/' + 'rangareddy_lab_usage_March31st2019.csv')


mizoram_lab_func = pandas.read_csv(data_path + '/progress_csv_data/' + 'mizoram_lab_usage_March31st2019.csv')


#cumul_lab_func = pandas.concat([ct_lab_func, rj_lab_func, mz_lab_func, tg_lab_func], ignore_index=True)
#lab_func_data_list = [(ct_lab_func, 'ct'), (rj_lab_func, 'rj'), (mz_lab_func, 'mz'), (tg_lab_func, 'tg'), (cumul_lab_func, 'all_states')]
#lab_func_data_list = [(sirohi_lab_func, 'sirohi'), (jaipur_lab_func, 'jaipur'),
#                      (karimnagar_lab_func, 'karimnagar'), (warangal_lab_func, 'warangal'),
#                      (rangareddy_lab_func, 'rangareddy')]
lab_func_data_list = [(mizoram_lab_func, 'mizoram')]

def correct_for_extra_logs(x):
    try:
     if x['days_server_wo_activity'] < 0 :
        x['days_server_wo_activity'] = 0
        x['total'] = x['tools_only_activity'] + x['module_only_activity'] + x['tool_with_module_activity']
    except Exception as e:
        print(e)
        import pdb
        pdb.set_trace()

    return x
#month_slots = [('Jul-Aug-2018', ('2018-07-01', '2018-08-31')), ('Sep-Oct-2018',('2018-09-01', '2018-10-31')), ('Nov-Dec-2018',('2018-11-01', '2018-12-31'))]
month_slots_sv = [('Jul-Dec-2018', ('2018-07-01', '2018-12-31'))]

for each_state in lab_func_data_list:

    for each_slot in month_slots_sv:
        each_state[0]['date_created'] = pandas.to_datetime(each_state[0]['timestamp'], format = "%Y-%m-%d %H:%M:%S").apply(lambda x: x.date())
        each_state_dframe = each_state[0].loc[each_state[0]["month_slot"] == each_slot[0]]

        lab_func_vis = each_state_dframe[cols_lab_func]
        lab_func_vis = lab_func_vis.drop_duplicates(subset=["school_server_code"])
        lab_func_vis = lab_func_vis.rename(columns = {'total_days_server_on' : 'total'})

        if each_slot[0] == 'Jul-Aug-2018':
            lab_func_vis = lab_func_vis.sort_values(by='total', ascending=False)
            schools_ordered = lab_func_vis['school_server_code']

        if each_slot[0] != 'Jul-Aug-2018':
            lab_func_vis = lab_func_vis.set_index('school_server_code')
            lab_func_vis = lab_func_vis.reindex(schools_ordered)
            lab_func_vis.reset_index(level=0, inplace=True)
            lab_func_vis = lab_func_vis.fillna(value=0)

        lab_func_vis = lab_func_vis.apply(lambda x: correct_for_extra_logs(x), axis=1).reset_index(level=None, drop=True)
        lab_func_vis = lab_func_vis.drop(['total'], axis=1)
        lab_func_vis = lab_func_vis.rename(columns = {'days_server_wo_activity' : 'Server on Without Activity',
                                                      'tools_only_activity' : 'Tools only Activity',
                                                      'module_only_activity': 'Modules only Activity',
                                                      'tool_with_module_activity': 'Tools with Modules Activity'})
        lab_func_vis.to_csv(data_path + '/data_for_vis/' + each_state[1] + '_' + each_slot[0] + "_lab_func.csv", index=False)

###############################################################################################
#To get reporting tables for tools data
'''
for eachdf in list_of_sv_tool_usage:

    sv_tool_total_stud = ['school_server_code', 'total_stud_e', 'total_stud_m', 'total_stud_s']
    domain_stud_sv = eachdf[1][sv_tool_total_stud].drop_duplicates(['school_server_code'])
    domain_stud_sv.to_csv(eachdf[0] + '_tools_stud_domainwise.csv', index=False)

    sv_tool_total_days = ['school_server_code', 'total_days_e', 'total_days_m', 'total_days_s']
    domain_days_sv = eachdf[1][sv_tool_total_days].drop_duplicates(['school_server_code'])
    domain_days_sv.to_csv(eachdf[0] + '_tools_days_domainwise.csv', index=False)

    sv_tool_avg_timesp = ['school_server_code', 'avg_timesp_year_e', 'avg_timesp_year_m', 'avg_timesp_year_s']
    domain_timesp_sv = eachdf[1][sv_tool_avg_timesp].drop_duplicates(['school_server_code'])
    domain_timesp_sv.to_csv(eachdf[0] + '_tools_timesp_domainwise.csv', index=False)



def get_report(df):
    school_data = {}
    school_data["school_server_code"] = [df["school_server_code"].unique()[0]]

    school_data["total_students"] = [df["total_students"].unique()[0]]

    school_data["total_days"] = [df["total_days"].unique()[0]]
    school_data["num_of_consistent_stud"] = [df["num_of_consistent_stud"].unique()[0]]

    school_data["num_stud_activity"] = [df["num_stud_activity"].unique()[0]]
    school_data["num_stud_lessons"] = [df["num_stud_lessons"].unique()[0]]
    return pandas.DataFrame(school_data)

reporting_df = final_progress_df.groupby(["school_server_code"]).apply(lambda x: get_report(x)).reset_index(level=None, drop=True)
'''



