# Module time calculation -- approach 3 
import numpy as np
from datetime import datetime
import pandas

path = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest/'
#karimnagar_progress_data = pandas.read_csv(path +'district_level/lab_usage_csvs/karimnagar_lab_usage_March31st2019.csv')
#rangareddy_progress_data = pandas.read_csv(path + 'district_level/lab_usage_csvs/rangareddy_lab_usage_March31st2019.csv')
#warangal_progress_data = pandas.read_csv(path + 'district_level/lab_usage_csvs/warangal_lab_usage_March31st2019.csv')
#mz_progress_data = pandas.concat([karimnagar_progress_data, rangareddy_progress_data, warangal_progress_data])
sirohi_progress_data = pandas.read_csv(path +'district_level/lab_usage_csvs/sirohi_lab_usage_March31st2019.csv')
jaipur_progress_data = pandas.read_csv(path + 'district_level/lab_usage_csvs/jaipur_lab_usage_March31st2019.csv')
mz_progress_data = pandas.concat([sirohi_progress_data, jaipur_progress_data])

mz_progress_data['date_created'] = pandas.to_datetime(mz_progress_data['timestamp'], format="%Y-%m-%d %H:%M:%S").apply(lambda x: str(x.date()))
mz_data = mz_progress_data

mz_data = mz_progress_data[mz_progress_data['month_slot'] == 'Jul-Dec-2018']

# time diff calculations for module logs
def get_time_diff(timestamps):
    unique_ts = timestamps.unique()
    if len(unique_ts) > 1:
        # If a given student has more than one timestamp in a day
        def f_ts(ts_string):
            return datetime.strptime(ts_string, "%Y-%m-%d %H:%M:%S")
        sorted_ts = sorted(np.array(list(map(f_ts, unique_ts))))
        time_slots = [(t2-t1).seconds//3600 for (t1, t2) in zip(sorted_ts, sorted_ts[1:])]
        #ts_min = min(sorted_ts)
        #ts_max = max(sorted_ts)
        timespent_hours = sum([each for each in time_slots if each >= 1])
        if timespent_hours >= 1:
            return timespent_hours
        else:
            # If the time difference between records is less than an hour
            return 0.5
    else:
        # If a given student has only one timestamp in a day
        return 0.5

modules_domain_map = {"e" : ["[u'English Beginner']", "[u'English Elementary']"],
                      "m" : ["[u'Geometric Reasoning Part I']", "[u'Geometric Reasoning Part II']", "[u'Linear Equations']",
                             "[u'Proportional Reasoning']"],
                      "s" : ["[u'Atomic Structure']", "[u'Sound']", "[u'Understanding Motion']", "[u'Basic Astronomy']",
                             "[u'Health and Disease']", "[u'Ecosystem']", "[u'Reflecting on Values']"]
                      }

domain_module_map = {each: key for key, value in modules_domain_map.items() for each in value}
mz_data_calc3 = mz_data.groupby(['school_server_code', 'date_created', 'user_id', 'module_name'])['timestamp'].apply(lambda x: get_time_diff(x)).reset_index()
mz_data_calc3 = mz_data_calc3.rename(columns= {'timestamp': 'timespent'})
mz_data_calc3['domain'] = mz_data_calc3['module_name']
mz_data_calc3 = mz_data_calc3.replace({'domain': domain_module_map})
mz_data_calc3_ts = mz_data_calc3.groupby(['school_server_code', 'date_created']).apply(lambda x: get_dom_ts(x)).reset_index()

# Other metric calculations 

# To get graphs for MZ implementation 2-pager
'''
Following is the notes from Rajashekhar:
Just for reference: Looking for couple of graphs -
One, number of hours spent across each of the months (X-axis) for each of the three subjects - line graphs;
two, number of hours spent for CLIx overall and each subject for each of the schools (stacked bar graph for CLIx overall
and individual graphs for the next 3 subjects - left to right)

Following are the inputs from 7th Aug, 2019 call with Implementation team:
Need to show in the graph these many schools were involved, in each of these schools these many students
are involved and these many days they are engaged. In these days, students together did so many modules(broken down by domain) and
spent on an average so much time on modules(broken down by domain).

Following is approach to represent information required:
We will have grouped bar chart with grouping based on month.
In each group of charts, we will have:
 1. number of schools engaged during that month
 2. number of days engaged (if possible wit breakdown based on server idle and non-idle)
 3. number of students engaged during that month (number of user logins)
 4. number of modules attempted with breakdown by domain.
    To calculate number of modules:
    1. we get the
 5. average time spent in a day on modules with breakdown by domain

'''
import numpy as np
from datetime import datetime
import pandas

path = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest/district_level/lab_usage_csvs'
jaipur_labusage_data = pandas.read_csv(path + '/jaipur_lab_usage_March31st2019_monthly.csv')
sirohi_labusage_data = pandas.read_csv(path + '/sirohi_lab_usage_March31st2019_monthly.csv')
mz_labusage_data = pandas.concat([jaipur_labusage_data, sirohi_labusage_data])
#warangal_labusage_data = pandas.read_csv(path + '/warangal_lab_usage_March31st2019_monthly.csv')
#karimnagar_labusage_data = pandas.read_csv(path + '/karimnagar_lab_usage_March31st2019_monthly.csv')
#rangareddy_labusage_data = pandas.read_csv(path + '/rangareddy_lab_usage_March31st2019_monthly.csv')
#mz_labusage_data = pandas.concat([warangal_labusage_data, karimnagar_labusage_data, rangareddy_labusage_data])

#mz_labusage_data = pandas.read_csv(path + '/chattisgarh_lab_usage_March31st2019_monthly.csv')

# To get number of schools engaged in different months
mz_impl_dframe1 = mz_labusage_data.groupby(['month_slot'])['school_server_code'].apply(lambda x: len(x.unique())).reset_index()
mz_impl_dframe1 = mz_impl_dframe1.rename(columns={'school_server_code': 'number_of_schools'})

# To get number of days with breakdown of idle and active days
def get_days(month_df):
    month_df['idle_days'] = month_df['days_server_wo_activity']
    month_df['active_days'] = month_df['tools_only_activity'] + month_df['module_only_activity'] + month_df['tool_with_module_activity']
    month_df_new = month_df.drop_duplicates(subset= ['school_server_code'])

    month_df_new['idle_days_avg'] = month_df_new['idle_days'].mean()
    month_df_new['active_days_avg'] = month_df_new['active_days'].mean()
    return month_df_new.drop_duplicates(subset = ['month_slot'])[['idle_days_avg', 'active_days_avg']]

mz_impl_dframe2 = mz_labusage_data.groupby(['month_slot']).apply(lambda x: get_days(x)).reset_index()
mz_impl_dframe2 = mz_impl_dframe2.drop(['level_1'], axis =1)

#To get number of students engaged in each month
mz_impl_dframe3_temp = mz_labusage_data.groupby(['month_slot', 'school_server_code'])['user_id'].apply(lambda x: len(x.unique())).reset_index()
mz_impl_dframe3 = mz_impl_dframe3_temp.groupby(['month_slot'])['user_id'].mean().reset_index()
mz_impl_dframe3 = mz_impl_dframe3.rename(columns={'user_id': 'num_of_students'})

# To get number of modules with breakdown by domain
modules_domain_map = {"e" : ["[u'English Beginner']", "[u'English Elementary']"],
                      "m" : ["[u'Geometric Reasoning Part I']", "[u'Geometric Reasoning Part II']", "[u'Linear Equations']",
                             "[u'Proportional Reasoning']"],
                      "s" : ["[u'Atomic Structure']", "[u'Sound']", "[u'Understanding Motion']", "[u'Basic Astronomy']",
                             "[u'Health and Disease']", "[u'Ecosystem']", "[u'Reflecting on Values']"]
                      }

domain_module_map = {each: key for key, value in modules_domain_map.items() for each in value}

def get_modules(month_df):
    # number of modules done in schools (averaged across schools) in a month
    month_df['domain'] = month_df['module_name']
    month_df = month_df.replace({'domain': domain_module_map})
    month_df = month_df[month_df['domain'] != "[u'Pre-CLIx Survey']"]
    month_df = month_df[month_df['domain'] != "[u'Post-CLIx Survey']"]
    # Number of unique modules touched during this month irrespective of students and dates on which it was touched
    month_df_new = month_df.groupby(['school_server_code', 'domain'])['module_name'].apply(lambda x: len(x.unique())).reset_index()
    # number of modules per domain averaged across schools
    month_df_final = month_df_new.groupby(['domain'])['module_name'].mean().reset_index()
    month_df_final = month_df_final.rename(columns={'module_name': 'num_of_mod_avg'})
    month_df_last = month_df_final.T
    month_df_last.columns = [each + '_num_mod_avg' for each in month_df_last.iloc[0]]
    return month_df_last.drop(['domain'], axis = 0)

def get_modules_daily(month_df):
    month_df['domain'] = month_df['module_name']
    month_df = month_df.replace({'domain': domain_module_map})
    month_df = month_df[month_df['domain'] != "[u'Pre-CLIx Survey']"]
    month_df = month_df[month_df['domain'] != "[u'Post-CLIx Survey']"]
    # Number of unique modules touched by a student in a day during this month
    # To get number of modules in a domain in a day by a student.now module_name is the number of modules in corresponding domain in a corresponding day.
    month_df_new = month_df.groupby(['school_server_code', 'user_id', 'date_created', 'domain'])['module_name'].apply(lambda x: len(x.unique())).reset_index()
    month_df_dom = month_df_new.groupby(['school_server_code', 'date_created', 'domain'])['module_name'].mean().reset_index()
    month_df_final = month_df_dom.groupby(['school_server_code', 'domain'])['module_name'].mean().reset_index()
    # number of modules per domain averaged across schools
    month_df_final1 = month_df_final.groupby(['domain'])['module_name'].mean().reset_index()
    month_df_final1 = month_df_final1.rename(columns={'module_name': 'num_of_mod_perday_avg'})
    month_df_last = month_df_final1.T
    month_df_last.columns = [each + '_num_mod_perday_avg' for each in month_df_last.iloc[0]]
    return month_df_last.drop(['domain'], axis = 0)

mz_impl_dframe4 = mz_labusage_data.groupby(['month_slot']).apply(lambda x: get_modules(x)).reset_index()
mz_impl_dframe4 = mz_impl_dframe4.drop(['level_1'], axis = 1)

mz_impl_dframe6 = mz_labusage_data.groupby(['month_slot']).apply(lambda x: get_modules_daily(x)).reset_index()
mz_impl_dframe6 = mz_impl_dframe6.drop(['level_1'], axis = 1)

# To get time spent on modules (broken down by domain) by students in a day averaged across schools
modules_domain_map = {"e" : ["[u'English Beginner']", "[u'English Elementary']"],
                      "m" : ["[u'Geometric Reasoning Part I']", "[u'Geometric Reasoning Part II']", "[u'Linear Equations']",
                             "[u'Proportional Reasoning']"],
                      "s" : ["[u'Atomic Structure']", "[u'Sound']", "[u'Understanding Motion']", "[u'Basic Astronomy']",
                             "[u'Health and Disease']", "[u'Ecosystem']", "[u'Reflecting on Values']"]
                      }

domain_module_map = {each: key for key, value in modules_domain_map.items() for each in value}
mz_data_calc3_ts['month_slot'] = mz_data_calc3_ts['date_created'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").strftime("%b-%Y"))

mz_impl_dframe5_temp  = mz_data_calc3_ts.groupby(['month_slot', 'domain'])['timespent_dom'].mean().reset_index()

mz_impl_dframe5 = mz_impl_dframe5_temp.pivot(index='month_slot', columns='domain', values='timespent_dom')
mz_impl_dframe5.columns = [each + '_ts_dom' for each in mz_impl_dframe5.columns]
#mz_impl_dframe5['month_slot'] = mz_impl_dframe5.index
mz_impl_dframe5 = mz_impl_dframe5.reset_index()

mz_impl_dframe_temp = mz_impl_dframe1.merge(mz_impl_dframe2, left_on = 'month_slot', right_on = 'month_slot').merge(mz_impl_dframe3, left_on='month_slot', right_on = 'month_slot')
mz_impl_dframe_temp1 = mz_impl_dframe_temp.merge(mz_impl_dframe4, left_on = 'month_slot', right_on='month_slot').merge(mz_impl_dframe5, left_on='month_slot', right_on='month_slot')
mz_impl_dframe = mz_impl_dframe_temp1.merge(mz_impl_dframe6, left_on = 'month_slot', right_on='month_slot')


mz_impl_dframe['month_slot_dt'] = mz_impl_dframe['month_slot'].apply(lambda x: datetime.strptime(x, "%b-%Y"))
mz_impl_dframe = mz_impl_dframe.sort_values(by=['month_slot_dt'])
mz_impl_dframe.to_csv(path + '/rj_impl_metrics_new.csv')


