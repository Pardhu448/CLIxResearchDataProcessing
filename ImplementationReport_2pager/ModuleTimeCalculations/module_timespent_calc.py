
'''
# Time spent on module at the granularity of 1 hr (minimum we can measure is 1hr), as the records are generated hourly.
# Also our method depends on the possibility of multiple records in a day for a student on a given module or across all modules.
# To calculate the time spent on modules by students in a school, we have two approaches :

# 1. Calculate time difference between timestamps of a student in a given day in a given module. Sum up the time spent by a student on
# different modules. Then take average of time spent by all students in a day.

# 2. Calculate time difference between timestamps (logs) in a given day across all students and modules(which students have attempted that day).
# This gives the students engagement time from the point of view of system.
'''
#tools_domain_map = {
#        'e': [],
#        'm': ['ice', 'factorisation', 'coins_puzzle','rationpatterns', 'food_sharing_tool', 'ages_puzzle', 'policesquad'],
#        's': ['astroamer_element_hunt_activity', 'astroamer_moon_track', 'astroamer_planet_trek_activity']
#    }

import numpy as np
from datetime import datetime
import pandas

path = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest/'
mz_progress_data = pandas.read_csv(path +'state_level/lab_usage_csvs/mz_progress_lab_func_March31st2019_tv.csv')
mz_data = mz_progress_data[mz_progress_data['month_slot'] == 'Jul-Dec-2018']

def get_time_diff(timestamps):
    unique_ts = timestamps.unique()
    if len(unique_ts) > 1:
        # If a given student has more than one timestamp in a day
        def f_ts(ts_string):
            return datetime.strptime(ts_string, "%Y-%m-%d %H:%M:%S")
        sorted_ts = sorted(np.array(list(map(f_ts, unique_ts))))
        ts_min = min(sorted_ts)
        ts_max = max(sorted_ts)
        timespent_hours = (ts_max - ts_min).seconds//3600
        if timespent_hours >= 1:
            return timespent_hours
        else:
            # If the time difference between records is less than an hour
            return 0.5
    else:
        # If a given student has only one timestamp in a day
        return 0.5

def get_stud_ts(stud_df):
    stud_df['timespent_avg'] = stud_df.groupby(['school_server_code', 'date_created', 'user_id'])['timespent'].apply(lambda x: sum(x)).mean()
    return stud_df

def get_dom_ts(domain_df):
    domain_df_ts = domain_df.groupby(['school_server_code', 'date_created', 'user_id', 'domain'])['timespent'].apply(sum).reset_index()
    domain_df_ts = domain_df_ts.rename(columns= {'timespent': 'timespent_dom'})
    domain_df_ts = domain_df_ts[domain_df_ts['domain'] != "[u'Pre-CLIx Survey']"]
    domain_df_ts = domain_df_ts[domain_df_ts['domain'] != "[u'Post-CLIx Survey']"]
    domain_df_final = domain_df_ts.groupby(['school_server_code', 'date_created', 'domain'])['timespent_dom'].mean().reset_index()
    return domain_df_final[['domain', 'timespent_dom']]

mz_data_calc1 = mz_data.groupby(['school_server_code', 'date_created', 'user_id', 'module_name'])['timestamp'].apply(lambda x: get_time_diff(x)).reset_index()
mz_data_calc1 = mz_data_calc1.rename(columns= {'timestamp': 'timespent'})
mz_data_calc1_ts = mz_data_calc1.groupby(['school_server_code', 'date_created']).apply(lambda x: get_stud_ts(x)).reset_index()
mz_data_calc1_ts = mz_data_calc1_ts.drop_duplicates(subset=['school_server_code', 'date_created'])[['school_server_code', 'date_created', 'timespent']]
mz_data_calc1_ts.to_csv(path + 'mz_module_timespent_calc1.csv')


mz_data_calc2 = mz_data.groupby(['school_server_code', 'date_created'])['timestamp'].apply(lambda x: get_time_diff(x)).reset_index()
mz_data_calc2_ts = mz_data_calc2.rename(columns= {'timestamp': 'timespent'})
mz_data_calc2_ts.to_csv(path + 'mz_module_timespent_calc2.csv')

#Module engagement time - subjectwise
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

mz_data_calc3_ts.drop(['level_2'], axis=1).to_csv(path + 'mz_module_timespent_calc3.csv')

########################################################################################################################
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
 2. number of days engaged with breakdown based on server idle and non-idle
 3. number of students engaged during that month
 4. number of modules used with breakdown by domain (Shekar suggested to use average number of lessons done by students across all schools out of total 
 lessons across all schools)
 5. average time spent in a day on modules with breakdown by domain

'''
import numpy as np
from datetime import datetime
import pandas

path = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest/'
mz_labusage_data = pandas.read_csv(path + '/mizoram_lab_usage_March31st2019_new.csv')

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
mz_impl_dframe3 = mz_labusage_data.groupby(['month_slot'])['user_id'].apply(lambda x: len(x.unique())).reset_index()
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
    # Number of unique modules touched during this month
    month_df_new = month_df.groupby(['school_server_code', 'domain'])['module_name'].apply(lambda x: len(x.unique())).reset_index()
    # number of modules per domain averaged across schools
    month_df_final = month_df_new.groupby(['domain'])['module_name'].mean().reset_index()
    month_df_final = month_df_final.rename(columns={'module_name': 'num_of_mod_avg'})
    month_df_last = month_df_final.T
    month_df_last.columns = [each + '_num_mod_avg' for each in month_df_last.iloc[0]]
    return month_df_last.drop(['domain'], axis = 0)

mz_impl_dframe4 = mz_labusage_data.groupby(['month_slot']).apply(lambda x: get_modules(x)).reset_index()
mz_impl_dframe4 = mz_impl_dframe4.drop(['level_1'], axis = 1)

# To get time spent on modules (broken down by domain) by students in a day averaged across schools

# To get box plot representing range of number of lessons done by a typical student in the state irrespective of schools
# Variation of this range over months.
import pandas
from datetime import datetime

path = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest/'
mz_labusage_data = pandas.read_csv(path + '/mizoram_lab_usage_March31st2019_new.csv')
modules_domain_map = {"e" : ["[u'English Beginner']", "[u'English Elementary']"],
                      "m" : ["[u'Geometric Reasoning Part I']", "[u'Geometric Reasoning Part II']", "[u'Linear Equations']",
                             "[u'Proportional Reasoning']"],
                      "s" : ["[u'Atomic Structure']", "[u'Sound']", "[u'Understanding Motion']", "[u'Basic Astronomy']",
                             "[u'Health and Disease']", "[u'Ecosystem']", "[u'Reflecting on Values']"]
                      }

domain_module_map = {each: key for key, value in modules_domain_map.items() for each in value}

def get_lessons_range(month_df):
    month_df_lessons = month_df.groupby(['user_id', 'module_name'])['lessons_visited'].max().reset_index()

    month_df_lessons['domain'] = month_df_lessons['module_name']
    month_df_lessons = month_df_lessons.replace({'domain': domain_module_map})
    month_df_lessons = month_df_lessons[month_df_lessons['domain'] != "[u'Pre-CLIx Survey']"]
    month_df_lessons = month_df_lessons[month_df_lessons['domain'] != "[u'Post-CLIx Survey']"]

    month_df_lessons_temp = month_df_lessons.groupby(['user_id', 'domain'])['lessons_visited'].sum().reset_index()

    def get_range(lessons_df):
        lessons_df['max_lessons'] = lessons_df['lessons_visited'].max()
        lessons_df['min_lessons'] = lessons_df['lessons_visited'].min()
        lessons_df['average_lessons'] = lessons_df['lessons_visited'].mean()
        lessons_df['lower_quantile_lessons'] = lessons_df['lessons_visited'].quantile(0.25)
        lessons_df['upper_quantile_lessons'] = lessons_df['lessons_visited'].quantile(0.75)
        return lessons_df
    month_df_lessons_final = month_df_lessons_temp.groupby(['domain']).apply(lambda x: get_range(x)).reset_index()
    return month_df_lessons_final.drop_duplicates(subset=['domain']).drop(['index', 'user_id', 'lessons_visited'], axis=1)

mz_lessons_box_df = mz_labusage_data.groupby(['month_slot']).apply(lambda x: get_lessons_range(x)).reset_index()
mz_lessons_box_df['month_slot_dt'] = mz_lessons_box_df['month_slot'].apply(lambda x: datetime.strptime(x, "%b-%Y"))
mz_lessons_box_df = mz_lessons_box_df.sort_values(by=['month_slot_dt'])
mz_lessons_box_df.to_csv(path + '/mz_lessons_boxplot_data.csv')

# To get box plot representing range of number of lessons done by a typical student in the state irrespective of schools
# Variation of this range over months.
import pandas
from datetime import datetime

path = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest/'
mz_labusage_data = pandas.read_csv(path + '/mizoram_lab_usage_March31st2019_new.csv')
modules_domain_map = {"e" : ["[u'English Beginner']", "[u'English Elementary']"],
                      "m" : ["[u'Geometric Reasoning Part I']", "[u'Geometric Reasoning Part II']", "[u'Linear Equations']",
                             "[u'Proportional Reasoning']"],
                      "s" : ["[u'Atomic Structure']", "[u'Sound']", "[u'Understanding Motion']", "[u'Basic Astronomy']",
                             "[u'Health and Disease']", "[u'Ecosystem']", "[u'Reflecting on Values']"]
                      }

domain_module_map = {each: key for key, value in modules_domain_map.items() for each in value}

def get_lessons_range(month_df):
    month_df_lessons = month_df.groupby(['user_id', 'module_name'])['lessons_visited'].max().reset_index()

    month_df_lessons['domain'] = month_df_lessons['module_name']
    month_df_lessons = month_df_lessons.replace({'domain': domain_module_map})
    month_df_lessons = month_df_lessons[month_df_lessons['domain'] != "[u'Pre-CLIx Survey']"]
    month_df_lessons = month_df_lessons[month_df_lessons['domain'] != "[u'Post-CLIx Survey']"]

    month_df_lessons_final = month_df_lessons.groupby(['user_id', 'domain'])['lessons_visited'].sum().reset_index()

    return month_df_lessons_final[['domain', 'lessons_visited', 'user_id']]

mz_lessons_box_df = mz_labusage_data.groupby(['month_slot']).apply(lambda x: get_lessons_range(x)).reset_index()

mz_lessons_box_df['month_slot_dt'] = mz_lessons_box_df['month_slot'].apply(lambda x: datetime.strptime(x, "%b-%Y"))
mz_lessons_box_df = mz_lessons_box_df.sort_values(by=['month_slot_dt'])
mz_lessons_box_df.to_csv(path + '/mz_lessons_boxplot_data1.csv')




