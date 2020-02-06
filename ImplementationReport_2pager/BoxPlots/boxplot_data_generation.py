#To get avg lessons visited using simple averaging calculations
import pandas
from datetime import datetime

path = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest/district_level/lab_usage_csvs'
#jaipur_progress_data = pandas.read_csv(path +'/jaipur_lab_usage_March31st2019_monthly.csv')
#sirohi_progress_data = pandas.read_csv(path +'/sirohi_lab_usage_March31st2019_monthly.csv')
#rj_progress_data =  pandas.concat([jaipur_progress_data, sirohi_progress_data])

warangal_progress_data = pandas.read_csv(path +'/warangal_lab_usage_March31st2019_monthly.csv')
rangareddy_progress_data = pandas.read_csv(path +'/rangareddy_lab_usage_March31st2019_monthly.csv')
karimnagar_progress_data = pandas.read_csv(path +'/karimnagar_lab_usage_March31st2019_monthly.csv')
tg_progress_data =  pandas.concat([warangal_progress_data, rangareddy_progress_data, karimnagar_progress_data])

#mz_labusage_data = pandas.read_csv(path + '/mizoram_lab_usage_March31st2019_new.csv')
#mz_labusage_data = rj_progress_data
mz_labusage_data = tg_progress_data
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

mz_lessons_box_df = get_lessons_range(mz_labusage_data)
mz_lessons_box_df.groupby(['domain'])['lessons_visited'].mean()
mz_lessons_box_df.groupby(['domain'])['user_id'].apply(lambda x: len(x.unique()))
len(mz_labusage_data['school_server_code'].unique())



mz_lessons_box_df['month_slot_dt'] = mz_lessons_box_df['month_slot'].apply(lambda x: datetime.strptime(x, "%b-%Y"))
mz_lessons_box_df = mz_lessons_box_df.sort_values(by=['month_slot_dt'])
mz_lessons_box_df.to_csv(path + '/mz_lessons_boxplot_data1.csv')
