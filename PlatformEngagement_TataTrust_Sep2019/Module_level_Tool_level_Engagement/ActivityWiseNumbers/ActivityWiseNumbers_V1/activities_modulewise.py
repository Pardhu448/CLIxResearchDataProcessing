# To get activity level information for modules engagement
import pandas
path_modules = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest/district_level/lab_usage_csvs'
path_dest = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest/platform_engagement_schoollevel/modulewise_toolwise'
path_tools = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest/state_level/state_tools_data'

#Rajasthan
jaipur_labusage_data = pandas.read_csv(path_modules + '/jaipur_lab_usage_March31st2019_monthly.csv')
sirohi_labusage_data = pandas.read_csv(path_modules + '/sirohi_lab_usage_March31st2019_monthly.csv')
labusage_data = pandas.concat([jaipur_labusage_data, sirohi_labusage_data])
#Telangana
#warangal_labusage_data = pandas.read_csv(path_modules + '/warangal_lab_usage_March31st2019_monthly.csv')
#karimnagar_labusage_data = pandas.read_csv(path_modules + '/karimnagar_lab_usage_March31st2019_monthly.csv')
#rangareddy_labusage_data = pandas.read_csv(path_modules + '/rangareddy_lab_usage_March31st2019_monthly.csv')
#labusage_data = pandas.concat([warangal_labusage_data, karimnagar_labusage_data, rangareddy_labusage_data])

#chattisgarh_labusage_data = pandas.read_csv(path_modules + '/chattisgarh_lab_usage_March31st2019_monthly.csv')
#labusage_data = chattisgarh_labusage_data
#mizoram_labusage_data = pandas.read_csv(path_modules + '/mizoram_lab_usage_March31st2019_monthly.csv')
#labusage_data = mizoram_labusage_data
#labusage_data = pandas.concat([jaipur_labusage_data, sirohi_labusage_data, warangal_labusage_data, karimnagar_labusage_data,
#                               rangareddy_labusage_data, chattisgarh_labusage_data, mizoram_labusage_data])

# Metrics for modules
labusage_data = labusage_data[labusage_data['module_name'] != "[u'Pre-CLIx Survey']"]
labusage_data = labusage_data[labusage_data['module_name'] != "[u'Post-CLIx Survey']"]

def get_module_metrics(df):


    #df_lessons = df.groupby(['user_id', 'unit_name'])['lessons_visited'].max().reset_index()
    df_activities = df.groupby(['user_id', 'unit_name'])['activities_visited'].max().reset_index()

    #df_lessons = df_lessons[df_lessons['lessons_visited'] != 0]
    df_activities = df_activities[df_activities['activities_visited'] != 0]

    #df_lessons_1 = df_lessons.groupby(['user_id'])['lessons_visited'].sum().reset_index()
    df_activities_1 = df_activities.groupby(['user_id'])['activities_visited'].sum().reset_index()

    #df_lessons_total = df.groupby(['user_id', 'unit_name'])['total_lessons'].max().reset_index()
    #df_lessons_2 =  df_lessons_total.groupby(['user_id'])['total_lessons'].sum().reset_index()
    df_activities_total = df.groupby(['user_id', 'unit_name'])['total_activities'].max().reset_index()
    df_activities_2 = df_activities_total.groupby(['user_id'])['total_activities'].sum().reset_index()

    #total_users = len(df_lessons_1['user_id'].unique())
    #lessons_visited = df_lessons_1['lessons_visited'].sum()
    #lessons_per_user = lessons_visited/total_users
    total_users = len(df_activities_1['user_id'].unique())
    activities_visited = df_activities_1['activities_visited'].sum()
    activities_per_user = activities_visited / total_users

    # To get other statistics for lessons visited
    #lessons_sd = df_lessons_1['lessons_visited'].std()
    #lessons_median = df_lessons_1['lessons_visited'].median()
    #lessons_mode = df_lessons_1['lessons_visited'][df_lessons_1['lessons_visited'] != 0].mode().values[0]

    activities_sd = df_activities_1['activities_visited'].std()
    activities_median = df_activities_1['activities_visited'].median()
    activities_mode = df_activities_1['activities_visited'][df_activities_1['activities_visited'] != 0].mode().values[0]

    #total_lessons = df_lessons_2['total_lessons'].unique()[0]
    #num_schools = len(df['school_code'].unique())
    total_activities = df_activities_2['total_activities'].unique()[0]
    num_schools = len(df['school_code'].unique())

    #return pandas.DataFrame({'users': [total_users], 'lessons_per_user': [lessons_per_user],
    #                         'lessons_sd': [lessons_sd], 'lessons_median': [lessons_median],
    #                         'lessons_mode': [lessons_mode],
    #                         'total_lessons': [total_lessons],
    #                         'num_schools': [num_schools]})
    return pandas.DataFrame({'users': [total_users], 'activities_per_user': [activities_per_user],
                             'activities_sd': [activities_sd], 'activities_median': [activities_median],
                             'activities_mode': [activities_mode],
                             'num_schools': [num_schools]})

modulewise_schools = labusage_data.groupby(['module_name']).apply(lambda x: get_module_metrics(x)).reset_index()
modulewise_schools['module_name'] = modulewise_schools['module_name'].apply(lambda x: x.split("'")[1])

#modulewise_schools.to_csv(path_dest + '/mz_modules_data_V1.csv')
#modulewise_schools.to_csv(path_dest + '/rj_modules_activities_data_V1.csv')
#modulewise_schools.to_csv(path_dest + '/tg_modules_activities_data_V1.csv')
#modulewise_schools.to_csv(path_dest + '/ct_modules_activities_data_V1.csv')
modulewise_schools.to_csv(path_dest + '/AllStates_modules_activities_data_V1.csv')

