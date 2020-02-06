import pandas
path_modules = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest/district_level/lab_usage_csvs'
path_dest = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest/platform_engagement_schoollevel/modulewise_toolwise'
path_tools = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest/state_level/state_tools_data'

#Rajasthan
#jaipur_labusage_data = pandas.read_csv(path_modules + '/jaipur_lab_usage_March31st2019_monthly.csv')
#sirohi_labusage_data = pandas.read_csv(path_modules + '/sirohi_lab_usage_March31st2019_monthly.csv')
#labusage_data = pandas.concat([jaipur_labusage_data, sirohi_labusage_data])
#Telangana
#warangal_labusage_data = pandas.read_csv(path_modules + '/warangal_lab_usage_March31st2019_monthly.csv')
#karimnagar_labusage_data = pandas.read_csv(path_modules + '/karimnagar_lab_usage_March31st2019_monthly.csv')
#rangareddy_labusage_data = pandas.read_csv(path_modules + '/rangareddy_lab_usage_March31st2019_monthly.csv')
#labusage_data = pandas.concat([warangal_labusage_data, karimnagar_labusage_data, rangareddy_labusage_data])

#chattisgarh_labusage_data = pandas.read_csv(path_modules + '/chattisgarh_lab_usage_March31st2019_monthly.csv')
#labusage_data = chattisgarh_labusage_data
mizoram_labusage_data = pandas.read_csv(path_modules + '/mizoram_lab_usage_March31st2019_monthly.csv')
labusage_data = mizoram_labusage_data
#labusage_data = pandas.concat([jaipur_labusage_data, sirohi_labusage_data, warangal_labusage_data, karimnagar_labusage_data,
#                               rangareddy_labusage_data, chattisgarh_labusage_data, mizoram_labusage_data])

# Metrics for modules
labusage_data = labusage_data[labusage_data['module_name'] != "[u'Pre-CLIx Survey']"]
labusage_data = labusage_data[labusage_data['module_name'] != "[u'Post-CLIx Survey']"]

def get_module_metrics(df):

    df_lessons = df.groupby(['user_id', 'unit_name'])['lessons_visited'].max().reset_index()
    df_lessons = df_lessons[df_lessons['lessons_visited'] != 0]
    df_lessons_1 = df_lessons.groupby(['user_id'])['lessons_visited'].sum().reset_index()

    df_lessons_total = df.groupby(['user_id', 'unit_name'])['total_lessons'].max().reset_index()
    df_lessons_2 =  df_lessons_total.groupby(['user_id'])['total_lessons'].sum().reset_index()

    total_users = len(df_lessons_1['user_id'].unique())
    lessons_visited = df_lessons_1['lessons_visited'].sum()
    lessons_per_user = lessons_visited/total_users
    # TO get other statistics for lessons visited
    lessons_sd = df_lessons_1['lessons_visited'].std()
    lessons_median = df_lessons_1['lessons_visited'].median()
    lessons_mode = df_lessons_1['lessons_visited'][df_lessons_1['lessons_visited'] != 0].mode().values[0]

    total_lessons = df_lessons_2['total_lessons'].unique()[0]
    num_schools = len(df['school_code'].unique())
    return pandas.DataFrame({'users': [total_users], 'lessons_per_user': [lessons_per_user],
                             'lessons_sd': [lessons_sd], 'lessons_median': [lessons_median],
                             'lessons_mode': [lessons_mode],
                             'total_lessons': [total_lessons],
                             'num_schools': [num_schools]})

modulewise_schools = labusage_data.groupby(['module_name']).apply(lambda x: get_module_metrics(x)).reset_index()
modulewise_schools['module_name'] = modulewise_schools['module_name'].apply(lambda x: x.split("'")[1])

modulewise_schools.to_csv(path_dest + '/mz_modules_data_V1.csv')

# To get metrics at individual tools level
tg_state_tools = pandas.read_csv(path_tools + "/tg_metrics_tools_March31st2019.csv")
rj_state_tools = pandas.read_csv(path_tools + "/rj_metrics_tools_March31st2019.csv")
ct_state_tools = pandas.read_csv(path_tools + "/ct_metrics_tools_March31st2019.csv")
mz_state_tools = pandas.read_csv(path_tools + "/mz_metrics_tools_March31st2019.csv")

state_tools_data = pandas.concat([tg_state_tools, rj_state_tools, ct_state_tools, mz_state_tools])
start_date = "2018-07-01"
end_date = "2018-12-01"
state_tools_data['createdat_start'] = pandas.to_datetime(state_tools_data['createdat_start'], format="%Y-%m-%d %H:%M:%S")
state_tools_data['createdat_end'] = pandas.to_datetime(state_tools_data['createdat_end'], format="%Y-%m-%d %H:%M:%S")
#spurious_date_json_files = cumul_data[cumul_data['date_created'] > "2019-01-31"]
state_tools_data = state_tools_data[state_tools_data['createdat_end'] >= pandas.to_datetime(start_date, format="%Y-%m-%d")]
state_tools_data = state_tools_data[state_tools_data['createdat_end'] <= pandas.to_datetime(end_date, format="%Y-%m-%d")]

def get_tool_metrics(df):

    if df['tool_name'].unique()[0] == 'policesquad':
        df = df[df['time_spent'] <= 60]

    df_timespent = df.groupby(['user_id', 'session_id'])['time_spent'].apply(lambda x: x.unique()[0]).reset_index()
    num_anon_users = len(df_timespent[df_timespent['user_id'] == 0]) - 1
    df_timespent_1 = df_timespent.groupby(['user_id'])['time_spent'].sum().reset_index()

    total_users = len(df_timespent_1['user_id'].unique()) + num_anon_users
    total_time_spent = df_timespent_1['time_spent'].sum()
    timespent_per_user = total_time_spent/total_users
    num_schools = len(df['school_server_code'].unique())
    return pandas.DataFrame({'users': [total_users], 'timespent_per_user': [timespent_per_user],
                             'num_schools': [num_schools]})

toolwise_schools = state_tools_data.groupby(['tool_name']).apply(lambda x: get_tool_metrics(x)).reset_index()
toolwise_schools['tool_name'] = toolwise_schools['tool_name'].apply(lambda x: x.replace("_" , " "))
toolwise_schools.to_csv(path_dest + '/allStates_tools_data.csv')
