'''
Code to get metrics using policesquad data for math team.
primarily, three metrics at school level are expected:
1. Total time spent per student in a school on GR1
2. Total time spent per student in a school on policesquad
3. Total time spent per student in a school on mission 1 of policesquad
4. Total time spent per student in a school on mission 2 of policesquad
5. Total time spent per student in a school on mission 3 of policesquad
6. Total time spent per student in a school on mission 4 of policesquad
7. Total lessons of GR1 per student in a school
'''
import os
import re
import json
from collections import defaultdict
import logging
import operator
import itertools
import pandas
from functools import reduce

def get_state_school_json(json_file_path):
    path_items = json_file_path.split('/')
    state_code = path_items[path_items.index('gstudio') - 2]
    school_server_code = path_items[path_items.index('gstudio') - 1]
    if not school_server_code:
        print('Couldnt fetch school_server_code for file - {}'.format(json_file_path))
        logging.debug('Couldnt fetch school_server_code for file - %s', json_file_path)
    return {'state_code': state_code, 'school_server_code': school_server_code}

def get_files_of_tool(path, tool):
    '''
    Gets all the filenames of particular pattern in a given directory name pattern (both given by regex)
    Use of nested tuples is to make it amenable for parallel processing in future if need be
    :param path: root path of data
    :param regex_file: regex to identify files say, .csv, .json etc
    :param regex_dir: regex to identify immediate parent directory of files
    :return: list of tuples - [(filename, file_attributes)]
    '''
    files_list = list()
    regex_file = r'.+(json)$'
    regex_dir = r'gstudio_tools_logs'
    regObj_file = re.compile(regex_file)
    regObj_dir = re.compile(regex_dir)
    regex_parent_dir = re.compile(r'gstudio')
    for root, dnames, fnames in os.walk(os.path.join(path)):
        for dname in dnames:
           if regex_parent_dir.match(dname):
                for d1name in os.listdir(os.path.join(root, dname)):
                    #If in gstudio folder do the following:
                    #Go to tools folder - to get actual json files for tools data
                    if regObj_dir.match(d1name):
                      gstudio_tools_log_folder_path =  os.path.join(root, dname, d1name)
                      for eachdir in os.listdir(gstudio_tools_log_folder_path):
                         tool_dir_path = os.path.join(root, dname, d1name, eachdir)
                         tool_dir_files = os.listdir(tool_dir_path)
                         if (not tool_dir_files):
                             logging.debug('No Tool log data in folder - %s', tool_dir_path)
                         else:
                             for eachfile in tool_dir_files:
                                 if regObj_file.match(eachfile):
                                  if eachfile.split('-')[1].split('.')[0] in tool:
                                   json_file_path = os.path.join(root, dname, d1name, eachdir, eachfile)
                                   state_code = get_state_school_json(json_file_path)['state_code']
                                   school_server_code = get_state_school_json(json_file_path)['school_server_code']
                                   files_list.append((state_code, (school_server_code, json_file_path)))
    return files_list

def get_all_school_names():
    return None

def get_files(path):
    '''
    Gets all the filenames of particular pattern in a given directory name pattern (both given by regex)
    Use of nested tuples is to make it amenable for parallel processing in future if need be
    :param path: root path of data
    :param regex_file: regex to identify files say, .csv, .json etc
    :param regex_dir: regex to identify immediate parent directory of files
    :return: list of tuples - [(filename, file_attributes)]
    '''
    files_list = list()
    regex_file = r'.+(json)$'
    regex_dir = r'gstudio_tools_logs'
    regObj_file = re.compile(regex_file)
    regObj_dir = re.compile(regex_dir)
    regex_parent_dir = re.compile(r'gstudio')

    for root, dnames, fnames in os.walk(os.path.join(path)):
        for dname in dnames:
            if regex_parent_dir.match(dname):
                for d1name in os.listdir(os.path.join(root, dname)):
                    # If in gstudio folder do the following:
                    # Go to tools folder - to get actual json files for tools data
                    if regObj_dir.match(d1name):
                        gstudio_tools_log_folder_path = os.path.join(root, dname, d1name)
                        for eachdir in os.listdir(gstudio_tools_log_folder_path):
                            tool_dir_path = os.path.join(root, dname, d1name, eachdir)
                            tool_dir_files = os.listdir(tool_dir_path)
                            if (not tool_dir_files):
                                logging.debug('No Tool log data in folder - %s', tool_dir_path)
                            else:
                                for eachfile in tool_dir_files:
                                    if regObj_file.match(eachfile):
                                        json_file_path = os.path.join(root, dname, d1name, eachdir, eachfile)
                                        state_code = get_state_school_json(json_file_path)['state_code']
                                        school_server_code = get_state_school_json(json_file_path)['school_server_code']
                                        files_list.append((state_code, (school_server_code, json_file_path)))
    return files_list


def parse_nested_dict(dict_nes, item_key):
    '''
    parses through a nested dictionary to tech value corresponding to 'item_key' key.
    :param dict_nes: nested dictionary
    :param item_key: item key we want value for
    :return:
    '''
    try:
        if item_key == 'sessionID':
            reg_Obj = re.compile(re.escape(item_key))
        else:
            reg_Obj = re.compile('(' + re.escape(item_key) + ')', re.IGNORECASE)

        key_match_list = [key for key in dict_nes.keys() if reg_Obj.match(key)]
        if key_match_list:
            return dict_nes[key_match_list[0]]
        else:
            for k, v in dict_nes.items():
                if isinstance(v, dict):
                    return parse_nested_dict(v, item_key)
                elif isinstance(v, list):
                    for each in v:
                        parse_nested_dict(each, item_key)
    except Exception as e:
        if item_key == 'sessionId':
            print("{0}: {1}".format(e, dict_nes))
            # logging.debug("{0}: {1}".format(e, dict_nes))
            raise Exception("{0}: {1}".format(e, dict_nes))
            return None
        else:
            raise Exception("{0}: {1}".format(e, dict_nes))


tools_domain_map = {
    'e': ['rationpatterns'],
    'm': ['ice', 'factorisation', 'coins_puzzle', 'food_sharing_tool', 'ages_puzzle', 'policesquad'],
    's': ['astroamer_element_hunt_activity', 'astroamer_moon_track', 'astroamer_planet_trek_activity']
}

def accumulator(files):
        '''
        Function to accumulate all the json file logs for a all states and school_ids in them
        If a user has logged into tools, it will reflect as a row in this data with a timestamp
        Also state level data are saved as csvs in current working directory
        Only following fields are extracted related to json files:
         1. created_at
         2. user_id
         3. tool_name
         4. school_server_code
         5. state_code
        :param files: nested list of tuples - (state_code, (school_code, tool_json_file))
        :return: dictionary of dictionaries - {'state': {'user_id': user_id, 'created_at': time, 'tool_name': tool_name}}
        '''
        final_data = dict()
        for state_code, all_state_files in itertools.groupby(files, operator.itemgetter(0)):
            schools_tool_dict = defaultdict(list)
            for school in [all_schools[1] for all_schools in all_state_files]:
                school_json_file = school[1]
                input_file = open(school_json_file, 'r')
                try:
                   json_input = json.load(input_file)
                except Exception as e:
                   logging.debug(e)
                   logging.debug('Some issue with loading file : %s', school_json_file)
                for items in json_input:
                   try:
                       schools_tool_dict['school_server_code'].append(school[0])
                       tool_name = school_json_file.split('/')[-1:][0].split('.')[0].split('-')[1]
                       tool_name = ''.join(c.lower() for c in tool_name if not c.isspace())
                       schools_tool_dict['tool_name'].append(tool_name)
                       normal_tools = ["food_sharing_tool", "ages_puzzle", "astroamer_element_hunt_activity", "coins_puzzle", "factorisation"]

                       if pandas.Series([tool_name]).isin(normal_tools)[0]:
                           schools_tool_dict['createdat_start'].append(None)
                           schools_tool_dict['createdat_end'].append(parse_nested_dict(items, 'create'))
                           schools_tool_dict['time_spent'].append(None)

                       elif tool_name == "ice":
                           schools_tool_dict['createdat_start'].append(parse_nested_dict(items, 'tool_startTime'))
                           schools_tool_dict['createdat_end'].append(parse_nested_dict(items, 'tool_endTime'))
                           schools_tool_dict['time_spent'].append(None)

                       elif tool_name == "policesquad":
                           schools_tool_dict['createdat_start'].append(None)
                           schools_tool_dict['createdat_end'].append(parse_nested_dict(items, 'create'))
                           schools_tool_dict['current_mission'].append(parse_nested_dict(items, 'CurrentMission'))
                           schools_tool_dict['time_spent'].append(parse_nested_dict(items, 'TimeSpentStage'))
                           #schools_tool_dict['appData'].append(parse_nested_dict(items, 'appData'))


                       elif tool_name == "astroamer_planet_trek_activity":
                           schools_tool_dict['createdat_start'].append(parse_nested_dict(items, 'tool_startTime'))
                           schools_tool_dict['createdat_end'].append(parse_nested_dict(items, 'tool_endTime'))
                           schools_tool_dict['time_spent'].append(None)

                       elif tool_name == "astroamer_moon_track":
                           schools_tool_dict['createdat_start'].append(parse_nested_dict(items, 'tool_startTime'))
                           schools_tool_dict['createdat_end'].append(parse_nested_dict(items, 'tool_endTime'))
                           schools_tool_dict['time_spent'].append(None)

                       elif tool_name == "rationpatterns":
                           start_time = parse_nested_dict(items, 'gameOverallStartTime')
                           mod_starttime = ' '.join([start_time.split(':')[0], ':'.join(start_time.split(':')[1:])])
                           end_time = parse_nested_dict(items, 'gameOverallEndTime')
                           mod_endtime = ' '.join([end_time.split(':')[0], ':'.join(end_time.split(':')[1:])])
                           schools_tool_dict['createdat_start'].append(mod_starttime)
                           schools_tool_dict['createdat_end'].append(mod_endtime)

                           schools_tool_dict['time_spent'].append(None)

                       schools_tool_dict['session_id'].append(parse_nested_dict(items, 'sessionId'))
                       #schools_tool_dict['session_id'].append(parse_nested_dict(items, 'user'))
                       schools_tool_dict['user_id'].append(school_json_file.split('/')[-1:][0].split('-')[0])
                   except Exception as e:
                       import pdb
                       pdb.set_trace()
                       logging.debug(e)
                       print(e)
                       print('There was some error fetching items from json. Have to debug in (accumulator) function')
                input_file.close()

            dframe = pandas.DataFrame.from_dict(schools_tool_dict, orient = 'index').transpose()
            dframe['createdat_start'] = pandas.to_datetime(dframe['createdat_start'], errors='ignore', format="%Y%m%d %H:%M:%S")
            dframe['createdat_end'] = pandas.to_datetime(dframe['createdat_end'], errors='ignore', format="%Y%m%d %H:%M:%S")

            tools_map = dict()
            for k, v in tools_domain_map.items():
                for each_v in v:
                    tools_map[each_v] = k

            dframe['tool_new'] = dframe['tool_name'].apply(lambda x: ''.join(c.lower() for c in x if not c.isspace()))
            dframe['tool_new'].replace(tools_map, inplace=True)
            dframe = dframe.rename(columns={'tool_new': 'domain'})

           # dframe_final = dframe.groupby(['tool_name',
           #                                'user_id']).apply(get_time_spent).reset_index().drop(['index'], axis =1)
            final_data[state_code] = dframe.to_dict()
        return final_data

def get_time_spent(df):
    '''
    This function estimates the time spent in minutes by a student in a day in a
    particular session.

    :param df:
    :return:
    '''
    df['date_created'] = df['createdat_end'].apply(lambda x: x.date())

    def timespent(x):

        normal_tools = ["food_sharing_tool", "ages_puzzle", "astroamer_element_hunt_activity",
                        "coins_puzzle", "factorisation"]
        rationpatt = x['tool_name'] == 'rationpatterns'
        ice = x['tool_name'] == 'ice'
        ast_moon = x['tool_name'] == 'astroamer_moon_track'
        ast_trek = x['tool_name'] == 'astroamer_planet_trek_activity'
        polic = x['tool_name'] == 'policesquad'
        normal = x['tool_name'].isin(normal_tools)

        if not x[normal].empty:
           x.loc[normal, ['time_spent']] = ((max(x[normal]['createdat_end']) - min(x[normal]['createdat_end'])).seconds//60)%60

        if not x[ast_trek].empty:
           ts_asttrek =  reduce(lambda x,y: x + y + datetime.timedelta(0), x[ast_trek]['createdat_end'] - x[ast_trek]['createdat_start'])
           x.loc[ast_trek, ['time_spent']] = (ts_asttrek.seconds//60)%60

        if not x[ast_moon].empty:
           ts_moon = reduce(lambda x,y: x + y + datetime.timedelta(0), x[ast_moon]['createdat_end'] - x[ast_moon]['createdat_start'])
           x.loc[ast_moon, ['time_spent']] =  (ts_moon.seconds//60)%60

        if not x[ice].empty:
           ts_ice =  reduce(lambda x,y: x + y + datetime.timedelta(0), x[ice]['createdat_end'] - x[ice]['createdat_start'])
           x.loc[ice, ['time_spent']] = (ts_ice.seconds//60)%60

        if not x[rationpatt].empty:
           ts_ratio = reduce(lambda x,y: x+y+datetime.timedelta(0), x[rationpatt]['createdat_end'] - x[rationpatt]['createdat_start'])
           x.loc[rationpatt, ['time_spent']] = (ts_ratio.seconds//60)%60

        if not x[polic].empty:
            def get_minute(x):
                try:
                    ts = x['time_spent']
                    if (len(ts.split(':')) > 1):
                       hh_minut = int(ts.split(':')[0]) * 60
                       minut_minut = int(ts.split(':')[1])
                       sec_minut = float(ts.split(':')[2]) / 60
                    else:
                       hh_minut = int(ts) * 60
                       minut_minut = 0
                       sec_minut = 0
                except Exception as e:
                    import pdb
                    pdb.set_trace()

                return hh_minut + minut_minut + sec_minut

            """
            def splittime(x):
               ts = x['time_spent']
               hh_minut = int(ts.split(':')[0]) * 60
               minut_minut = int(ts.split(':')[1])
               sec_minut = float(ts.split(':')[2])/60
               return hh_minut + minut_minut + sec_minut
            """


            x.loc[polic, ['time_spent']] = x.loc[polic, ['time_spent']].apply(lambda x: get_minute(x), axis=1)
        return x

    df = df.groupby(['user_id', 'date_created', 'session_id']).apply(lambda x: timespent(x)).reset_index(level=None, drop=True)
    return df
def get_metrics_in_daterange(state_df, date_range):

    start_date = date_range[0]
    end_date = date_range[1]
    try:
      state_df['state_code'] = state_df['school_server_code'].apply(lambda x: x.split('-')[1][:2])
      state_df['createdat_start'] = pandas.to_datetime(state_df['createdat_start'], format="%Y-%m-%d %H:%M:%S")
      state_df['createdat_end'] = pandas.to_datetime(state_df['createdat_end'], format="%Y-%m-%d %H:%M:%S")
      #spurious_date_json_files = cumul_data[cumul_data['date_created'] > "2019-01-31"]
      state_df = state_df[state_df['createdat_end'] >= pandas.to_datetime(start_date, format="%Y-%m-%d")]
      state_df = state_df[state_df['createdat_end'] <= pandas.to_datetime(end_date, format="%Y-%m-%d")]
      state_code = state_df['school_server_code'].apply(lambda x: x.split('-')[1][:2]).unique()[0]
    except Exception as e:
      print(e)
      import pdb
      pdb.set_trace()

    return get_indicators_tools_data(state_df, state_code)

def get_clean_data(state_df):

    def get_rid_of_spurious_dates(date_entry):
        # Modifying '2019-0-12' kind of dates to '2019-01-12'
        if not pandas.isnull(date_entry):
            if '2019-0-' in date_entry:
                dstring = date_entry.split('-')
                dstring[1] = '01'
                return '-'.join(dstring)
            else:
                return date_entry
        else:
            return date_entry

    state_df['createdat_start'] = state_df['createdat_start'].apply(lambda x: get_rid_of_spurious_dates(x))
    state_df['createdat_end'] = state_df['createdat_end'].apply(lambda x: get_rid_of_spurious_dates(x))
    return state_df

working_dir = os.getcwd()
logging.basicConfig(filename=working_dir + '/session_outputs.log', level=logging.DEBUG)
#parent_directory = input('Please enter any top level directory to fetch tools data from:\n')

parent_directory = '/home/parthae/Documents/Projects/TISS_Git/projects/CLIxData/syncthing_Aug2019'
tool_name = ['policesquad']
# Tools related json files - indexed by state and school ids
tools_json_files = get_files_of_tool(parent_directory, tool = tool_name)
tools_all_states_data = accumulator(tools_json_files)

dest_path = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest/policesquad_math_team/'
for each_state in ['ct']:
   dframe_final = pandas.DataFrame(tools_all_states_data[each_state])
   cols_final = list(dframe_final.columns)
   #cols_final.remove('appData')
   dframe_final = dframe_final.drop_duplicates(subset=cols_final)
   dframe_final.to_csv(dest_path + each_state + '_json_tools_data_Aug2019.csv', mode='w', index=False)

ct_tools_data = pandas.read_csv(dest_path + 'ct_json_tools_data_Aug2019.csv')
# To Extract relevant information from policesquad student logs
def get_indicators_tools_data(state_dframe, state_code):

   def get_mission_time(df):
       df = df[df['time_spent'] <= 60]
       df_new =  df.groupby(['current_mission', 'user_id'])['time_spent'].sum().reset_index()

       df_mission = df_new.groupby(['current_mission'])['time_spent'].sum().reset_index()
       df_mission['current_mission'] = df_mission['current_mission'].apply(lambda x: '_ts_'.join(x.split(' ')))

       df_mission1= df_new.groupby(['current_mission'])['user_id'].apply(lambda x: len(x.unique())).reset_index()
       df_mission1['current_mission'] = df_mission1['current_mission'].apply(lambda x: '_num_users_'.join(x.split(' ')))

       ts_df = df_mission.set_index('current_mission').T.reset_index().drop('index', axis=1)
       num_users_df = df_mission1.set_index('current_mission').T.reset_index().drop('index', axis=1)
       final_df = pandas.concat([ts_df, num_users_df], axis=1, join='inner')
       final_df['num_users_pq'] = len(df['user_id'].unique())

       return final_df

   time_spent_dframe = state_dframe.groupby(['school_server_code']).apply(lambda x: get_time_spent(x)).reset_index(level=None, drop=True)
   timespent_per_mission = time_spent_dframe.groupby(['school_server_code']).apply(lambda x: get_mission_time(x)).reset_index()
   timespent_per_mission = timespent_per_mission.fillna(0)
   timespent_per_mission.to_csv(dest_path + state_code + "_policesquad_metrics.csv")
   return (timespent_per_mission,  state_code)

final_tools_data_state = {}
date_range = ["2018-07-01", "2018-12-31"]
# Manually removed spurious dates from rj (rj_json_tools_March31st2019.csv)
# These were date recorded as '2019-0-1' etc. There were around 10 logs of them
list_of_state_data = [ct_tools_data]
for each_state in list_of_state_data:
    #each_state = get_clean_data(each_state)
    state_output = get_metrics_in_daterange(each_state, date_range)
    final_tools_data_state[state_output[1]] = state_output[0]

# To get metrics corresponding to GR1 module:
from datetime import datetime
import numpy as np

path_modules = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest/district_level/lab_usage_csvs'
ct_modules_data = pandas.read_csv(path_modules + '/chattisgarh_lab_usage_March31st2019_monthly.csv')

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
            return 0.25
    else:
        # If a given student has only one timestamp in a day
        return 0.25

def get_ts_metrics(df):
    df['num_users_gr1'] = len(df['user_id'].unique())
    df['ts_module'] = df['timespent'].sum()
    return df.drop(['user_id', 'timespent'], axis=1).drop_duplicates()

ct_modules_new = ct_modules_data.groupby(['school_server_code', 'date_created', 'user_id', 'module_name'])['timestamp'].apply(lambda x: get_time_diff(x)).reset_index().rename(columns= {'timestamp': 'timespent'})
ct_gr1_ts_df1 = ct_modules_new.groupby(['school_server_code', 'module_name', 'user_id'])['timespent'].sum().reset_index()
ct_gr1_ts_df = ct_gr1_ts_df1.groupby(['school_server_code', 'module_name']).apply(lambda x: get_ts_metrics(x)).reset_index(level=None, drop=True)

ct_gr1_ts_df = ct_gr1_ts_df[ct_gr1_ts_df['module_name'] == "[u'Geometric Reasoning Part I']"]

def get_num_lessons(school_df, module_name):
    try:
        # To get total module days in a school
        lessons_visited = school_df.groupby(['user_id', 'module_name', 'unit_name'])['lessons_visited'].max().reset_index()
        school_df_lessons = lessons_visited.groupby(['user_id', 'module_name'])['lessons_visited'].sum().reset_index()

        #total_lessons = school_df.groupby(['user_id', 'module_name', 'unit_name'])['total_lessons'].apply(lambda x: x.unique()[0]).reset_index()
        #school_df_total_lessons = total_lessons.groupby(['user_id', 'module_name'])['total_lessons'].sum().reset_index()
        #school_df_final = school_df_total_lessons.merge(school_df_lessons, left_on=['user_id', 'module_name'], right_on=['user_id', 'module_name'])
        school_df_final = school_df_lessons
        school_df_final = school_df_final[school_df_final['module_name'] == module_name]
        school_df_final['lessons_visited'] = sum(school_df_final['lessons_visited'])

        school_df_final['num_users_gr1'] = len(school_df_final['user_id'].unique())
        #school_df_new['num_lessons_peruser'] = school_df_new['lessons_visited'].sum()/len(school_df_new['user_id'].unique())
        df_final = school_df_final.drop_duplicates(subset=['module_name'])[school_df_final.columns.drop('user_id')]

    except Exception as e:
        print(e)
        import pdb
        pdb.set_trace()

    return df_final
module_name = "[u'Geometric Reasoning Part I']"
ct_modules_lessons = ct_modules_data.groupby(['school_server_code']).apply(lambda x: get_num_lessons(x, module_name)).reset_index().drop(['level_1'], axis=1)

ct_modules_final = ct_modules_lessons.merge(ct_gr1_ts_df, left_on=['school_server_code', 'module_name', 'num_users_gr1'], right_on=['school_server_code', 'module_name', 'num_users_gr1'])
ct_modules_final.to_csv(dest_path  + 'ct_gr1_metrics.csv')

############################################################################################################################






