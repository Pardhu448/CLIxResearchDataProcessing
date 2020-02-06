'''
This code collates json files (tool section logs) of all the schools,
extracts relevant data from them and puts them all in a csv file for further
analysis.

This is fifth version.
Additional info from policesquad tool was extracted - time spent per mission.
This was for LO study team request - ruchi and sourav.
'''
import os
import re
import json
from collections import defaultdict
import logging
import operator
import itertools
import pandas

def get_state_school_json(json_file_path):
    path_items = json_file_path.split('/')
    state_code = path_items[path_items.index('gstudio') - 2]
    school_server_code = path_items[path_items.index('gstudio') - 1]
    if not school_server_code:
        print('Couldnt fetch school_server_code for file - {}'.format(json_file_path))
        logging.debug('Couldnt fetch school_server_code for file - %s', json_file_path)
    return {'state_code': state_code, 'school_server_code': school_server_code}

def get_files_from_a_tool_n_user(path):
    '''
    Gets all the filenames of particular pattern in a given directory name pattern (both given by regex)
    Use of nested tuples is to make it amenable for parallel processing in future if need be
    :param path: root path of data
    :param regex_file: regex to identify files say, .csv, .json etc
    :param regex_dir: regex to identify immediate parent directory of files
    :return: list of tuples - [(filename, file_attributes)]
    '''
    files_list = list()
    user_id = "0"
    #regex_tool_n_user = r"^" + re.escape(user_id) + "-policesquad.json"
    regex_tool_n_user = r".+-policesquad.json$"
    regObj_toolnuser = re.compile(regex_tool_n_user)
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
                                 
                                 if regObj_toolnuser.match(eachfile):
                                   json_file_path = os.path.join(root, dname, d1name, eachdir, eachfile)
                                   state_code = get_state_school_json(json_file_path)['state_code']
                                   school_server_code = get_state_school_json(json_file_path)['school_server_code']
                                   files_list.append((state_code, (school_server_code, json_file_path)))
    return files_list

def get_all_school_names():

    return None


def get_files(path, tool):
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
            #logging.debug("{0}: {1}".format(e, dict_nes))
            raise Exception("{0}: {1}".format(e, dict_nes))
            return None
        if (item_key == 'CorrectRelease') or (item_key == 'IncorrectRelease'):
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
                           schools_tool_dict['appData'].append(parse_nested_dict(items, 'appData'))
                           #schools_tool_dict['CaseDetails'].append(parse_nested_dict(items, 'CaseDetails'))
                           #schools_tool_dict['ReplayUsedNumber'].append(parse_nested_dict(items, 'ReplayUsedNumber'))
                           #schools_tool_dict['CorrectRelease'].append(parse_nested_dict(items, 'CorrectRelease'))
                           #schools_tool_dict['IncorrectRelease'].append(parse_nested_dict(items, 'IncorrectRelease'))
                           schools_tool_dict['StagePlayed_new'].append(parse_nested_dict(items, 'StagePlayed'))
                           schools_tool_dict['language'].append(parse_nested_dict(items, 'language'))

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


if __name__ == '__main__':

    working_dir = os.getcwd()
    logging.basicConfig(filename=working_dir + '/session_outputs.log', level=logging.DEBUG)

    print('This script parses through the top directory provided by user to fetch json files '
          '(hopefully saved in gstudio_tools_logs folder) and \n extract information from each '
          'of them at tool level.\n')
    print('This is for Adoption studies of Clix Project\n')
    print('---------------------------------------------------------------------------------')
    #parent_directory = input('Please enter any top level directory to fetch tools data from:\n')
    parent_directory = '/home/parthae/Documents/Projects/TISS_Git/projects/CLIxData/syncthing_Aug2019'
    tool_name = ['policesquad']
    # Tools related json files - indexed by state and school ids
    tools_json_files = get_files(parent_directory, tool = tool_name)
    print('Done fetching all the relevant json files indexed by state_code and school_server_id')
    print('------------------------------------------------------------------------------------')
    # Extracted data from tools json files - for all states and schools
    tools_all_states_data = accumulator(tools_json_files)

    for each_state in tools_all_states_data.keys():
       dframe_final = pandas.DataFrame(tools_all_states_data[each_state])
       cols_final = list(dframe_final.columns)
       cols_final = [each for each in cols_final if each not in ['appData', 'CaseDetails', 'CorrectRelease', 'IncorrectRelease']]
       dframe_final = dframe_final.drop_duplicates(subset=cols_final)
       dframe_final.to_csv(each_state + '_json_tools_data_vArundathi_Oct21st_v1.csv', mode='w', index=False)

    print('Done with extracting relevant information from all the json files.\n Also please '
          'find csv files generated for each state.')

# Adhoc script to fetch data
#parent_directory = '/home/parthae/Documents/Projects/TISS/projects/data/syncthing/sync-clix.tiss.edu'
#tools_json_files = get_files_from_a_tool_n_user(parent_directory)
#tools_all_states_data = accumulator(tools_json_files)
#
#rj_data = tools_all_states_data['rj']
#rj_dframe = pandas.DataFrame(rj_data)
#final = rj_dframe.groupby(['school_server_code', 'domain'])['time_spent'].apply(sum)
#
#def tool_names(df):
#    return list(df['tool_name'].unique())
#
#rj_dframe.groupby(['school_server_code', 'domain']).apply(tool_names)
#
#
##ct_dframe = pandas.DataFrame(ct_data)
##ct_dframe.groupby(['school_server_code', 'domain'])['time_spent'].apply(sum)