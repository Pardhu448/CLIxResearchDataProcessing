# To get seq flag for policesquad data
import pandas
from functools import reduce

def getChangeSeqFlag(df):
    # To get number of one level skips of adjacent logs in terms of mission skips
    mission_seq = df['current_mission'].apply(lambda x: int(x[len(x)-1]))
    mission_skips = [each for each in map(lambda x: x[1] - x[0], list(zip(mission_seq[:len(mission_seq)-1], mission_seq[1:])))]
    df['mission_skip_number'] = mission_skips + [999]
    mission_skip_number_forward = sum([each >= 2 for each in mission_skips])
    mission_skip_number_backward = sum([each < 0 for each in mission_skips])
    df['mission_skip_forward'] = mission_skip_number_forward
    df['mission_skip_backward'] = mission_skip_number_backward
    return df

def getChangeSeqFlagStage(df):

        if len(df) == 1:
            df['stage_skip_number'] = 0
            df['stage_skip_number_backward'] = 0
        else:
            stage_seq = list(df['StagePlayed_new'])
            stage_skips = [each for each in map(lambda x: x[1] - x[0], list(zip(stage_seq[:len(stage_seq)-1], stage_seq[1:])))]
            df['stage_skip_number'] = stage_skips + [777]
            stage_skip_number_backward = sum([each < 0 for each in stage_skips])
            df['stage_skip_number_backward'] = stage_skip_number_backward
            if stage_skip_number_backward == 1:
                import pdb
                pdb.set_trace()

        return df

def getAnamolyFlag(df):
    #To flag the abnormal logs, where log timestamp, session, date and user id same but different entries in case details. If the case details
    # are same they just correspond to redundant data
    #df.groupby(['createdat_end'])
    df['createdat_end_new'] = df['createdat_end'].apply(lambda x: x[:16])
    df['anomoly_flag'] = sum(df.groupby(['createdat_end_new']).apply(lambda x: len(x['session_id'].unique()) > 1 if 1 else 0))
    return df


for each_state in ['ct', 'rj', 'mz', 'tg']:
    pq_data = pandas.read_csv(each_state + '_json_tools_data_vArundathi_Oct21st_v1.csv')
    pq_data['date_created'] = pandas.to_datetime(pq_data['createdat_end'], format="%Y-%m-%d %H:%M:%S", errors="coerce").apply(lambda x: x.date())

    pg_data_new = pq_data.groupby(['school_server_code', 'date_created', 'user_id', 'session_id']).apply(lambda x: getChangeSeqFlag(x)).reset_index(level=None, drop=True)

    pg_data_new1 = pg_data_new.groupby(['school_server_code', 'date_created', 'user_id', 'session_id', 'current_mission']).apply(lambda x: getChangeSeqFlagStage(x)).reset_index(level=None, drop=True)

    pg_data_new2 = pg_data_new1.groupby(['school_server_code', 'user_id', 'date_created']).apply(lambda x: getAnamolyFlag(x)).reset_index(level=None, drop=True)
    pg_data_new2.to_csv('pq_mission_seq_data_' + each_state + '_Oct21st_v2.csv', index=False)
#####################################################################################################
import pandas
import re

#ps_data_raw = pandas.read_csv(path + 'ct_json_tools_data_vArundathi.csv')
ps_data_raw = pandas.read_csv('pq_mission_seq_data_ct' + '_Oct21st_v2.csv')
#ps_data_exported = pandas.read_csv(path + 'exported.csv')

def parse_appData(df_appdata):

    appdata_dict = {}
    try:
        appdata_text = df_appdata.replace("\'", "")
    except Exception as e:
        import pdb
        pdb.set_trace()

    reg_exp = re.compile("[A-Z|a-z]+:")
    keys = reg_exp.findall(appdata_text)
    for each in keys:
        if each == 'CaseDetails:':
            regex_val = re.compile(re.escape(each) + "(\s\[.*\]\])")
        elif (each == 'MissionsPlayed:' or each == 'MissionsCompleted:'):
            regex_val = re.compile(re.escape(each) + "(\s\[.*?\d\])")
        else:
            regex_val = re.compile(re.escape(each) + "(\s.*?),\s")
        value = regex_val.findall(appdata_text)
        try:
         if value:
            appdata_dict[each.replace(':', '')] = value[0].replace(' ', '')
         else:
            appdata_dict[each.replace(':', '')] = ''
        except Exception as e:
            import pdb
            pdb.set_trace()

    return appdata_dict
ps_data_raw = ps_data_raw.dropna(axis=0, subset=['appData'])
ps_data_raw['appData_new'] = ps_data_raw['appData'].apply(lambda x: parse_appData(x))
appdata_dict = ps_data_raw['appData_new'][0]

for each_key in appdata_dict.keys():
    ps_data_raw[each_key] = ps_data_raw['appData_new'].apply(lambda x: x[each_key])

ps_data_raw['ts_hr'] = ps_data_raw['time_spent'].apply(lambda x: x.split(':')[0])
ps_data_raw['ts_min'] = ps_data_raw['time_spent'].apply(lambda x: x.split(':')[1])
ps_data_raw['ts_sec'] = ps_data_raw['time_spent'].apply(lambda x: x.split(':')[2])

ps_data_raw = ps_data_raw.drop(columns=['appData_new'])
ps_data_raw.to_csv('final_cg_policesquad_arundhati_Oct21st.csv', index=False)

# To get clue fig and result separately
import numpy
def get_mission_details(df_mission, mission_flag):
    # To get case details for each log. Each mission is treated differently
    # to get required details from logs.
    def get_mission1_details(df):

        reg_Obj_res = re.compile( '.*' + re.escape('correct') + '*.', re.IGNORECASE)
        if df['language'] == 'hi':
            reg_Obj_clue = re.compile( '.*' + re.escape('\\xa') + '*.', re.IGNORECASE)
        elif df['language'] == 'en':
            reg_Obj_clue = re.compile( '.*' + re.escape('culprit') + '*.', re.IGNORECASE)
        else:
            import pdb
            pdb.set_trace()
        #if df['CaseDetails']

        try:
         clue_index = [i for i, each in enumerate(df['CaseDetails'].split(',')) if reg_Obj_clue.match(each)]
         result_index = [i for i, each in enumerate(df['CaseDetails'].split(',')) if reg_Obj_res.match(each)]

         clue_number = [each + 1 for each in range(len(clue_index))][::-1]
         clue_text = [df['CaseDetails'].split(',')[i] for i in clue_index]
         results = [df['CaseDetails'].split(',')[i] for i in result_index]

         if len(results) != len(clue_text):
          results.extend(['Notavailable'] * abs(len(results) - len(clue_text)))

         df_new = pandas.DataFrame({'clue_number': clue_number, 'clue_text': clue_text, 'clue_result': results})
         df_final = pandas.concat([pandas.DataFrame(df).T, df_new], axis=1)
         df_final_sorted = df_final.sort_values(by=['clue_number'])
         df_final_sorted = df_final_sorted.drop(['CaseDetails'], axis=1)
         df_final_fillna = df_final_sorted.fillna(method='bfill')
        except Exception as e:
         if numpy.isnan(df['CaseDetails']):
             return pandas.DataFrame()
         else:
             import pdb
             pdb.set_trace()
        return df_final_fillna

    def get_mission2_details(df_mission):

        return None

    if mission_flag == 1:
        #try:
        #    df_mission['figures'] = df_mission['CaseDetails'].apply(lambda x: ','.join(x.split(',')[1:9]))
        #except Exception as e:
        #    import pdb
        #    pdb.set_trace()

        df_mission = df_mission.apply(lambda x: get_mission1_details(x), axis=1)
        mission_df = pandas.concat([each for each in df_mission])

    if mission_flag == 2:
        df_mission['figures'] = df_mission['CaseDetails'].apply(lambda x: ','.join(x.split(',')[1:9]))
        df_mission = df_mission.apply(lambda x: get_mission2_details(x), axis=1)
        mission_df = pandas.concat([each for each in df_mission])
    return mission_df

import re
import pandas
pq_data = pandas.read_csv('final_cg_policesquad_arundhati_Oct21st.csv')
pq_data = pq_data.groupby(['CurrentMission']).apply(lambda x: get_mission_details(x, mission_flag=1))
pq_data1 = pq_data[pq_data['CurrentMission'] == 'Mission1']
pq_data_final = pq_data1.fillna(method = 'ffill')
col_req =  list(set(pq_data_final.columns) - set(['appData', 'GlossaryDownloaded', 'HyperlinksClicked', 'StorySkiped', '']))

#To get all possible clues used
pq_data_final1 = pq_data_final.drop_duplicates(subset=col_req)
pq_data_final1[pq_data_final1['user_id'].isin([546212, 546227, 546278])].to_csv('ThreeStudentPQ.csv')
pandas.DataFrame({'clue_text': pandas.Series(pq_data_final['clue_text'].unique())}).to_csv('clue_text.csv')
# To replace clues with english codes
import pandas
clue_text_data = pandas.read_csv('clue_text.csv')
clue_text_data_with_code = pandas.read_csv('clue_text_mission1_with_code.csv')
sample = clue_text_data_with_code[:10]
mapping = {each[0]: each[1] for each in zip(sample['clue_text'], sample['english_code'])}

converted_csv = pandas.DataFrame(clue_text_data_with_code['clue_text'].replace(mapping)).to_csv('clue_text_replaced_with_code.csv')




# To get all possible figures used
pq_data['caseflag'] = pq_data['CaseDetails'].isna()
pq_data = pq_data[pq_data['caseflag'] == False]
pq_data['figures'] = pq_data['CaseDetails'].apply(lambda x: ','.join(x.split(',')[1:9]))

fig_list_all = []
#fig_list_all1 = []
def get_all_fig(x):
        reg_Obj_res = re.compile( '.*' + re.escape('fig') + '*.', re.IGNORECASE)
        fig_list = [i for i in x['figures'].split(',') if reg_Obj_res.match(i)]
        stage = x['StagePlayed']
        fig_list_final = [each.replace('[', '') for each in fig_list]
        fig_list_final1 = [each.replace(']', '') for each in fig_list_final]
        fig_stage_map = [(each, stage) for each in fig_list_final1]
        fig_list_all.extend(fig_stage_map)
        return None

pq_data.apply(lambda x: get_all_fig(x), axis=1)
fig_stage_df = pandas.DataFrame([[each, stage] for each, stage in pandas.Series(fig_list_all).unique()])
fig_stage_df.columns = ['fig', 'stage']
fig_stage_df.groupby(['fig']).apply(lambda x: list(x['stage'].unique())).reset_index()

#pandas.DataFrame(pandas.Series(fig_list_all1).unique()).to_csv('all_fig_mission1.csv')

# To get pattern of score increase with replays and reattempts
import pandas
pq_data = pandas.read_csv('final_cg_policesquad_arundhati_Oct21st.csv')
# To check if there is any pattern in score achieved as the user repeates a stage
# For a given school, for a given session, for a given student we find the difference
# in score achieved as a user repeats the stage.
# We want to see the change in score when a user repeats the stage as compared to when he
# progresses the stage in a mission.
def get_diff_score_when_repeat(df):

    repeat_index = [t for t, skip in enumerate(list(df['stage_skip_number'])) if (skip == 0 or skip < 0)]
    not_repeat_index = list(set([each for each in range(len(df['stage_skip_number']))]) - set(repeat_index))
    df['score_diff_repeat'] = df['scoreAchieved'].diff(periods=-1) * -1
    df['score_diff_repeat'].iloc[not_repeat_index] = 888

    df['score_diff_non_repeat'] = df['scoreAchieved'].diff(periods=-1) * -1
    df['score_diff_non_repeat'].iloc[repeat_index] = 555
    df['score_diff_non_repeat'] = df['score_diff_non_repeat'].fillna(555)

    return df

pq_data = pq_data.groupby(['school_server_code', 'session_id', 'user_id', 'CurrentMission']).apply(lambda x: get_diff_score_when_repeat(x)).reset_index()
pq_data_mission1 = pq_data[pq_data['CurrentMission'] == 'Mission1']
pq_data.to_csv('pq_data_cg_with_score_diffs_when_repeated.csv')

