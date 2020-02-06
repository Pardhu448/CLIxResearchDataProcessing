
#############################################
import pandas
from datetime import datetime, timedelta
from lifelines import KaplanMeierFitter

data_path = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest'
cg_data = pandas.read_csv(data_path + '/district_level/lab_usage_csvs/chattisgarh_lab_usage_March31st2019_monthly.csv')
cg_data['state'] = 'cg'
cg_data['state'] = 'cg_d'
mz_data = pandas.read_csv(data_path +  '/district_level/lab_usage_csvs/mizoram_lab_usage_March31st2019_monthly.csv')
mz_data['state'] = 'mz'
mz_data['district'] = 'mz_d'
jaipur_data = pandas.read_csv(data_path +  '/district_level/lab_usage_csvs/jaipur_lab_usage_March31st2019_monthly.csv')
jaipur_data['state'] = 'rj'
jaipur_data['district']  = 'jaipur'
sirohi_data = pandas.read_csv(data_path +  '/district_level/lab_usage_csvs/sirohi_lab_usage_March31st2019_monthly.csv')
sirohi_data['state'] = 'rj'
sirohi_data['district'] = 'sirohi'
warangal_data =  pandas.read_csv(data_path +  '/district_level/lab_usage_csvs/warangal_lab_usage_March31st2019_monthly.csv')
warangal_data['state'] = 'tg'
warangal_data['district'] = 'warangal'
rangareddy_data = pandas.read_csv(data_path +  '/district_level/lab_usage_csvs/rangareddy_lab_usage_March31st2019_monthly.csv')
rangareddy_data['state'] = 'tg'
rangareddy_data['district'] = 'rangareddy'
karimnagar_data  = pandas.read_csv(data_path +  '/district_level/lab_usage_csvs/karimnagar_lab_usage_March31st2019_monthly.csv')
karimnagar_data['state'] = 'tg'
karimnagar_data['state'] = 'karimnagar'

total_data = pandas.concat([cg_data, mz_data, jaipur_data, sirohi_data, warangal_data, rangareddy_data, karimnagar_data], ignore_index=True)
#total_data = cg_data
def get_survival_times(df):
    # To get survival times for each module in a particular school
    def get_time_diff(xdf):
        #https://stackoverflow.com/questions/14191832/how-to-calculate-difference-between-two-dates-in-weeks-in-python
        #Returns 0 if both dates fall withing one week, 1 if on two consecutive weeks, etc.
        #d2 = datetime.strptime(xdf['date_created'].max(), '%Y-%m-%d')
        #d1 = datetime.strptime(xdf['date_created'].min(), '%Y-%m-%d')
        #monday1 = (d1 - timedelta(days=d1.weekday()))
        #monday2 = (d2 - timedelta(days=d2.weekday()))
        #duration = (monday2 - monday1).days / 7
        # Considering only weeks on which modules were rolled out
        duration = len(xdf['date_created'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').date().isocalendar()[1]).unique())
        return duration
    df_new = df.groupby(['module_name']).apply(lambda x: get_time_diff(x)).reset_index()
    return df_new.rename(columns={0: 'duration_weeks'})
module_survival_data = total_data.groupby(['school_code']).apply(lambda x: get_survival_times(x)).reset_index(level=None)

module_survival_data['module_name'] = module_survival_data['module_name'].apply(lambda x: '_'.join(x.split("'")[1].split(" ")))
module_survival_data = module_survival_data[~ module_survival_data['module_name'].isin(['Pre-CLIx_Survey', 'Post-CLIx_Survey'])]
module_survival_data['event'] = 1
module_survival_data.to_csv('module_survival_data_V1.csv')

groups = module_survival_data['module_name']
T = module_survival_data['duration_weeks']
E = module_survival_data['event']

kmf = KaplanMeierFitter()
for i, each in enumerate(list(module_survival_data['module_name'].unique())):
    ix = (groups == each)

    kmf.fit(T[ix], E[ix], label=each)
    if i == 0:
        ax = kmf.plot(ci_show=False)

    else:
        ax = kmf.plot(ax=ax, ci_show=False)
ax.set_title(r"Survival Curves for Different Modules across all States")

# To get hazard functions for different modules
module_survival_data = total_data.groupby(['school_code']).apply(lambda x: get_survival_times(x)).reset_index(level=None)

module_survival_data['module_name'] = module_survival_data['module_name'].apply(lambda x: '_'.join(x.split("'")[1].split(" ")))
module_survival_data = module_survival_data[~ module_survival_data['module_name'].isin(['Pre-CLIx_Survey', 'Post-CLIx_Survey'])]
module_survival_data['event'] = 1

groups = module_survival_data['module_name']
T = module_survival_data['duration_weeks']
E = module_survival_data['event']

from lifelines import NelsonAalenFitter
naf = NelsonAalenFitter()
bandwidth = 3.

for i, each in enumerate(list(module_survival_data['module_name'].unique())):
    ix = (groups == each)

    naf.fit(T[ix], event_observed=E[ix], label=each)
    if i == 0:
        ax = naf.plot_hazard(bandwidth=bandwidth, ci_show=False)
    else:
        ax = naf.plot_hazard(ax=ax, bandwidth=bandwidth, ci_show=False)
ax.set_title("Hazard function of different modules | bandwidth=%.1f" % bandwidth);

# Survival curves for tools
import pandas
from datetime import datetime, timedelta
from lifelines import KaplanMeierFitter

data_path = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest'
cg_data = pandas.read_csv(data_path + '/state_level/state_tools_data/ct_metrics_tools_March31st2019.csv')
cg_data['state'] = 'cg'
mz_data = pandas.read_csv(data_path +  '/state_level/state_tools_data/mz_metrics_tools_March31st2019.csv')
mz_data['state'] = 'mz'
rj_data = pandas.read_csv(data_path +  '/state_level/state_tools_data/rj_metrics_tools_March31st2019.csv')
rj_data['state'] = 'rj'
tg_data = pandas.read_csv(data_path +  '/state_level/state_tools_data/tg_metrics_tools_March31st2019.csv')
tg_data['state'] = 'tg'

total_data = pandas.concat([cg_data, mz_data, rj_data, tg_data])
def get_survival_times(df):
    # To get survival times for each module in a particular school
    def get_time_diff(xdf):
        #https://stackoverflow.com/questions/14191832/how-to-calculate-difference-between-two-dates-in-weeks-in-python
        #Returns 0 if both dates fall withing one week, 1 if on two consecutive weeks, etc.
        #d2 = datetime.strptime(xdf['date_created'].max(), '%Y-%m-%d')
        #d1 = datetime.strptime(xdf['date_created'].min(), '%Y-%m-%d')
        #monday1 = (d1 - timedelta(days=d1.weekday()))
        #monday2 = (d2 - timedelta(days=d2.weekday()))
        #duration = (monday2 - monday1).days / 7
        # Considering only weeks on which modules were rolled out
        duration = len(xdf['date_created'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').date().isocalendar()[1]).unique())
        return duration
    df_new = df.groupby(['tool_name']).apply(lambda x: get_time_diff(x)).reset_index()
    return df_new.rename(columns={0: 'duration_weeks'})
tool_survival_data = total_data.groupby(['school_server_code']).apply(lambda x: get_survival_times(x)).reset_index(level=None)

#tool_survival_data['module_name'] = tool_survival_data['module_name'].apply(lambda x: '_'.join(x.split("'")[1].split(" ")))
#tool_survival_data = tool_survival_data[~ tool_survival_data['module_name'].isin(['Pre-CLIx_Survey', 'Post-CLIx_Survey'])]
tool_survival_data['event'] = 1
tool_survival_data.to_csv('tool_survival_data_V1.csv')
groups = tool_survival_data['tool_name']
T = tool_survival_data['duration_weeks']
E = tool_survival_data['event']

kmf = KaplanMeierFitter()
for i, each in enumerate(list(tool_survival_data['tool_name'].unique())):
    ix = (groups == each)

    kmf.fit(T[ix], E[ix], label=each)
    if i == 0:
        ax = kmf.plot(ci_show=False)
    else:
        ax = kmf.plot(ax=ax, ci_show=False)
ax.set_title(r"Survival Curves for Different Tools")

# To get hazard functions for different tools
tool_survival_data = total_data.groupby(['school_server_code']).apply(lambda x: get_survival_times(x)).reset_index(level=None)

#tool_survival_data['module_name'] = tool_survival_data['module_name'].apply(lambda x: '_'.join(x.split("'")[1].split(" ")))
#tool_survival_data = tool_survival_data[~ tool_survival_data['module_name'].isin(['Pre-CLIx_Survey', 'Post-CLIx_Survey'])]
tool_survival_data['event'] = 1
tool_survival_data.to_csv('tool_survival_data_all_states.csv')
groups = tool_survival_data['tool_name']
T = tool_survival_data['duration_weeks']
E = tool_survival_data['event']

from lifelines import NelsonAalenFitter
naf = NelsonAalenFitter()
bandwidth = 3.

for i, each in enumerate(list(tool_survival_data['tool_name'].unique())):
    ix = (groups == each)

    naf.fit(T[ix], event_observed=E[ix], label=each)
    if i == 0:
        ax = naf.plot_hazard(bandwidth=bandwidth, ci_show=False)
    else:
        ax = naf.plot_hazard(ax=ax, bandwidth=bandwidth, ci_show=False)
ax.set_title("Hazard function of different tools | bandwidth=%.1f" % bandwidth);
#################################################################################
#To connect teacher features with consistency in rolling out the modules
import pandas
import re
teachers_el_survey_data = pandas.read_csv("code/SurvivalAnalysis/Teacher_EL_4states.csv")
data_path = '/home/parthae/Documents/Projects/TISS_Git/projects/data_collation/data/data_latest'

cg_data = pandas.read_csv(data_path + '/district_level/lab_usage_csvs/chattisgarh_lab_usage_March31st2019_monthly.csv')
cg_data['state'] = 'cg'
cg_data['state'] = 'cg_d'
mz_data = pandas.read_csv(data_path +  '/district_level/lab_usage_csvs/mizoram_lab_usage_March31st2019_monthly.csv')
mz_data['state'] = 'mz'
mz_data['district'] = 'mz_d'
jaipur_data = pandas.read_csv(data_path +  '/district_level/lab_usage_csvs/jaipur_lab_usage_March31st2019_monthly.csv')
jaipur_data['state'] = 'rj'
jaipur_data['district']  = 'jaipur'
sirohi_data = pandas.read_csv(data_path +  '/district_level/lab_usage_csvs/sirohi_lab_usage_March31st2019_monthly.csv')
sirohi_data['state'] = 'rj'
sirohi_data['district'] = 'sirohi'
warangal_data =  pandas.read_csv(data_path +  '/district_level/lab_usage_csvs/warangal_lab_usage_March31st2019_monthly.csv')
warangal_data['state'] = 'tg'
warangal_data['district'] = 'warangal'
rangareddy_data = pandas.read_csv(data_path +  '/district_level/lab_usage_csvs/rangareddy_lab_usage_March31st2019_monthly.csv')
rangareddy_data['state'] = 'tg'
rangareddy_data['district'] = 'rangareddy'
karimnagar_data  = pandas.read_csv(data_path +  '/district_level/lab_usage_csvs/karimnagar_lab_usage_March31st2019_monthly.csv')
karimnagar_data['state'] = 'tg'
karimnagar_data['state'] = 'karimnagar'

total_data = pandas.concat([cg_data, mz_data, jaipur_data, sirohi_data, warangal_data, rangareddy_data, karimnagar_data], ignore_index=True)

#State codes in teachers data : cg == 1, mz == 2, rj == 3, tg == 4
def get_teacher_codes(df_school):

    try:
      school_server_code = df_school['school_server_code'].unique()[0]
      state_code = school_server_code.split('-')[1][:2]

      T_EL_data = teachers_el_survey_data
      school_code = school_server_code.split('-')[0][3:8]
      teachers_data_school = T_EL_data[T_EL_data['school_code'] == int(school_code)]

      if not teachers_data_school.empty:
        school_teachers = ','.join(map(str, list(teachers_data_school['uid'])))
      else:
        school_teachers = ''

      df_school['teacher_uids'] = school_teachers

      return df_school

    except Exception as e:
        import pdb
        pdb.set_trace()

module_n_teachers_data   = total_data.groupby(['school_name']).apply(lambda x: get_teacher_codes(x)).reset_index()

modules_domain_map = {"English" : ["[u'English Beginner']", "[u'English Elementary']"],
                      "Math" : ["[u'Geometric Reasoning Part I']", "[u'Geometric Reasoning Part II']", "[u'Linear Equations']",
                             "[u'Proportional Reasoning']"],
                      "Science" : ["[u'Atomic Structure']", "[u'Sound']", "[u'Understanding Motion']", "[u'Basic Astronomy']",
                             "[u'Health and Disease']", "[u'Ecosystem']", "[u'Reflecting on Values']"]
                      }

domain_module_map = {each: key for key, value in modules_domain_map.items() for each in value}
module_n_teachers_data['domain'] = module_n_teachers_data['module_name']
module_n_teachers_data = module_n_teachers_data.replace({'domain': domain_module_map})
module_n_teachers_data = module_n_teachers_data[module_n_teachers_data['domain'] != "[u'Pre-CLIx Survey']"]
module_n_teachers_data = module_n_teachers_data[module_n_teachers_data['domain'] != "[u'Post-CLIx Survey']"]

# To get survival curves domainwise
from lifelines import KaplanMeierFitter
from datetime import datetime
def get_survival_times(df):
    # To get survival times for each module in a particular school
    def get_time_diff_new(xdf):
        # Considering only weeks on which modules were rolled out
        duration = len(xdf['date_created'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').date().isocalendar()[1]).unique())
        return duration
    df_new = df.groupby(['domain']).apply(lambda x: get_time_diff_new(x)).reset_index()
    df_new['teacher_uids'] = df['teacher_uids'].unique()[0]

    return df_new.rename(columns={0: 'duration_weeks'})

module_n_teachers_data = module_n_teachers_data.groupby(['school_server_code']).apply(lambda x: get_survival_times(x)).reset_index(level=None)

#module_n_teachers_data['module_name'] = module_n_teachers_data['module_name'].apply(lambda x: '_'.join(x.split("'")[1].split(" ")))
module_n_teachers_data['event'] = 1
module_n_teachers_data.to_csv('module_survival_data_domainwise_with_teacherids.csv', index=False)

groups = module_n_teachers_data['domain']
T = module_n_teachers_data['duration_weeks']
E = module_n_teachers_data['event']

kmf = KaplanMeierFitter()
for i, each in enumerate(list(module_n_teachers_data['domain'].unique())):
    ix = (groups == each)

    kmf.fit(T[ix], E[ix], label=each)
    if i == 0:
        ax = kmf.plot(ci_show=False)

    else:
        ax = kmf.plot(ax=ax, ci_show=False)
ax.set_title(r"Survival Curves for Different Subjectwise")
# Log rank Test for differences in survival curves
from lifelines import statistics
df_survival_test = pandas.DataFrame(
    { 'durations': T,
      'events': E,
      'groups': groups
      })
result = statistics.pairwise_logrank_test(df_survival_test['durations'],
                                              df_survival_test['groups'],
                                              df_survival_test['events'])

result.test_statistic
result.p_value
result.print_summary()

# Given that math and science are significantly different, we need to see if there is any difference in
# their features of teachers of math and science from EL survey data.
# Codes:
# teacher_gender: male - 1, female - 2
# edu_qual: Phd, Mphil, DoublePG, PG, Grad, Sec, HrSec
# qual_bechalore: Indian_languages, eng, math, sci, soc_stud, other, NA
# qual_master: Indian_languages, eng, math, sci, soc_stud, other, NA
# teacher_profqual: MEd, BEd, DEd, other, no_prof_qual
# teacher_sub: indian_languages, eng, math, sci, soc_stud, other, NA
# appointment: regular, contract, guest, other
# residence: same_panchayat, same_block, same_district, other
# tpd_it: yes:1 , no: 2
import pandas
from functools import reduce
teachers_el_survey_data = pandas.read_csv("code/SurvivalAnalysis/Teacher_EL_4states.csv")
teacher_features = ['uid', 'age', 'teacher_gender', 'edu_qual', 'qual_bechalore',
                    'qual_master', 'teacher_profqual', 'teacher_sub', 'years_sub_taughtyears_language',
                    'years_sub_taughtyears_english', 'years_sub_taughtyears_math', 'years_sub_taughtyears_science',
                    'years_sub_taughtyears_socialstud', 'appointment', 'residence', 'tpd_it']
teachers_data_for_survival = teachers_el_survey_data[teacher_features]
teachers_data_for_survival = teachers_data_for_survival.dropna()
# To combine Teachers EL survey with platform data
module_n_teachers_data = pandas.read_csv('module_survival_data_domainwise_with_teacherids.csv')
module_n_teachers_data = module_n_teachers_data.dropna(subset=['teacher_uids'])
def get_sub_teacher_id(df_log):
    try:
        school_teacher_logs =  teachers_data_for_survival[teachers_data_for_survival['uid'].isin(df_log['teacher_uids'].split(','))]
    except Exception as e:
        import pdb
        pdb.set_trace()

    if df_log['domain'] == 'English':
        dom_code = 2
    elif df_log['domain'] == 'Math':
        dom_code = 3
    elif df_log['domain'] == 'Science':
        dom_code = 4
    else:
        print('Domain is something other than math , science and english')
        import pdb
        pdb.set_trace()
    try:
      teacher_index = school_teacher_logs['teacher_sub'].apply(lambda x:  reduce(lambda x, y: x or y, [each == '2' for each in x.split(' ')]))
      domain_teacher_uid = map(str, list(school_teacher_logs[teacher_index]['uid']))
      if domain_teacher_uid:
        return ','.join(domain_teacher_uid)
      else:
        return 'not_in_survey'
    except Exception as e:
        import pdb
        pdb.set_trace()

def get_tpd_status(teacher_uid):
    try:
        if teacher_uid != '':
            uid_list = list(map(int, teacher_uid.split(',')))
            tpd_status = list(teachers_data_for_survival[teachers_data_for_survival['uid'].isin(uid_list)]['tpd_it'])
            tpd = reduce(lambda x, y: x or y, [each == 1 for each in tpd_status])
            final_tpd = 1 if tpd == 1 else 2
            return final_tpd
        else:
            return 2
    except Exception as e:
        import pdb
        pdb.set_trace()

module_n_teachers_data['sub_teacherid'] = module_n_teachers_data.apply(lambda x: get_sub_teacher_id(x), axis=1)
module_n_teachers_data['tpd_ict'] = module_n_teachers_data['sub_teacherid'].apply(lambda x: get_tpd_status(x))
module_n_teachers_data.to_csv('module_survival_data_domainwise_with_tpd_status.csv')
# Survival curves for tpd and non-tpd schools
# Assuming that we have all teachers who tool tpd in end line survey
# If this is not true, may be we have to reduce the sample space to only survey schools
# If assumption hold we are also assuming that non-survey school teachers didnt take tpd (which need some verification)
from lifelines import KaplanMeierFitter
groups = module_n_teachers_data['tpd_ict']
T = module_n_teachers_data['duration_weeks']
E = module_n_teachers_data['event']

kmf = KaplanMeierFitter()
for i, each in enumerate(list(module_n_teachers_data['tpd_ict'].unique())):
    ix = (groups == each)

    kmf.fit(T[ix], E[ix], label=each)
    if i == 0:
        ax = kmf.plot(ci_show=False)

    else:
        ax = kmf.plot(ax=ax, ci_show=False)
ax.set_title(r"Survival Curves for TPD and Non-TPD schools")
# Log rank Test for differences in survival curves
from lifelines import statistics
df_survival_test = pandas.DataFrame(
    { 'durations': T,
      'events': E,
      'groups': groups
      })
result = statistics.pairwise_logrank_test(df_survival_test['durations'],
                                              df_survival_test['groups'],
                                              df_survival_test['events'])

result.test_statistic
result.p_value
result.print_summary()

# Survival curves for tpd and non tpd schools subjectwise



