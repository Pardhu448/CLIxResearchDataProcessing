import re
import os
import shutil

def copy_files(regex_file, source_path, dest_path):
    '''
    Fetch all the file names(with full path) following a given filename pattern and in a given folder name pattern.
    :param data_path: parent folder containing all the data
    :param regex_file: filename's regex
    :param regex_dir: immediate parent directory's regex
    :return: List of tuples - (state_code, school_server_code, [-- List of csv files --])
    '''

    regObj_file = re.compile(regex_file)
    regex_parent_dir = re.compile(r'gstudio')

    for root, dnames, fnames in os.walk(os.path.join(source_path)):
        for dname in dnames:
            if regex_parent_dir.match(dname):
                for d1name in os.listdir(os.path.join(root, dname)):
                    #If in gstudio folder do the following:
                    if regObj_file.match(d1name):
                        tar_file_path = os.path.join(root, dname, d1name)
                        school_name = os.path.join(root, dname).split('/')[-2]
                        dest_path_new = dest_path + '/' + school_name+ '_db_dump.tar.gz'
                        shutil.move(tar_file_path, dest_path_new)
                        print('Done moving school - {0} tar.gz file'.format(school_name))

    return None

source_path = '/home/parthae/Documents/Projects/TISS/projects/data/'
destination_path = '/home/parthae/mongo_csv/data/mongo_db_code/data/'
#regex_file_log = r'.+(log)$'
#regex_dir = r'gstudio-exported-users-analytics-csvs'
regex_file = r'.+(tar.gz)$'
result = copy_files(regex_file=regex_file, source_path=source_path, dest_path=destination_path)


