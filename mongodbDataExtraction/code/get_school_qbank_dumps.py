import re
import os
import tarfile
import subprocess
import time
import tarfile

def get_mongodump(regex_file, source_path, dest_path):
    '''
    Fetch tar.gz files in each of the school folders in syncthing data
    untar them into a folder with the school name specified

    :param data_path: parent folder containing all the data
    :param regex_file: filename's regex
    :param regex_dir: immediate parent directory's regex
    '''

    regObj_file = re.compile(regex_file)
    regex_parent_dir = re.compile(r'gstudio')

    for root, dnames, fnames in os.walk(os.path.join(source_path)):
        for dname in dnames:
            if regex_parent_dir.match(dname):
                for files1 in os.listdir(os.path.join(root, dname)):
                    #If in gstudio folder do the following:
                    if regObj_file.match(files1):
                        #if found db folder
                        src_file_path = os.path.join(root, dname, files1)
                        school_name = os.path.join(root, dname).split('/')[-2]
                        dst_path_new = dest_path + '/' + school_name+ '_dump'

                        #proc = subprocess.Popen(['tar', '-C', dst_path_new + '/tmp', '-xvf', src_file_path])
                        #time.sleep(3)
                        #proc.terminate()

                        tar = tarfile.open(src_file_path)
                        tar.extractall(path = dst_path_new + "/tmp")
                        time.sleep(60)
                        tar.close()

                        print('Done moving school - {0} tar files'.format(school_name))
    return None

working_dir = os.getcwd()
source_path = '/home/parthae/Documents/Projects/TISS_Git/projects/mongodb_data/data/syncthing_qbank_test/syncthing_dump/patchr6school'
destination_path = '/home/parthae/Documents/Projects/TISS_Git/projects/mongodb_data/data/syncthing_qbank_test/syncthing_dump/patchr6school'

#regex_file_log = r'.+(log)$'
#regex_dir = r'gstudio-exported-users-analytics-csvs'
regex_file = r'.+(tar.gz)$'
#regex_folder = r'gstudio'
result = get_mongodump(regex_file=regex_file, source_path=source_path, dest_path=destination_path)


