"""
Extract from gstudio logs the time spent for a specific user,
across gstudio platform and all Tools.

Use the code from:

https://github.com/gnowledge/gstudio/blob/master/gnowsys-ndf/gnowsys_ndf/ndf/management/commands/activity_timespent.py

But directly reach into MongoDB, like we do for assessments. This can
now be found in `activity_timespent.py`.

Requires a local copy of the student ID data, available from:

https://drive.google.com/open?id=1KtqrVKHZX93EnUWG_vU_1BA870F6Wm_g
"""
import datetime
import os
import subprocess
import sys
import time

from activity_timespent_latest_v1 import school_filename,\
    activity_details, convert_utc_to_ist
from settings import MONGO_DB_PORT, SYNC_DATA_PATH
from utilities import spawn_process, close_process, is_windows


def is_matching_school_directory(directory, files, school):
    """ Returns whether or not a given directory has a matching
        school code in `server_settings.py` """
    if 'server_settings.py' in files:
        with open(os.path.join(
                directory,
                'server_settings.py'), 'r') as settings_file:
            for row in settings_file.readlines():
                expected_setting = "GSTUDIO_INSTITUTE_ID ='{0}'".format(
                    school
                )
                if row.strip() == expected_setting:
                    return True
    return False


def aggregate_user_activity(date):
    """ Expects a school code as a lowercase string, like sp100.
        Expects a date for a starting date, as a datetime object.
        This then checks all the directories for the matching
        value in `server_settings.py` `GSTUDIO_INSTITUTE_ID`. """
    if not isinstance(date, datetime.datetime):
        raise TypeError('date must be a datetime object')

    dbs_found = 1
    num_students = 0
    for directory, subdirs, files in os.walk(SYNC_DATA_PATH):
        # Do school code match on server_settings.py
        #    GSTUDIO_INSTITUTE_ID
        if directory.endswith('data'):
            # First call MongoDB with the --repair flag because
            #   some schools have an unclean shutdown
            # Use pexpect here so that we don't wait unnecessarily
            #   for the repair to finish. On large data sets it can
            #   take up to 90 seconds. For small ones, 15.
            db_path = os.path.join(directory, 'db')
            command = 'mongod --repair --dbpath {0} --port {1}'.format(
                db_path,
                str(MONGO_DB_PORT)
            )
            p = spawn_process(command, timeout=90)

            p.expect('shutting down with code:0')
            close_process(p)
            proc = subprocess.Popen(['mongod',
                                     '--dbpath', db_path,
                                     '--port', str(MONGO_DB_PORT)])
            time.sleep(5)
            command = 'mongo --port {0} --eval \'{1}\''.format(
                    str(MONGO_DB_PORT),
                    'db=db.getSiblingDB("gstudio-mongodb");db.Benchmarks.createIndex({calling_url: "text"});'
                )
            p = spawn_process(command,
                              timeout=60)
            p.expect('numIndexesAfter')
            close_process(p)

            school = directory.split('/')[-1]
            num_students += activity_details(school, date, directory)

            proc.terminate()
            time.sleep(10)

            print('Processed {0} databases, {1} students'.format(
                str(dbs_found),
                str(num_students)))
            dbs_found += 1
            if dbs_found == 2:
                import pdb
                pdb.set_trace()
    # the last increment doesn't count as a processed DB

    return dbs_found - 1, num_students


def remove_old_files(school):
    """ Remove the old {school}-student-activity.csv file, so we don't
        append to them. """
    filename = school_filename(school)
    if os.path.isfile(filename):
        os.remove(filename)


# Make this method available at the command line, so we can call it
# from Jenkins.

if __name__ == '__main__':
    try:
        date = datetime.datetime(2018, 12, 01)
        db_number, num_students = aggregate_user_activity(date)
        # Make the student analytics files, too?
    except:
        # http://stackoverflow.com/questions/1000900/how-to-keep-a-python-script-output-window-open#1000968
        import traceback
        print sys.exc_info()[0]
        print traceback.format_exc()
    finally:
        print('Done!')
