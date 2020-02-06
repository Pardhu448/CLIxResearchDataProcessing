"""Sample script for generating CSV for assessment results

(c) 2018-Present Massachusetts Institute of Technology

Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the "Software"),
to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included
in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

=====================================


This script attempts to:
  1) collect tool logging data across schools. (MIT tools)

Note that the basic directory structure from clixplatform.tiss.edu is:

school-with-code/
  |- <possibly other directories>
    |- gstudio-exported-users-analytics-csvs/
      |- sp100-blahblah.csv
    |- gstudio_tools_logs/
      |- policesquad/
        |- en/
          |- 1260861.....json

It appears that all the hosted files are on their production server (IP 103.36.84.157)
at ``/home/core/setup-software/Reseach_data/...``.

So let's use wget to get the files that we want.

1) Get the files onto the local server via rsync. Do this in Jenkins as
   a separate job (clix-sync-research-data).

   $ rsync -avzm --exclude="Reseach_data_backup_HDD_20170601" --exclude="Reseach_data_backup_Pendrive_20170601" --exclude="School_data_backup_HDD_20170925_vivek_rj" --exclude="rcs-repo" --include="*/" --include="*.wt*" --include="*.json" --include="*.csv" --include="storage.bson" --include="mongod.lock" --include="WiredTiger*" --include="*.turtle" --include="server_settings.py" --include="postgres-dump" --prune-empty-dirs --exclude="*" -e "ssh -i $HOME/.ssh/tiss_server" --progress --stats cole@103.36.84.157:/home/core/setup-software/Reseach_data/ sync

   * This is much faster than wget if the TISS server doesn't die -- takes < 10 minutes

   * If rsync keeps hanging after
     a couple of files, might have to use wget instead.
     So, I propose wget with an Accept list, to only
     grab the MongoDB files, tool logs, and student analytics. This translates
     to (not sure -R is required):

   $ wget -m --no-parent -e robots=off -nH --cut-dirs=2 -A "*.wt*,*.csv,*.json,storage.bson,mongod.lock,WiredTiger*,*.turtle" -R "index.html,*.sql,*.py,*access.log,*error.log,*heartbeat*,*.tar*" https://clixplatform.tiss.edu/softwares/Reseach_data/rj-tech-visit-2017-dec/

   * Note, this wget command:
      * Takes a long time to run, since it has to evaluate each filename.
        Approximately 10+ hours?
      * Means that researchers will have to grab actual student response
        files from the server -- they won't be downloaded locally.

2) Run this script, which will walk through the directories (glob.iglob()).
   We should include a column for the path to each database plus the file name
   for the gstudio logs (because some of those encapsulate school code)....
   we can't assume any part of the path is the school code, because
   everything is at a different depth / structure -- there is no consistency
   in where the school data is dumped.
   For example, we currently see stuff like:

       2017/
         |- rj2/
             |- data/
                 |- db/
         |- tg36/
       rj66/
         |- gstudio/
             |- data/
                 |- db/
                 |- gstudio-exported-users-analytics-csvs/
                     |- 0310xx-tg36-foo.csv

3) For each school database, we can either start a MongoDB instance +
   a QBank instance, then run the existing script against QBank. Or, we
   just re-do the script to talk directly to MongoDB via pymongo...
   this seems less complex, to skip the QBank interface.

4) Append to one giant CSV file the same log data.

5) Send mail to research team (MIT Moira list?).
   Include the result files in the e-mail?

Let's get to actual code
"""
import getopt
import json
import os
import subprocess
import sys
import time

from pymongo import MongoClient

from local_assessment_results_and_logs import get_log_entries,\
    write_log_data, make_id, get_identifier
from send_mail import send_tool_log_results_mail
from settings import MONGO_DB_PORT, SYNC_DATA_PATH
from utilities import spawn_process, close_process


def aggregate_school_log_data(tool):
    """ Iterate through all the school directories and
        find MongoDB databases (/data/db). Start up a
        MongoDB instance on the MONGO_DB_PORT, then
        connect to it / parse the data from that school.
    """
    remove_old_files(tool)

    dbs_found = 1
    for directory, subdirs, files in os.walk(SYNC_DATA_PATH):
        if directory.endswith('/db'):
            # First call MongoDB with the --repair flag because
            #   some schools have an unclean shutdown
            # Use pexpect here so that we don't wait unnecessarily
            #   for the repair to finish. On large data sets it can
            #   take up to 90 seconds. For small ones, 15.
            command = 'mongod --repair --dbpath {0} --port {1}'.format(
                directory,
                str(MONGO_DB_PORT)
            )
            p = spawn_process(command,
                              timeout=90)
            p.expect('shutting down with code:0')
            close_process(p)
            proc = subprocess.Popen(['mongod',
                                     '--dbpath', directory,
                                     '--port', str(MONGO_DB_PORT)])
            time.sleep(5)
            write_generic_log_entries(
                tool,
                directory,
                dbs_found)

            proc.terminate()
            time.sleep(5)

            print('Processed {0} databases'.format(str(dbs_found)))
            dbs_found += 1
    return dbs_found - 1  # the last increment doesn't count as a processed DB


def remove_old_files(tool):
    """ Remove the old tool-logs.csv files, so we don't
        append to them. """
    if os.path.isfile(tool_logfile_name(tool)):
        os.remove(tool_logfile_name(tool))


def get_default_log(MC):
    """ Find the default log using genusTypeId """
    return MC['logging']['Log'].find_one(
        {
            "genusTypeId": "log-genus-type%3Adefault-clix%40ODL.MIT.EDU"
        })


def extract_user_ids_from_log_entries(log_entries):
    """ find out all the userIds that have entries, and return them
        as a list """
    user_ids = []
    for log_entry in log_entries:
        user_id = get_identifier(log_entry['agentId'])
        if user_id not in user_ids:
            user_ids.append(user_id)
    return user_ids


def filter_log_entries_by_tool(tool, log_id, MC):
    """ get only the log entries for a specific tool """
    all_log_entries = get_log_entries(log_id, MC)
    tool_entries = []
    for log_entry in all_log_entries:
        message = json.loads(log_entry['text']['text'])
        if message['app_name'] == tool:
            tool_entries.append(log_entry)
    return tool_entries


def tool_logfile_name(tool):
    """ helper method to generate consistent tool file name """
    return '{0}-tool-logs.csv'.format(tool)


def write_generic_log_entries(tool, db_path, db_number):
    """ Find the default log ID by genusTypeId, and then get all LogEntries
        for the given tool. Write to disk
    """
    MC = MongoClient(host='localhost', port=MONGO_DB_PORT)

    default_log = get_default_log(MC)
    if default_log is not None:
        log_entries = filter_log_entries_by_tool(tool,
                                                 make_id(default_log['_id'],
                                                         'logging.Log'),
                                                 MC)
        student_ids = extract_user_ids_from_log_entries(log_entries)

        write_log_data(db_number,
                       db_path,
                       tool_logfile_name(tool),
                       log_entries,
                       student_ids,
                       generic_tool_log=True)

# Make this method available at the command line, so we can call it
# from Jenkins.


if __name__ == '__main__':
    try:
        sargs = sys.argv[1:]

        opts, args = getopt.getopt(sargs,
                                   "te:",
                                   ["tool=", "email="])
        all_opts = {}
        for o in opts:
            all_opts[o[0]] = o[1]

        email = 'clix-research-data@mit.edu'
        tool = ''

        if '-e' in all_opts.keys():
            email = all_opts['-e']
        elif '--email' in all_opts:
            email = all_opts['--email']

        if '@' not in email:
            raise TypeError('invalid email: {0}'.format(email))

        if not any(children_label in all_opts.keys()
                   for children_label in ['-t', '--tool']):
            raise TypeError(
                'need to supply a tool with -t or --tool flag.'
                '"OpenStory" or "runkittyrun".')

        if '-t' in all_opts.keys():
            tool = all_opts['-t']
        elif '--tool' in all_opts.keys():
            tool = all_opts['--tool']

        db_number = aggregate_school_log_data(tool)
        send_tool_log_results_mail(
            email=email,
            tool=tool,
            db_number=db_number)
        remove_old_files(tool)
    except:
        # http://stackoverflow.com/questions/1000900/how-to-keep-a-python-script-output-window-open#1000968
        import traceback
        print sys.exc_info()[0]
        print traceback.format_exc()
