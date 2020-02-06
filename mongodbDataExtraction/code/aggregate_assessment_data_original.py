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


This script attempts to do two things:
  1) generate assessment data across schools, for specific bank and assessment.
  2) merge in gstudio user analytics files
(i.e. sp100-red-turtle-sp100-activity-visits-20180423-20h19m.csv)
with the tool log files (i.e. 1260861-policesquad.json).

This is so the research team can compare the events on a single timeline.

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

   $ rsync -avzm --exclude="Reseach_data_backup_HDD_20170601" --exclude="Reseach_data_backup_Pendrive_20170601" --exclude="old_tools_json" --exclude="School_data_backup_HDD_20170925_vivek_rj" --exclude="rcs-repo" --include="*/" --include="*.wt*" --include="*.json" --include="*.csv" --include="storage.bson" --include="mongod.lock" --include="WiredTiger*" --include="*.turtle" --include="server_settings.py" --include="postgres-dump/pg_dump_all.sql" --prune-empty-dirs --exclude="*" -e "ssh -i $HOME/.ssh/tiss_server" --progress --stats cole@103.36.84.157:/home/core/setup-software/Reseach_data/ sync

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


3) In Jenkins, we should take a parameter for the bankId and the offeredId.
   These would get passed on to the actual data script (step 4).

4) For each school database, we can either start a MongoDB instance +
   a QBank instance, then run the existing script against QBank. Or, we
   just re-do the script to talk directly to MongoDB via pymongo...
   this seems less complex, to skip the QBank interface.

5) Append to one giant CSV file the same results data / log data.

6) Send mail to research team (MIT Moira list?).
   Include the result files in the e-mail?

Let's get to actual code
"""
import getopt
import os
import subprocess
import sys
import time

from local_assessment_results_and_logs import make
from send_mail import send_assessment_results_mail
from settings import MONGO_DB_PORT, SYNC_DATA_PATH
from utilities import spawn_process, close_process


def aggregate_school_assessment_data(bank_id, offered_id):
    """ Iterate through all the school directories and
        find MongoDB databases (/data/db). Start up a
        MongoDB instance on the MONGO_DB_PORT, then
        connect to it / parse the data from that school.
    """
    remove_old_files()

    dbs_found = 1
    has_headers = False
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
            has_headers = make(
                bank_id,
                offered_id,
                directory.replace(SYNC_DATA_PATH, ''),
                dbs_found,
                has_headers)

            proc.terminate()
            time.sleep(5)

            print('Processed {0} databases'.format(str(dbs_found)))
            dbs_found += 1
    return dbs_found - 1  # the last increment doesn't count as a processed DB


def remove_old_files():
    """ Remove the old results.csv and log.csv files, so we don't
        append to them. """
    if os.path.isfile('results.csv'):
        os.remove('results.csv')
    if os.path.isfile('log.csv'):
        os.remove('log.csv')

# Make this method available at the command line, so we can call it
# from Jenkins.


if __name__ == '__main__':
    try:
        sargs = sys.argv[1:]

        opts, args = getopt.getopt(sargs,
                                   "boe:",
                                   ["bank=", "offered=", "email="])
        all_opts = {}
        for o in opts:
            all_opts[o[0]] = o[1]

        email = 'clix-research-data@mit.edu'
        bank_id = ''
        offered_id = ''

        if '-e' in all_opts.keys():
            email = all_opts['-e']
        elif '--email' in all_opts:
            email = all_opts['--email']

        if not any(node_key in all_opts.keys() for node_key in ['-b', '--bank']):
            raise TypeError('need to supply a bankId with -b or --bank flag')

        if not any(children_label in all_opts.keys()
                   for children_label in ['-o', '--offered']):
            raise TypeError(
                'need to supply an offeredId with -o or --offered flag')

        if '@' not in email:
            raise TypeError('invalid email: {0}'.format(email))

        if '-b' in all_opts.keys():
            bank_id = all_opts['-b']
        elif '--bank' in all_opts.keys():
            bank_id = all_opts['--bank']

        if '-o' in all_opts.keys():
            offered_id = all_opts['-o']
        elif '--offered' in all_opts.keys():
            offered_id = all_opts['--offered']

        db_number = aggregate_school_assessment_data(bank_id, offered_id)
        # Make the student analytics files, too?
        send_assessment_results_mail(
            email=email,
            bank_id=bank_id,
            offered_id=offered_id,
            db_number=db_number)
        remove_old_files()
    except:
        # http://stackoverflow.com/questions/1000900/how-to-keep-a-python-script-output-window-open#1000968
        import traceback
        print sys.exc_info()[0]
        print traceback.format_exc()
