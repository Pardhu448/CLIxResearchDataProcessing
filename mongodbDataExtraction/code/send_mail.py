# For attachments, check:
# https://stackoverflow.com/questions/3362600/how-to-send-email-attachments
import datetime
import os
import smtplib

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from bs4 import BeautifulSoup

from settings import JENKINS_JOB_BUILD_PATH
from activity_timespent import school_filename


def get_data_sync_date_from_jenkins():
    """ Using the Jenkins job to sync CLIx data from TISS,
        return the last time the job successfully built. """
    if os.path.isfile(JENKINS_JOB_BUILD_PATH):
        with open(JENKINS_JOB_BUILD_PATH, 'r') as build_file:
            soup = BeautifulSoup(build_file.read(), 'lxml')
        # Notes on Jenkins:
        #   * Saves the time as milliseconds
        # Notes on BeautifulSoup
        #   * When souping this with 'lxml',
        #       lowercases "startTime" to "starttime"
        build_time_utc_s = int(soup.find('starttime').get_text()) / 1000
        local_time = datetime.datetime.fromtimestamp(
            build_time_utc_s).strftime('%Y-%m-%d %H:%M:%S')
        return local_time
    return 'Unknown'


def send_assessment_results_mail(
        email='clix-research-data@mit.edu',
        bank_id=None,
        offered_id=None,
        db_number=None):
    """ email utility to send assessment `results.csv` and `logs.csv`
        to the provided email """
    fromaddr = 'clix-research-data@mit.edu'
    subject = 'CLIx: Results for bank {0} / offered {1}'.format(
        bank_id,
        offered_id
    )
    last_data_sync = get_data_sync_date_from_jenkins()
    body = """
Attached are the cross-school results for:
  * Bank with ID: {0}
  * Assessment Offered with ID: {1}

Data was last sync\'d on {2}, from an estimated {3} schools.


""".format(
        bank_id,
        offered_id,
        last_data_sync,
        db_number
    )

    msg = MIMEMultipart()
    msg['From'] = 'clix-research-data@mit.edu'
    msg['To'] = email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    # Here also attach the results files
    for f in ['results.csv', 'log.csv']:
        with open(f, "rb") as file_handle:
            part = MIMEApplication(
                file_handle.read(),
                Name=os.path.basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="{0}"'.format(
            os.path.basename(f)
        )
        msg.attach(part)

    server = smtplib.SMTP('outgoing.mit.edu', 25)
    server.sendmail(fromaddr, email, msg.as_string())
    server.close()


def send_tool_log_results_mail(
        email='clix-research-data@mit.edu',
        tool=None,
        db_number=None):
    """ email utility to send tool log `tool-logs.csv`
        to the provided email """
    from get_default_log_entries import tool_logfile_name

    fromaddr = 'clix-research-data@mit.edu'
    subject = 'CLIx: Results for {0} tool logs'.format(tool)
    last_data_sync = get_data_sync_date_from_jenkins()
    body = """
Attached are the cross-school log results for {0}.

Data was last sync\'d on {1}, from an estimated {2} schools.


""".format(
        tool,
        last_data_sync,
        db_number
    )

    msg = MIMEMultipart()
    msg['From'] = 'clix-research-data@mit.edu'
    msg['To'] = email
    msg['Subject'] = subject

    # Here also attach the results files
    for f in [tool_logfile_name(tool)]:
        try:
            with open(f, "rb") as file_handle:
                part = MIMEApplication(
                    file_handle.read(),
                    Name=os.path.basename(f)
                )
        except IOError:
            body = """
No tool logs were found for {0}.

Data was last sync\'d on {1}, from an estimated {2} schools.""".format(
                tool,
                last_data_sync,
                db_number
            )
        else:
            # After the file is closed
            part['Content-Disposition'] = 'attachment; filename="{0}"'.format(
                os.path.basename(f)
            )
            msg.attach(part)

    msg.attach(MIMEText(body, 'plain'))

    server = smtplib.SMTP('outgoing.mit.edu', 25)
    server.sendmail(fromaddr, email, msg.as_string())
    server.close()


def send_user_activity_mail(
        email='clix-research-data@mit.edu',
        school=None,
        date=None,
        db_number=None,
        num_students=None):
    """ email utility to send user activity `{username}-activity-data.csv`
        to the provided email """
    fromaddr = 'clix-research-data@mit.edu'
    subject = 'CLIx: Activity Data for {0}, after {1}'.format(
        school,
        str(date.date())
    )
    last_data_sync = get_data_sync_date_from_jenkins()
    body = """
Attached are activity data logs for the following schools, after {0}:
  * School: {1}

Data was last sync\'d on {2}. {3} users were tracked across {4} school(s).


""".format(
        str(date.date()),
        school,
        last_data_sync,
        num_students,
        db_number
    )

    msg = MIMEMultipart()
    msg['From'] = 'clix-research-data@mit.edu'
    msg['To'] = email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    # Here also attach the result file
    for f in [school_filename(school)]:
        with open(f, "rb") as file_handle:
            part = MIMEApplication(
                file_handle.read(),
                Name=os.path.basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="{0}"'.format(
            os.path.basename(f)
        )
        msg.attach(part)

    server = smtplib.SMTP('outgoing.mit.edu', 25)
    server.sendmail(fromaddr, email, msg.as_string())
    server.close()
