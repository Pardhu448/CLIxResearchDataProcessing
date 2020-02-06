import datetime
import json

import pytz


def get_tool_logs(log_file, start_date):
    """ Assumes the log file is the full path to a JSON file,
        with list of log objects.
        start_date is the date from which we want data.

        Assumes keys are (per policequad examples):
            * user_id
            * created_at
            * params
            * app_name
            * event_type

        Some TISS tools switched to camelCase...so we'll just
            convert back if we see camelCase. But it's not consistent.
    """
    with open(log_file, 'r') as tool_log_data:
        filtered_results = []
        try:
            tool_log = json.load(tool_log_data)
        except ValueError:
            print('{0} has invalid JSON data...report it to TISS'.format(
                log_file))
        else:
            for blob in tool_log:
                if 'created_at' in blob:
                    blob['created_at'] = convert_iso_string_to_ist(
                        blob['created_at'])
                elif 'createdAt' in blob:
                    blob['created_at'] = convert_iso_string_to_ist(
                        blob['createdAt'])
                else:
                    # Some tools aren't logging data correctly.. put some
                    #   absurdly future time here so the data isn't lost...
                    blob['created_at'] = fake_future_ist()
                blob['visited_on'] = blob['created_at']  # need this to sort
                blob['_type'] = 'tool'
                if 'appName' in blob:
                    blob['app_name'] = blob['appName']
                if blob['created_at'].date() >= start_date.date():
                    filtered_results.append(blob)
        return filtered_results


def convert_iso_string_to_ist(ist_string):
    """ because the tools save JSON strings like "2017-11-7 11:14:12"
        as the created_at values...but to do datetime comparisons and
        sort, we need a datetime object """
    ist = pytz.timezone('Asia/Kolkata')
    # Use strptime because the tool logs do not 0-pad minutes / seconds...
    #   i.e."created_at": "2017-11-7 11:35:6"
    # Which breaks libraries like iso8601.
    input_format = '%Y-%m-%d %H:%M:%S'
    naive_time = datetime.datetime.strptime(ist_string, input_format)
    return naive_time.replace(tzinfo=ist)


def fake_future_ist():
    """ create a fake future IST time since some tools like Astroamer
    Moon Track aren't logging a `created_at` time in the logs entries """
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.datetime.now().replace(
        tzinfo=ist) + datetime.timedelta(days=5000 * 365)


def tool_headers():
    """ return the standardized set of tool headers for each tool
        interaction """
    return ['AppName', 'CreatedAt (IST)',
            'EventType', 'Params']
