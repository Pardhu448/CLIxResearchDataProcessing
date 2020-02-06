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

"""

import datetime

import requests
import unicodecsv as csv

BASE_URL = 'https://localhost:8080/api/v1/'


def make(bank_id, assessment_offered_id):
    """Run through the results for an assessment offered"""

    results_url = (BASE_URL +
                   'assessment/banks/' +
                   bank_id + '/assessmentsoffered/' +
                   assessment_offered_id +
                   '/results?additionalAttempts')

    items_url = (BASE_URL +
                 'assessment/banks/' +
                 bank_id + '/items')

    log_url = (BASE_URL +
               'logging/logs/' +
               bank_id + '/logentries')

    results = requests.get(results_url, verify=False).json()
    items = requests.get(items_url, verify=False).json()
    log = requests.get(log_url, verify=False).json()

    # Write results.csv file for this assessment offered:
    with open('results.csv', 'w') as csv_file:
        student_ids = []
        result_writer = csv.writer(csv_file, dialect='excel')

        result_writer.writerow(make_results_header(results))
        for result in results:
            row = make_result_row(result, items)
            student_ids.append(row[0])
            result_writer.writerow(row)

    # Write log.csv file for this assessment offered:
    with open('log.csv', 'wb') as csv_file:
        log_writer = csv.writer(csv_file, dialect='excel')

        log_writer.writerow(['student_id', 'timestamp', 'action', 'etc...'])
        for student_id in student_ids:
            if not student_id or student_id == 'None':
                continue
            entries = []
            for log_entry in log:
                message = eval(log_entry['text']['text'])
                if ('assessmentOfferedId' in message and
                        get_identifier(log_entry['agentId']) == student_id and
                        'action' in message):
                    entries.append(get_datetime(log_entry['timestamp']))
                    entries.append(message['action'])
            log_writer.writerow([student_id] + entries)


def make_results_header(results):
    """return the header, list of strings to appear as column headings"""

    header = ['student_id',
              'assessment_offered_name',
              'start_time',
              'completion_time']

    for num in range(len(results[0]['sections'][0]['questions'])):
        label = 'q' + str(num)
        header += [label + '_question_text',
                   label + '_submission_times',
                   label + '_attempts']
        # label + '_correctness']  # This is not useful in /results
        # because always True
    return header


def get_item(item_id, items):
    """ return the given item from all the items """
    return [i for i in items if i['id'] == item_id][0]


def get_question_text(question, items):
    """ get the item's question text from the list of items """
    item = get_item(question['itemId'], items)
    return item['question']['text']['text']


def get_submission_times(question):
    """ return a stringified version of all the submission times,
        in reverse chronological order """
    if question['response']:
        times = [str(get_datetime(
            question['response']['submissionTime']))]
    else:
        times = ['None']
    if ('additionalAttempts' in question and
            len(question['additionalAttempts']) > 0):
        for additional_attempt in question['additionalAttempts']:
            times.append(str(get_datetime(
                additional_attempt['submissionTime'])))
    return ','.join(times)


def get_choice_text(choice_id, item):
    """ grab the choice text """
    return [c['text']
            for c in item['question']['choices']
            if c['id'] == choice_id][0]


def get_identifier(osid_id):
    """ return the identifier of an Osid ID.
    For `osid_id` = 'repository.AssetContent%3A5a27dea1eb5c87013410532c%40ODL.MIT.EDU'
    this would return '5a27dea1eb5c87013410532c'
    """
    return osid_id.split('%3A')[-1].split('%40')[0]


def format_file_name(file_dict):
    """ Return a formatted file name by guessing at the extension from the
    file label. For example, for:

    'fileIds': {
        u 'My Project (1)_tb': {
            u 'assetId': u 'repository.Asset%3A5a27dea1eb5c87013410532a%40ODL.MIT.EDU',
            u 'assetContentTypeId': u 'asset-content-genus-type%3Atb%40ODL.MIT.EDU',
            u 'assetContentId': u 'repository.AssetContent%3A5a27dea1eb5c87013410532c%40ODL.MIT.EDU'
        }
    }

    This method would return `5a27dea1eb5c87013410532c.tb`.

    NOTE: we'll assume only a single file here, since this should only be used
    with responses, which take one file right now.
    """
    label = file_dict.keys()[0]
    guessed_extension = label.split('_')[-1]
    filename = get_identifier(file_dict[label]['assetContentId'])
    return '{0}.{1}'.format(filename, guessed_extension)


def get_attempts(question, items):
    """ from the list of items, get the actual choice text
        for each attempt """
    item = get_item(question['itemId'], items)
    if question['response']:
        response = question['response']
        if 'choiceIds' in response:
            attempts = [get_choice_text(response['choiceIds'][0],
                                        item)]
        elif 'text' in response:
            attempts = [response['text']['text']]
        elif 'fileIds' in response and response['fileIds'] != {}:
            # non-empty file submission
            attempts = [format_file_name(response['fileIds'])]
        else:
            raise TypeError('need to account for new response type,',
                            response)
    else:
        attempts = ['None']
    if ('additionalAttempts' in question and
            len(question['additionalAttempts']) > 0):
        for additional_attempt in question['additionalAttempts']:
            if 'choiceIds' in additional_attempt:
                attempts.append(get_choice_text(
                    additional_attempt['choiceIds'][0],
                    item))
            elif 'text' in additional_attempt:
                attempts.append(additional_attempt['text']['text'])
            elif ('fileIds' in additional_attempt and
                  additional_attempt['fileIds'] != {}):
                # non-empty file submission
                attempts.append(
                    format_file_name(additional_attempt['fileIds']))
            else:
                raise TypeError('need to account for new response type,',
                                additional_attempt)
    return ','.join(attempts)


def get_correctness(question):
    """ get the qbank evaluation of isCorrect
        Not useful for the MC questions in Astronomy
        becuase always True in the results """
    if question['response']:
        correctness = [str(question['response']['isCorrect'])]
    else:
        correctness = ['None']
    if ('additionalAttempts' in question and
            len(question['additionalAttempts']) > 0):
        for additional_attempt in question['additionalAttempts']:
            correctness.append(str(additional_attempt['isCorrect']))
    return ','.join(correctness)


def make_result_row(result, items):
    """return one row representing one result"""

    row = [get_identifier(result['takingAgentId']),
           result['displayName']['text'],
           get_datetime(result['actualStartTime']),
           get_datetime(result['completionTime']),
           ]

    for question in result['sections'][0]['questions']:
        row += [get_question_text(question, items),  # grab the item text
                get_submission_times(question),
                get_attempts(question, items)]  # grab the choice text
        # get_correctness(question)]  # grab the correctness
        # This is not useful in /results
        # because always True
    return row


def get_datetime(dt_map):
    """Converts a datetime map from QBank into a regular python datetime"""
    if dt_map is None:
        return 'None'
    else:
        return datetime.datetime(**dt_map)
