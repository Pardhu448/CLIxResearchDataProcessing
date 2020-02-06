import json
import unicodecsv as csv

from bs4 import BeautifulSoup
from pymongo import MongoClient
from bson import ObjectId

from settings import MONGO_DB_PORT

""" Like assessment_results_and_logs.py except
this runs directly against MongoDB instead of QBank RESTful APIs.
This also returns the school results in a list, for a single CSV instead
of creating the CSV for you. """

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


def make_id(identifier, namespace):
    """ Return an OSID ID string from the identifier and
        namespace. """
    return '{0}%3A{1}%40ODL.MIT.EDU'.format(namespace, identifier)


def identifier(id_str):
    """ Return the identifier of an OSID ID string """
    return id_str.split('%3A')[-1].split('%40')[0]


def get_results(offered_id, MC):
    """ get the sections / results for a given assessment offered """
    SECTIONS = MC['assessment']['AssessmentSection']
    TAKENS = MC['assessment']['AssessmentTaken']

    results = []
    for taken in TAKENS.find(
            {"assessmentOfferedId": offered_id}):
        full_sections = []
        # Do it this way instead of generic section search to match
        #   `assessmentTakenId` because some sections are crufty
        #   and not getting deleted.
        section_ids = [ObjectId(get_identifier(section))
                       for section in taken['sections']]
        for section in SECTIONS.find(
                {"_id": {"$in": section_ids}}):
            full_sections.append(section)
        taken['sections'] = full_sections
        results.append(taken)
    return results


def get_log_entries(bank_id, MC, assessment_offered_id=None):
    """ get the log entries for a corresponding bank """
    LOG_ENTRIES = MC['logging']['LogEntry']
    entries = []
    ident = identifier(bank_id)
    log_id = make_id(ident, 'logging.Log')
    for log_entry in LOG_ENTRIES.find({"assignedLogIds": [log_id]}):
        if assessment_offered_id is None:
            entries.append(log_entry)
        else:
            # Filter for entries that match the offeredId only...
            # NOTE: Some of the offeredIds are NOT escaped...
            log_blob = json.loads(log_entry['text']['text'])
            if ('assessmentOfferedId' in log_blob and
                    ensure_escaped(
                        log_blob['assessmentOfferedId']
                    ) == assessment_offered_id):
                entries.append(log_entry)
    return entries


def ensure_escaped(osid_id):
    """ make sure that : => %3A and @ => %40 """
    if ':' in osid_id:
        return osid_id.replace(':', '%3A').replace('@', '%40')
    return osid_id


def make(bank_id, offered_id, path_to_db, db_number, has_headers):
    """Run through the results for an assessment offered"""
    MC = MongoClient(host='localhost', port=MONGO_DB_PORT)
    results = get_results(offered_id, MC)
    log_entries = get_log_entries(bank_id,
                                  MC,
                                  assessment_offered_id=offered_id)

    # Write results.csv file for this assessment offered:
    with open('results.csv', 'a') as csv_file:
        student_ids = []
        result_writer = csv.writer(csv_file, dialect='excel')

        if not has_headers and len(results) > 0:
            result_writer.writerow(make_results_header(results))
            has_headers = True
        for result in results:
            row = [path_to_db] + make_result_row(result, MC)
            student_ids.append(row[1])
            result_writer.writerow(row)

    # Write log.csv file for this assessment offered:
    write_log_data(db_number, path_to_db, 'log.csv', log_entries, student_ids)
    return has_headers


def is_valid_message(message, student_id, agent_id, generic_tool_log):
    """ For assessment logs (generic_tool_log = False), checks if
    the log entry has an `action` key and `assessmentOfferedId`, and
    agentId matches.
    For generic tool logs (generic_tool_log = True), just checks agentId """
    agent_id = get_identifier(agent_id)
    if generic_tool_log:
        return agent_id == student_id
    return ('assessmentOfferedId' in message and
            agent_id == student_id and
            'action' in message)


def write_log_data(db_number,
                   path_to_db,
                   filename,
                   log_entries,
                   student_ids,
                   generic_tool_log=False):
    """ Extract this out for convenience """
    # Write log.csv file for this assessment offered:
    student_ids = list(set(student_ids))
    student_ids.sort()
    with open(filename, 'a') as csv_file:
        log_writer = csv.writer(csv_file, dialect='excel')

        if db_number == 1:
            log_writer.writerow(['database_path', 'student_id',
                                 'timestamp', 'action', 'etc...'])
        for student_id in student_ids:
            entries = []
            for log_entry in log_entries:
                message = json.loads(log_entry['text']['text'])
                if is_valid_message(message,
                                    student_id,
                                    log_entry['agentId'],
                                    generic_tool_log):
                    entries.append(get_datetime(log_entry['timestamp']))
                    if generic_tool_log:
                        entries.append('{0} -- {1}'.format(
                            message['event_type'],
                            json.dumps(message['params'])))
                    elif 'action' in message:
                        entries.append(message['action'])
                    else:
                        entries.append(message)
            log_writer.writerow([path_to_db, student_id] + entries)


def make_results_header(results):
    """return the header, list of strings to appear as column headings"""

    header = ['database_path',
              'student_id',
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


def get_text_from_texts(texts):
    """ grab the english one if available otherwise return first one """
    english_language_type_id = '639-2%3AENG%40ISO'
    eng = [t['text'] for t in texts
           if t['languageTypeId'] == english_language_type_id]
    text_markup = ''
    if eng:
        text_markup = eng[0]
    else:
        text_markup = texts[0]['text']
    soup = BeautifulSoup(text_markup, 'lxml')
    if any(obj_tag in text_markup for obj_tag in ['audio', 'video', 'img']):
        # Because we probably want to know the details of the
        #   embedded object
        return str(soup)
    return soup.get_text()


def get_item(item_id, MC):
    """ return the given item from all the items """
    ITEMS = MC['assessment']['Item']

    item = ITEMS.find_one({"_id": ObjectId(identifier(item_id))})
    return item


def get_question_text(question, MC):
    """ get the item's question text from the list of items """
    item = get_item(question['itemId'], MC)
    question_texts = item['question']['texts']
    return get_text_from_texts(question_texts)


def get_submission_times(question):
    """ return a stringified version of all the submission times,
        in reverse chronological order """
    if (question['responses'] and
            'missingResponse' not in question['responses'][0]):
        times = [str(get_datetime(
            question['responses'][0]['submissionTime']))]
    else:
        times = ['None']
    if len(question['responses']) > 1:
        for additional_attempt in question['responses'][1::]:
            times.append(str(get_datetime(
                additional_attempt['submissionTime'])))
    return ','.join(times)


def get_choice_text(choice_id, item):
    """ grab the choice text """
    choice_texts = [c['texts']
                    for c in item['question']['choices']
                    if c['id'] == choice_id][0]
    return get_text_from_texts(choice_texts)


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
    file_data = file_dict[label]
    filename = get_identifier(file_data['assetContentId'])
    return '{0}.{1}'.format(filename, guessed_extension)


def format_file_name_for_single_file_upload(file_dict, MC):
    """ Return a formatted file name by guessing at the extension from the
    file label. For example, for:

    'fileIds': {
        u 'My Project (1)_tb': {
            u 'assetId': u 'repository.Asset%3A5a27dea1eb5c87013410532a%40ODL.MIT.EDU',
            u 'assetContentTypeId': u 'asset-content-genus-type%3Atb%40ODL.MIT.EDU'
        }
    }

    This method would look up Asset matching the ID, find the AssetContent
    matching the `assetContentTypeId`, and return `5a27dea1eb5c87013410532c.tb`.

    This is needed for ART submissions prior to a March 2018 QBank update
    that forced response records into a standard format. The pre-March 2018
    data used FileFormRecord instead of FilesFormRecord, which stores
    data in _my_map differently.

    NOTE: we'll assume only a single file here, since this should only be used
    with responses, which take one file right now.
    """
    guessed_extension = get_identifier(file_dict['assetContentTypeId'])
    # Get the Asset from repository service, and then
    #   find the AssetContent with the matching `assetContentTypeId`.
    # Need this for pre-March 2018 data because those responses
    #   used FileFormRecord instead of FilesFormRecord.
    ASSETS = MC['repository']['Asset']
    asset_id = ObjectId(get_identifier(file_dict['assetId']))
    asset = ASSETS.find_one({'_id': asset_id})
    ac = [ac for ac in asset['assetContents']
          if ac['genusTypeId'] == file_dict['assetContentTypeId']]
    if len(ac) == 0:
        raise LookupError('No asset content of that type')
    filename = str(ac[0]['_id'])
    return '{0}.{1}'.format(filename, guessed_extension)


def get_attempts(question, MC):
    """ from the list of items, get the actual choice text
        for each attempt """
    item = get_item(question['itemId'], MC)
    if (question['responses'] and
            'missingResponse' not in question['responses'][0]):
        response = question['responses'][0]
        if 'choiceIds' in response:
            # Need to account for multiple choice, MW sentence, etc.
            #   So may have more than one `choiceId`.
            attempts = [' '.join(
                [get_choice_text(choice_id, item)
                 for choice_id in response['choiceIds']])]
        elif 'text' in response:
            attempts = [response['text']['text']]
        elif 'fileIds' in response and response['fileIds'] != {}:
            # non-empty file submission
            attempts = [format_file_name(response['fileIds'])]
        elif 'fileIds' in response and response['fileIds'] == {}:
            attempts = ['empty file response']
        elif 'fileId' in response and response['fileId'] != {}:
            # non-empty file submission pre-March 2018
            attempts = [format_file_name_for_single_file_upload(
                response['fileId'],
                MC)]
        elif item['genusTypeId'] == 'item-genus-type%3Aqti-extended-text-interaction%40ODL.MIT.EDU':
            # text-interaction but no text submitted -- due to old qbank bug?
            attempts = ['No response recorded']
        else:
            raise TypeError('need to account for new response type,',
                            response)
    else:
        attempts = ['None']
    if len(question['responses']) > 1:
        for additional_attempt in question['responses'][1::]:
            if 'choiceIds' in additional_attempt:
                attempts.append(' '.join(
                    [get_choice_text(choice_id, item)
                     for choice_id in additional_attempt['choiceIds']]))
            elif 'text' in additional_attempt:
                attempts.append(additional_attempt['text']['text'])
            elif ('fileIds' in additional_attempt and
                  additional_attempt['fileIds'] != {}):
                # non-empty file submission
                attempts.append(
                    format_file_name(additional_attempt['fileIds']))
            elif ('fileIds' in additional_attempt and
                  additional_attempt['fileIds'] == {}):
                attempts.append('empty file response')
            elif ('fileId' in additional_attempt and
                  additional_attempt['fileId'] != {}):
                # non-empty file submission pre-March 2018
                attempts.append(format_file_name_for_single_file_upload(
                    additional_attempt['fileId'],
                    MC))
            elif item['genusTypeId'] == 'item-genus-type%3Aqti-extended-text-interaction%40ODL.MIT.EDU':
                # text-interaction but no text submitted -- due to old qbank bug?
                attempts.append('No response recorded')
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


def make_result_row(result, MC):
    """return one row representing one result"""

    row = [get_identifier(result['takingAgentId']),
           result['displayName']['text'],
           get_datetime(result['actualStartTime']),
           get_datetime(result['completionTime']),
           ]

    for question in result['sections'][0]['questions']:
        row += [get_question_text(question, MC),  # grab the item text
                get_submission_times(question),
                get_attempts(question, MC)]  # grab the choice text
        # get_correctness(question)]  # grab the correctness
        # This is not useful in /results
        # because always True
    return row


def get_datetime(dt_obj):
    """ if a regular python datetime is not provided, None is returned """
    if dt_obj is None:
        return 'None'
    else:
        return dt_obj
