import os
from settings import OUTPUT_DIR, MONGO_DB_HOST, MONGO_DB_PORT
import pexpect
import tarfile

def compress_all_data():

    """ compress all qbank json dump files + student

    activity file into a single .tar.gz file """

    output_file = 'qbank_data.tar.gz'

    output_path = os.path.join(OUTPUT_DIR, 'qbank', output_file)

    with tarfile.open(output_path, 'w:gz') as tar:

        mongo_dump_dir = os.path.join(OUTPUT_DIR, 'qbank', 'mongo-dump')

        activity_dump_dir = os.path.join(OUTPUT_DIR, 'qbank', 'student-activities')

        tar.add(mongo_dump_dir, arcname=os.path.basename(mongo_dump_dir))

        #tar.add(activity_dump_dir, arcname=os.path.basename(activity_dump_dir))


def export_qbank_data():

    """ export the qbank data from mongo as json """

    output_qbank_dir = os.path.join(OUTPUT_DIR, 'qbank', 'mongo-dump')

    if not os.path.exists(output_qbank_dir):

        os.makedirs(output_qbank_dir)

    for database in ['assessment', 'assessment_authoring',

                     'hierarchy', 'id', 'logging',

                     'relationship', 'repository',

                     'resource']:

        command = 'mongodump --host {0} --port {1} -d {2} -o \'{3}\''.format(

            MONGO_DB_HOST,

            str(MONGO_DB_PORT),

            database,

            output_qbank_dir

        )

        p = pexpect.spawn(command,

                          timeout=120)

        p.expect('done dumping')

        p.close()

if __name__ == '__main__':

    export_qbank_data()
    output_filename = compress_all_data()

    print('Export all qbank data as {0}'.format(output_filename))


