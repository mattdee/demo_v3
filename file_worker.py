'''
Author: Matt D (mattdee@gmail.com)
Purpose: Get files from metadata catalog for processing

'''

import sys
from memsql.common import database
import boto3
import boto.connection
import json
import datetime
from time import sleep
import random
import MySQLdb # to use cursors and control the transaction

# Connect to MemSQL
host = '127.0.0.1'
db = 'aero'
user = 'root'
pwd =''
port = '3306'
#conn = database.connect(host=host,port=port,user=user,database=db,password=pwd)

conn = MySQLdb.connect(host,user,pwd,db )
cursor = conn.cursor()

# keys for s3 access
access_key = ''
secret_key = ''
# s3 bucket
s3_bucket = 'aero-poc'

# Get worker ID
try:
    if len(sys.argv) > 1:
        file_worker = sys.argv[1]
    else:
        file_worker = input("Enter worker name: ")
except Exception:
    print("Worker name is needed!")
    print("Exiting...")
    exit(1)


while True:
    try:
            file_status = """
                                select file_id,file_URI 
                                from 
                                file_metadata 
                                where file_status = 0 
                                order by rand();
                            """
            cursor.execute(file_status)
            for open_status in cursor.fetchall():
                file_id = open_status[0]
                file_URI = str.encode(open_status[1],'utf-8')
                # print(status)
                # print(file_id)
                # print(file_URI)

                file_update_stamp = str(datetime.datetime.now())
                update_record = """ 
                                    update file_metadata 
                                    set 
                                        file_status = 1, /* 0 = ready, 1 = processing, 2 = complete */
                                        file_worker = "%s",
                                        file_update = '%s'
                                        where file_id = %s;
                                    """ % (file_worker,file_update_stamp,file_id)

                cursor.execute(update_record)
                conn.commit()
    except:
            conn.rollback()

    # update the file
    # get all the files processed by this worker
    try:
        get_files = """
                        select file_id,file_URI 
                        from 
                        file_metadata 
                        where 
                        file_status = 1 and file_worker = '%s'; 
                    """ % (file_worker)

        cursor.execute(get_files)

        for close_status in cursor.fetchall():
            closer = []
            closer.append(close_status[0])

            #print(closer[0])
            for record in closer:
                #print(record)
                close_id = record
                print(str(close_id))

                # update the recore with file_download_start timestamp
                file_download_start = str(datetime.datetime.now())
                start_download = """
                                    update file_metadata
                                    set
                                    file_download_start = '%s',
                                    file_worker = "%s"
                                    where file_id = %s;
                                """ % (file_download_start,file_worker, close_id)

                print(start_download)

                cursor.execute(start_download)
                conn.commit()


                # print("Marking file id %s as complete.") % (str(close_id))

                file_name = (str(close_id) + '.csv')

                # # file processing logic here
                # # just list the file_id and contents

                s3 = boto3.resource('s3')
                fileobj = s3.Object(s3_bucket, file_name).get()['Body']
                file_content = []

                for x in range(100):
                    file_chunk = fileobj.read(1)
                    file_content.append(file_chunk)

                    strIt = ''.join(str(z) for z in file_content)

                    removeb = strIt.split("'b'")

                    real_content = ''.join(str(q) for q in removeb)

                    print(file_name)
                    print(file_content)
                    file_download_end = str(datetime.datetime.now())
                file_content.clear()

            file_complete_stamp = str(datetime.datetime.now())
            close_record = """
                                update file_metadata
                                set
                                file_status = 2, /* 0 = ready, 1 = processing, 2 = complete */
                                file_worker = "%s",
                                file_complete = '%s',
                                file_download_end = '%s'
                                where file_id = %s;
                            """ % (file_worker,file_complete_stamp,file_download_end,close_id)

            print(close_record)

            cursor.execute(close_record)
            conn.commit()

            closer.clear()
    except:
        conn.rollback()
