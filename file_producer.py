'''
Author: Matt D (mattdee@gmail.com)
Purpose: Create files in s3 and insert metadata for file processing

create database if not exists aero;use aero;
drop table if exists file_metadata;
create table file_metadata
(
    file_id bigint primary key,
    file_URI longtext,
    file_status int, /* 0 = ready, 1 = processing, 2 = complete */
    file_create datetime(6), /* when the file is created locally */
    file_upload_start datetime(6), /* start of the upload to s3 */
    file_upload_end datetime(6), /* end of the upload to s3 */
    file_update datetime(6), /* timestamp when worker updates file */
    file_complete datetime(6), /* timestamp when worker completes processing file */
    file_download_start datetime(6), /* timestamp when worker starts download file */
    file_download_end datetime(6), /* timestamp when worker ends download file */
    file_worker longtext,
    shard key (file_id)
)
;
'''

import sys
from memsql.common import database
import boto3
import boto.connection
import json
import datetime
import random
import MySQLdb # to use cursors and control the transaction

# Connect to MemSQL
host = '127.0.0.1'
db = ''
user = 'root'
pwd =''
port = '3306'
#conn = database.connect(host=host,port=port,user=user,database=db,password=pwd)

conn = MySQLdb.connect(host,user,pwd )
cursor = conn.cursor()


# keys for s3 access
access_key = ''
secret_key = ''
# s3 bucket
s3_bucket = 'aero-poc'

# file status
# 0 = ready, 1 = processing, 2 = complete
file_status = 0

# Get range
try:
    if len(sys.argv) > 1:
        work_range = int(sys.argv[1])
    else:
        raw_range = input("Enter number of files to create: ")
        work_range = int(raw_range)
except Exception:
    print("Need a number for files to create!")
    print("Exiting...")
    exit(1)

# create the database
create_db = '''
create database if not exists aero;use aero;
create table if not exists file_metadata
(
    file_id bigint primary key,
    file_URI longtext,
    file_status int, /* 0 = ready, 1 = processing, 2 = complete */
    file_create datetime(6), /* when the file is created locally */
    file_upload_start datetime(6), /* start of the upload to s3 */
    file_upload_end datetime(6), /* end of the upload to s3 */
    file_update datetime(6), /* timestamp when worker updates file */
    file_complete datetime(6), /* timestamp when worker completes processing file */
    file_download_start datetime(6), /* timestamp when worker starts download file */
    file_download_end datetime(6), /* timestamp when worker ends download file */
    file_worker longtext,
    shard key (file_id)
)
;
'''

try:
    conn.query(create_db)
except Exception:
    print("Yikes...couldn't create the database!")
    exit(1)

for i in range(work_range):
    try:
        csv_list = []
        file_id = random.randrange(1, 999999).__str__()
        file_name = (file_id + '.csv')
        file_URI = (s3_bucket+'/'+file_name)
        print(file_name)

        for x in range(74900):
            #print(x)
            csv_numb = random.randrange(100000,999999)
            csv_list.append(csv_numb)

        file_content = ','.join(map(str,csv_list))

        try:
            file_creation = str(datetime.datetime.now())
            file_upload_start = str(datetime.datetime.now())
            # insert file create and upload timestamp
            file_create =   '''
                                insert into aero.file_metadata
                                (file_id,file_URI,file_status,file_create,file_upload_start)
                                values
                                (%s,'%s',%s,'%s','%s')
                            ''' % (file_id,file_URI,file_status,file_creation,file_upload_start)
            cursor.execute(file_create)
            conn.commit()

        except Exception:
            conn.rollback()

        try:
            # create the s3 file
            s3 = boto3.resource('s3')
            obj = s3.Object(s3_bucket, file_name)
            obj.put(Body = file_content )

            file_upload_end = str(datetime.datetime.now())
            update_file =   """
                                    update aero.file_metadata
                                    set
                                    file_upload_end = '%s'
                                    where file_id = %s 
                                """ % (file_upload_end,file_id)

            cursor.execute(update_file)
            conn.commit()
        except Exception:
            conn.rollback()

    except Exception:
        conn.rollback()
        sys.exit()

