import sys
import datetime
import MySQLdb # to use cursors and control the transaction
from prettytable import PrettyTable

# Connect to MemSQL
host = '127.0.0.1'
db = ''
user = 'root'
pwd =''
port = '3306'
conn = MySQLdb.connect(host,user,pwd,db )
cursor = conn.cursor()

# Create database and table
try:
    create_db =     '''             
                    create database if not exists test
                    '''
    create_tbl =    '''
                    create table if not exists test.t(id bigint AUTO_INCREMENT primary key, indate datetime(6))
                    '''
    cursor.execute(create_db)
    cursor.execute(create_tbl)
except Exception:
    print("Something broke.... >.<")
    exit(1)

# Get a range of rows to insert
try:
    if len(sys.argv) > 1:
        work_range = int(sys.argv[1])
    else:
        raw_range = input("Enter a number to insert: ")
        work_range = int(raw_range)
except Exception:
    print("Need a number to insert!")
    print("Exiting...")
    exit(1)


for i in range(work_range):
    try:
        new_data = []
        now = str(datetime.datetime.now())
        new_data.append(now)

        insert_data = "insert into test.t(indate) values " + ''.join(["('%s')" % x for x in new_data])
        cursor.execute(insert_data)
        conn.commit()

    except Exception:
        conn.rollback()
        new_data.clear()

# Print a pretty table of the tuples
query_data  =   '''
                select * from test.t order by id;
                '''
table = PrettyTable(["ID","InDate"])
cursor.execute(query_data)
for row in cursor.fetchall():
    table.add_row(row)
print(table)








