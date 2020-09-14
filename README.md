## Contents

### Questions/comments - Matt D (mattdee@gmail.com)


1. run_consumers.sh - shell script to create multiple file workers
2. file_producer.py - python script to create files in s3 and register with database
3. file_worker.py - python script to process files.  Workers all access database for files without a current owner, register file as owned, process file, and finally mark the file as complete
4. microsecond_test.py - example of creating microsecond timestamps
5. make_files.sh - shell script to create multiple file producers
6. metrics.sql - example metrics for file metadata
7. packages - packages needed to run demo 
8. README.md - This file