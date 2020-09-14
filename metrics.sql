use aero;


select count(*) from file_metadata;


select avg(file_upload_end - file_upload_start) from file_metadata;
select avg(file_download_end -file_download_start) from file_metadata;


select count(*) work_completed,file_worker,avg(file_upload_end - file_upload_start) upload_times ,avg(file_download_end -file_download_start) download_times from file_metadata
group by file_worker;
