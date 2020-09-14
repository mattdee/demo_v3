
for i in {1..5}

do python3 file_worker.py worker_$(hostname)_$i &

done
