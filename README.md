./influxc.py -d mydb -L5 -W 'ps,host=z50,mod=mss,mod=mta cpu=0.33,vsz=40001,rss=40002'
./influxc.py -d mydb -L5 -W 'ps,host=z50,mod=mss,mod=mta cpu=0.43,vsz=40001,rss=40002'
./influxc.py -d mydb -L5 -W 'ps,host=z30,mod=mss,mod=mta cpu=0.43,vsz=40001,rss=40002'
./influxc.py -d mydb "SELECT *   from ps where host='z50' "
./influxc.py -d mydb 'select "%usr" from sar where SEQ='"'"'0'"'"
