nohup cat ../stats/*.stat | ./mxstat -d mydb -S30 &
nohup cat ../logs/mta*.log | ./mxlog  -d mydb -S30 &
nohup cat ../logs/imap*.log | ./mxlog  -d mydb -S30 &
nohup cat ../logs/pop*.log | ./mxlog  -d mydb -S30 &
#nohup ./psps -d mydb -S30 -i 10 -c 65535 &
#nohup ./sarsar -d mydb -S30 &
