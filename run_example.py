nohup tail -F ~/log/*.stat | ./mxstat -i &
nohup tail -F ~/log/*.log | ./mxlog -i &
nohup ./psps -i 10 -c 65535 &
nohup ./sarsar &
