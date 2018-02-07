#!/bin/sh
while true; do
    c=0
    for i in `ps -ef |grep -v grep|grep -v kill_py.sh |grep -a $1|grep -a $2|awk '{print $2}'`
    do
        kill -9 $i
        c=$[c+1]
    done
    if [ $c -eq 0 ]; then
        break
    fi
done
