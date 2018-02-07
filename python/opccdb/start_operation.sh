#!/bin/sh
set -euv
debug=False
if [[ $# -eq 2 ]]; then
    if [[ $2 == debug ]]; then
        debug=True
    fi
fi
if [ -f operation.out ]; then
        TIMESTR=`date +%Y%m%d_%H:%M:%S`
        mv operation.out operation.out.$TIMESTR
fi
sh  kill_py.sh BailingOperation.py $1
echo $1
nohup limit `which python` BailingOperation.py $1 </dev/null >operation.out 2>&1 & 
pid_num=`ps -ef |grep -a "python BailingOperation.py $1"|wc -l`

if [ $pid_num -ne 0 ];then
    echo "succeed to BailingOperation.py $1 "
else
    echo "fail to BailingOperation.py $1"
fi
