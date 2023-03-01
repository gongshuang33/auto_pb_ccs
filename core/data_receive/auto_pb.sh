cd /export/personal1/gongshuang/PB_CCS/auto_qsub/pb/core/data_receive
set -e
while : 
do
    sh find_new_bam.sh
    pid=`ps gaux | grep auto_pb.sh | grep -v grep | awk '{print $2}'`
    echo '10分钟更新一次：'`hostname` `pwd` `date` '进程号:' $pid	
    sleep 600
done
