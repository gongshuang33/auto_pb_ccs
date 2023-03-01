# 检查PB_data.json是否是写入状态
cd /home/gongshuang/pb

while :
do
if [ ! -f 'logs/WRITE' ];then
    break
fi
echo 'PB_data.json may be writing. Wait 5s...'
sleep 5s
done




#while :
## 定时任务1
## 由于使用crontab 启动，故注释循环
#do

# 检查PB_data.json是否是写入状态
while :
do
if [ ! -f 'logs/WRITE' ];then
    break
fi
echo 'PB_data.json may be writing. Wait 5s...'
sleep 5s
done

touch logs/WRITE

# 初始化 bam.list
# 检查新下机hifi.bam, json格式存储
# 检查新下机的subreads.bam，json格式存储
echo 'start:' `hostname`  `date`

# 运行之前先备份json
time=$(date "+-%Y-%m-%d-%H-%M-%S")

cp /home/gongshuang/pb/logs/Pacbio_data.json  /home/gongshuang/pb/logs/Pacbio_data.json'.bak'$time

echo 'find_new_bam.sh start'
sh /home/gongshuang/pb/core/data_receive/find_new_bam.sh
echo 'find_new_bam.sh end'


sleep 15
# 执行：生成新下机的bam工作目录以及运行脚本，并qsub投递，返回该qsub的日志文件路径，qsubID。记录该bam的json状态。
echo 'data_pre_v2.py start'
/home/gongshuang/Python3/bin/python3.7 /home/gongshuang/pb/core/data_run/data_pre_v2.py /home/gongshuang/pb/logs/Pacbio_data.json /pfs/Sequencing/Nanopore.temp/check/PB_CCS/auto 20
echo 'data_pre_v2.py end'

sleep 20
echo 'data_run.py start'
/home/gongshuang/Python3/bin/python3.7 /home/gongshuang/pb/core/data_run/data_run.py /home/gongshuang/pb/logs/Pacbio_data.json /home/gongshuang/pb/queue /home/gongshuang/pb/done /home/gongshuang/pb/run_log
echo 'data_run.py end'
echo 'end:' `hostname` `date`
echo '------------------------------------------------------------------------------------'

rm logs/WRITE

##sleep 1800


## 结束
#done
