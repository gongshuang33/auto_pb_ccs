*/7 * * * * sh /home/gongshuang/pb/core/data_update/run_update.sh >> /home/gongshuang/pb/core/data_update/update.log 2>&1
*/30 * * * * sh /home/gongshuang/pb/run.sh /home/gongshuang/pb/logs/Pacbio_data.json >> /home/gongshuang/pb/run.log 2>&1 
0 * * * * sh /home/gongshuang/pb/core/logs/run_monitor.sh >> /home/gongshuang/pb/core/logs/log.txt 2>&1
