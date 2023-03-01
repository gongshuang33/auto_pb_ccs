set -e

Pacbio_data_json='/home/gongshuang/pb/logs/Pacbio_data.json'
logs_path='/home/gongshuang/pb/logs/'

cd /home/gongshuang/pb/core/data_update

while :
do
      if [ ! -f $logs_path/WRITE ];then
          break
      fi
      echo [`date`] 'Pacbio_data.json may be writing.wait 5s...'
      sleep 5s
done


touch $logs_path/WRITE 

{
    echo [`date`] 'Update start.' && \
    /home/gongshuang/Python3/bin/python3.7 getStatus.py $Pacbio_data_json && \
    rm $logs_path/WRITE && \
    echo [`date`] 'Update done.'
} || {
    echo [`date`] 'Update Error.' &&\
    rm $logs_path/WRITE
}


