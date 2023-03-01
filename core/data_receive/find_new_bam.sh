set -e
cd /home/gongshuang/pb

#log_dir='/export/personal1/gongshuang/PB_CCS/auto_qsub/pb/core/data_receive/tmp'
log_dir='/home/gongshuang/pb/logs'
#work_dir='/export/personal1/gongshuang/PB_CCS/auto_qsub/pb/core/data_receive/tmp'
work_dir='/home/gongshuang/pb/logs'
#json_file_dir='/export/personal1/gongshuang/PB_CCS/auto_qsub/pb/core/data_receive/tmp'
json_file_dir='/home/gongshuang/pb/logs'

old_hifi_list=$log_dir/old.hifi.list
old_subreads_list=$log_dir/old.subreads.list

new_hifi_list=$log_dir/new.hifi.list
new_subreads_list=$log_dir/new.subreads.list

json_file=$json_file_dir/Pacbio_data.json


# 1. init old.*.list

if [ ! -f $old_subreads_list  ];then
    # find old hifi_reads
    realpath /pfs/Sequencing/Pacbio/Sequel_ii*/*/*/*/*.hifi_reads.bam  | sort > $old_hifi_list
    # find old subreads
    realpath /pfs/Sequencing/Pacbio/Sequel_ii*/*/*/*/*.subreads.bam | sort > $old_subreads_list
    echo '1'
fi

# 2. init json file
if [ ! -f $json_file ];then
    /home/gongshuang/Python3/bin/python3.7 /home/gongshuang/pb/core/data_receive/addToJson.py -i $old_hifi_list $old_subreads_list -o $json_file -c 1
fi


# 3. get current bam list
curr_time=`date +%Y%m%d%H%M%S`
curr_new_hifi=$log_dir/${curr_time}_new_hifi.list
curr_new_subreads=$log_dir/${curr_time}_new_subreads.list

#echo $curr_time

realpath /pfs/Sequencing/Pacbio/Sequel_ii*/*/*/*/*.hifi_reads.bam  | sort > $new_hifi_list
realpath /pfs/Sequencing/Pacbio/Sequel_ii*/*/*/*/*.subreads.bam | sort > $new_subreads_list

comm -13 $old_hifi_list $new_hifi_list > $curr_new_hifi
comm -13 $old_subreads_list $new_subreads_list > $curr_new_subreads

cat $curr_new_hifi
cat $curr_new_subreads

mv $new_hifi_list $old_hifi_list
mv $new_subreads_list $old_subreads_list

/home/gongshuang/Python3/bin/python3.7 /home/gongshuang/pb/core/data_receive/addToJson.py -i $curr_new_hifi $curr_new_subreads -o $json_file -c 0
















