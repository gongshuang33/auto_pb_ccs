set -vex

work_dir='/pfs/Sequencing/Nanopore.temp/check/PB_CCS/auto'
json_file='/home/gongshuang/pb/core/data_receive/tmp/Pacbio_data.json'


if [ ! -f "$json_file" ] || [  ! -d "$work_dir" ] ;then
    exit -1
fi

python3 data_run.py -j $json_file -p $work_dir 
