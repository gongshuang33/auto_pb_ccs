set -e

while :
do

echo [ `date` ] ....... start

MYDIR=`dirname $0`

old_subreadset_xml=$MYDIR/old_subreadset_xml
new_subreadset_xml=$MYDIR/new_subreadset_xml

# 1. init old.*.list

if [ ! -f $old_subreadset_xml ];then
    realpath /pfs/Sequencing/Pacbio/Sequel_ii*/*/*/*/*.subreadset.xml | sort > $old_subreadset_xml
fi


# 3. get current bam list
curr_time=`date +%Y%m%d%H%M%S`
curr_new_subreadset=$MYDIR/${curr_time}_new_subreadset_xml

#echo $curr_time

realpath /pfs/Sequencing/Pacbio/Sequel_ii*/*/*/*/*.subreadset.xml | sort > $new_subreadset_xml

comm -13 $old_subreadset_xml $new_subreadset_xml > $curr_new_subreadset


mv $new_subreadset_xml $old_subreadset_xml

python3 $MYDIR/data_import.py $curr_new_subreadset

rm $curr_new_subreadset

echo [ `date` ] ....... end

sleep 1800s

done














