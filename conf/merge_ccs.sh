#set -vex

#dir='/home/gongshuang/work/PB/20230213-64447e-C01-PBL230108/m64447e_230216_022128'
dir=$1
#movie='m64447e_230216_022128'
movie=$2
#chunknum=15
chunknum=$3



cd $dir

while :
do
cd $dir

num=`ls */* |grep -c done$ `

echo -e "当前目录："`pwd` "节点：" `hostname` "时间："`date` "\t完成数量:" $num "\n"

if (( $num == $chunknum  ));then

  set -vex

  cd $dir
  /home/smrtanalysis/software/smrtlink_11.1.0.166339/install/smrtlink-release_11.1.0.166339/smrtcmds/bin/pbmerge -o ${movie}.hifi_reads.bam ${dir}/ccs.*/${movie}.*.hifi.bam

  touch merge_done

  echo "合并完成：" `date`

break
exit 0

fi;


sleep 600s

done
