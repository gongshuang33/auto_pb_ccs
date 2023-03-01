set -vex

while : 
do
    sh find_new_bam.sh
    echo `hostname` `date`	
    sleep 1800
done
