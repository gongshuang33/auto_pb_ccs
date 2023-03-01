set -vex

# $1 Result 目录绝对路径
# $2 subreads 路径

if [ ! ${1} ];then
    exit 0
fi
if [ ! $2  ];then
    exit 0
fi

date

/home/gongshuang/Python3/bin/python3 /home/gongshuang/pb/tools/lims/ii_report_data.py  ${1} $2 >> ${1}/../ii_report_data.log 2>&1

date
