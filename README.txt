# queue  临时队列，对已经建立好目录的cell重新投递，内容，单列movie号

# logs  检测新subreads的结果文件以及数据cell的json文件

# Pacbio_data.json 质控结果文件【重要！】

# run_log  自动化投递过的cell信息（不一定运行完，会有重复）

# nohup.out 自动化脚本运行情况

# old.subreads.list 截至当前，已有的subreads路径会存在里面。若要重新跑某个cell，可根据movie号找到对应行并删除，然后直接sh run.sh。


