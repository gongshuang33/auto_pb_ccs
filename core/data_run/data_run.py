#coding=utf-8
import sys
import os
import re
import stat
import json
import subprocess
import argparse


def file_process(cmd):
    """
    命令行执行
    :param cmd: 命令
    :return: 命令执行返回值
    """
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='utf-8', executable='/bin/bash')
    # 获取返回值
    out, err = p.communicate()
    return_code = p.returncode
    # 成功判断和失败报警
    if return_code == 0:
        if out != '':
            return out
        return True
    else:
        #ilogger.error(out)
        return False

# 得到文件（只有一列）的内容到列表中
def get_list_from_file(infile):
    fin = open(infile, "r", encoding='utf-8')
    lists = fin.readlines()
    fin.close()
    alist = []
    if len(lists) > 0:
        for i in lists:
            i = i.strip()
            alist.append(i)
    return alist

# 将列表写入文件
def write_list_to_file(outfile, alist):
    fout = open(outfile, "w", encoding='utf-8')
    if len(alist) > 0:
        da = '\n'.join(alist)
        fout.write(da)
    else:
        fout.write('')
    fout.close()
    return alist

# 初始化queue队列
def init_queue(queue_file, cell_done, db):
    movies = get_list_from_file(queue_file)
    done_cells = get_list_from_file(cell_done)
    for m in db.keys():
        if str(db[m]['code']) == 'ccs_ready' and db[m]['status'] == 'only_subreads' and m not in movies and db[m]['runwell'] not in done_cells:
            #print(m)
            movies.append(m)
            db[m]['code'] = 'queue'
    write_list_to_file(queue_file, movies)
    #print('2')
    return db


#投递run_ccs.sh主脚本，获取主脚本的JOBID
#读入主脚本的日志，获取主脚本投递的子脚本JOBID
def qsub_ccs(queue_file, cell_done, data, logfile):
    print('qsub_run')
    movies = get_list_from_file(queue_file)
    movies = list(set(movies))
    print("Program will qsub these cells:", movies)
    db = data.copy()
    qw_num = file_process('/opt/sge/bin/lx-amd64/qstat | grep -c  qw ')
    print('qw num: {}'.format(str(qw_num)))
    #该处根据“qw”数目来限制投递的数目（暂未设置）
    if qw_num == 'NULL':
        return data
    elif int(qw_num) > 9999: # 暂未设置
        return data
    else:
        for i in movies:
            try:
                workdir = data[i]['ccs']['path']
                qsub_re = file_process(f"cd {workdir} && /opt/sge/bin/lx-amd64/qsub -S /usr/bin/bash -V -cwd -q ccs.q -pe smp 1 run_ccs.sh && touch qsub_done")
                qsub_id, shellname = re.findall('Your job (.*) \(\"(.*)\"\) has been submitted', qsub_re)[0]
                file_process(f"cd {workdir} && mv qsub_done qsub_{qsub_id}")
                data[i]['code'] = 'ccs_running'
                data[i]['ccs']['code'] = 'ccs_running'
                data[i]['ccs']['qsub_id'] = qsub_id
                data[i]['ccs']['script_file'] = os.path.abspath(workdir) + '/' + shellname
                #data[i]['ccs']['stderr'] = get_stderr_path(qsub_id)
                #data[i]['ccs']['stdout'] = get_stdout_path(qsub_id)
                #data[i]['ccs']['sub_jobs'] = get_subjobs_id(qsub_id, data[i]['ccs']['stderr'])
                open(logfile, 'a').write("{runwell}\t{movie}\t{path}\t{subreads}\n".format(runwell=data[i]['runwell'], movie=i, path=workdir, subreads=data[i]['subreads']))
                #open(log, 'a').write("{movie}\t{subreads}\t{workpath}\t{runwell}\n".format(movie=i, subreads=data[i]['subreads'], workpath=workdir, runwell=data[i]['runwell']))
            except:
                pass
        write_list_to_file(queue_file, [])
        return data



if __name__ == "__main__":
    
    #jsonfile = '/home/gongshuang/pb/core/data_receive/tmp/Pacbio_data.json'
    jsonfile = sys.argv[1]
    #queue_file = '/home/gongshuang/pb/core/data_run/tmp/run_queue'
    queue_file = sys.argv[2]
    #cell_done = '/home/gongshuang/pb/core/data_run/tmp/cell_done'
    cell_done = sys.argv[3]
    #logfile = ''
    logfile = sys.argv[4]
 
    data = json.load(open(jsonfile, 'r'),encoding='utf-8')
    
    data = init_queue(queue_file, cell_done, data)
    
    data = qsub_ccs(queue_file, cell_done, data, logfile)

    #a = file_process('/opt/sge/bin/lx-amd64/qstat | grep -c  qw ')

    #print(int(a))
    json.dump(data, open(jsonfile, 'w'), indent=2)
