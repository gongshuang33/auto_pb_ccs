#coding=utf-8
import os
import stat
import json
import subprocess
import sys

# 为每一个还没有开始质控的cell建立工作目录
# 返回[路径,code]
def pre_work(movie, wd, subreads, chunk_num, run_id, runwell):
    workplace = wd + '/' + runwell
    ccs_path = workplace + '/' + movie
    subreadspath = subreads
    file_process(f'mkdir -p {workplace}')
    run_ccs_sh = f"set -vex \nhostname\ndate\n\ncd {workplace}\nif [ ! -f '{workplace}/Result/collect_result_done' ];then\n    python3 /home/gongshuang/pb/tools/pbccs/run_ccs.py --workdir {workplace} --subreads_info subreads_info.txt --min_passes 3 --min_rq 0.99 --max_length 100000 --min_length 100 -c {chunk_num}\nfi\ndate\n\ntouch Call_CCS_DONE\n\nsh /home/gongshuang/work/report_to_lims.sh {workplace}/Result {subreadspath} \ndate\n\n" 
    subreads_info_txt = f"{movie}\t{subreads}"
    open(workplace + '/run_ccs.sh', 'w').write(run_ccs_sh)
    open(workplace + '/subreads_info.txt','w').write(subreads_info_txt)
    return [workplace, 'ready']


def pre_ccs(db, chunk_num, workdir):
    #print(type(db))
    for m in db.keys():
        #过滤掉文库编号异常异常的subreads
        if 'Sample' in db[m]['runwell'] or 'Name' in db[m]['runwell']:
            db[m]['code'] = 'library_error'
            continue
        if db[m]['code'] == 0 and db[m]['status'] == 'only_subreads':
            slist = pre_work(m, workdir, db[m]['subreads'], chunk_num, m[-6:], db[m]['runwell'])
            db[m]['ccs']['path'] = slist[0]
            db[m]['ccs']['code'] = slist[1]
            db[m]['code'] = 'ccs_ready'
            db[m]['ccs']['chunk_num'] = chunk_num
        elif db[m]['ccs']['code'] == 0 and db[m]['status'] == 'both':
            pass
        elif db[m]['ccs']['code'] == 0 and db[m]['status'] == 'only_hifi':
            pass
        else:
            pass
    return db


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
        logger.error(out)
        return False


if __name__ == "__main__":
    jsonfile = sys.argv[1]
    
    #print(pre_work('m64144_230218_221218', '/pfs/Sequencing/Nanopore.temp/check/PB_CCS/auto', '/pfs/Sequencing/Pacbio/Sequel_ii/sequel_ii_r64144/r64144_20230217_111027/2_B01/m64144_230218_221218.subreads.bam', '40', '3333', '1235-64447e-E111'))
    #jsonfile = "/home/gongshuang/pb/core/data_receive/tmp/Pacbio_data.json"
    data = json.load(open(jsonfile, 'r'), encoding='utf-8')
    outpath = sys.argv[2]
    chunk_num = sys.argv[3]
    #data = pre_ccs(data, 20, '/pfs/Sequencing/Nanopore.temp/check/PB_CCS/auto')
    data = pre_ccs(data, chunk_num, outpath)
    #print(data)
    json.dump(data, open(jsonfile, 'w'), indent=2)
