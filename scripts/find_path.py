#coding=utf-8

import os,sys
import re
import subprocess

def find_info(li):
    db = []
    li = open(li)
    for i in li:
        if i.startswith('m'):
            movie = i.strip()
            subreads_path = file_process('ls /pfs/Sequencing/Pacbio/Sequel_ii*/sequel_ii*/*/*/{movie}.subreads.bam'.format(movie = movie))
            hifi_path = file_process('ls /pfs/Sequencing/Pacbio/Sequel_ii*/sequel_ii*/*/*/{movie}.hifi_reads.bam'.format(movie = movie))
            subreads_share_path = file_process('ls /share/erapool/Sequencing/Sequel_ii*/sequel_ii*/*/*/{movie}.subreads.bam'.format(movie = movie))
            if subreads_path or hifi_path or subreads_share_path:
                m, t, h = re.findall(r'.*/Sequel_ii.*/sequel_ii.*_.*/r(.*)_(.*)_.*/.*_(.*)/.*.hifi_reads.bam.*', str(subreads_path) + str(hifi_path) + str(subreads_share_path))[0]
                runwell = '%s-%s-%s'%(m, t, h)
            else:
                runwell = False
            db.append({'runwell': runwell, 'movie': movie, 'subreads_path': subreads_path, 'hifi_path': hifi_path, 'subreads_share_path': subreads_share_path})
        else:
            runwell = i.strip()
            m = 'r' + runwell.split('-')[1]
            t = runwell.split('-')[0]
            h = runwell.split('-')[2]
            #print(runwell, m, t, h)
            subreads_path = file_process('ls /pfs/Sequencing/Pacbio/Sequel_ii*/sequel_ii*_{m}/{m}_{t}_*/*_{h}/*.subreads.bam'.format(m=m, t=t, h=h))
            subreads_share_path = file_process('ls /share/erapool/Sequencing/Sequel_ii*/sequel_ii*_{m}/{m}_{t}_*/*_{h}/*.subreads.bam'.format(m=m, t=t, h=h))
            #print(2, subreads_path)
            hifi_path = file_process('ls /pfs/Sequencing/Pacbio/Sequel_ii*/sequel_ii*_{m}/{m}_{t}_*/*_{h}/*.hifi_reads.bam'.format(m=m, t=t, h=h))
            #print(3, hifi_path)
            if subreads_path or hifi_path or subreads_share_path:
                try:
                    movie = re.findall(r'.*Sequel_ii.*/sequel_ii.*_.*/r.*_.*_.*/.*_.*/(.*)\..*.bam.*', str(subreads_path) + str(hifi_path) + str(subreads_share_path))[0]
                except:
                    print(re.findall(r'.*/Sequel_ii.*/sequel_ii.*_.*/r.*_.*_.*/.*_.*/(.*)\..*.bam.*', str(subreads_path) + str(hifi_path) + str(subreads_share_path)))
                    movie = 'null'
                    pass
            else:
                movie = 'null'
        db.append({'runwell': runwell, 'movie': movie, 'subreads_path': subreads_path, 'hifi_path': hifi_path, 'subreads_share_path': subreads_share_path})
    with open('stat.xls','w') as fout:
        fout.write('上机编号\tmovie号\tsubreads路径(pfs)\thifi路径\tsubreads路径(share)\n')
        for i in db:
            fout.write('{}\t{}\t{}\t{}\t{}\n'.format(str(i['runwell']).lstrip().rstrip(), i['movie'], str(i['subreads_path']).strip(), str(i['hifi_path']).lstrip().strip(), str(i['subreads_share_path']).lstrip().rstrip()))

        


    

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
        return [True]
    else:
        #logger.error(out)
        return False


if __name__ == "__main__":
    lists = sys.argv[1]
    find_info(lists)
    """
    input file lists:
        20230204-64181-B01-PBL230119
        20230204-64446e-A01-PBL230018
        20230204-64446e-B01-PBL230019
        20230204-64446e-C01-PBL230038
        ...
    """
