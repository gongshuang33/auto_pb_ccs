#coding=utf-8

import json
import time
import glob
import os

# 读入JSON文件
def readJSON(rjson):
    return 


# 返回cp状态
# 0: not cp  1: cp done  2: cp doing  -1: cp error
def get_cp_status(db, movie):
    if db['hifi_path'] != "":
        return 0
    elif os.path.exists(db['workdir'] + '/CP_DONE') and os.path.exists(db['workdir'] + '/' + movie + '.subreads.bam.pbi'):
        return 1
    else:
        try:
            file_list = glob.glob(db['workdir'] + '.*' + movie + '*')
            if file_list >= 1:
                f_size = os.stat(file_list[0]).st_size
                time.sleep(10)
                cur_size = os.stat(file_list[0]).st_size
                if cur_size > f_size and os.path.exists(db['workdir'] + '/CP_DOING'):
                    return 2
                else:
                    return -1
        except:
             return -1
            
# 返回CCS状态
# 0: not start  1: ccs done  2: ccs doing  -1: ccs error
def get_ccs_status(db, movie):
    if os.path.exists(db['workdir'] + '/CCS_DONE'):
        return 1
    

    
    
# 判断cell状态
def checkBam(db):
    # 对状态为非暂停和完成的cell进行目录日志扫描，更新该cell的json
    for movie in db.keys():       # 0:not start  1: done  2: pause -1: error
        if db[movie][status] in [0, 1, 2]:
            continue
        db[movie]['cp']['status'] = get_cp_status(db[movie], movie)
        db[movie]['ccs']['status'] = get_ccs_status(db, movie)
        db[movie]['lims']['status'] = get_lims_status(db, movie)
    return db
        
