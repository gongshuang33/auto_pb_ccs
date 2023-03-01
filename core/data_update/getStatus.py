#coding=utf-8

import json
import sys
import os



# 对每个路径进行检查（是否同步、是否完成ccs统计、是否call完ccs）
def check_done_file(movie, path):
    #path = '/pfs/Sequencing/Nanopore.temp/check/PB_CCS/auto/20230224-64181-A01'
    res = {"pb_ccs": {"status": "un", 'path': 'null'}, "collect_results": {'status': 'un', 'path': 'null'}, "update_results": {'status': 'un', 'path': 'null'}}
    try:
        lists = os.listdir(path)
        if len(lists) == 0:
            return res 
    except:
        return res
    # 1 如果同步完成，返回同步信息
    try:
        if 'POST_TO_LIMS_DONE' in os.listdir(os.path.join(path, 'Result')):
            res['update_results']['status'] = 'SUCCESSFULL'
            res['update_results']['path'] = os.path.join(path, 'Result')
    except:
        pass
    if 'ii_report_data.log' in lists:
        try:
            with open(os.path.join(path, 'ii_report_data.log')) as ret:
                for line in ret:
                    if '回填成功' in line:
                        info = json.loads(line.strip().replace('\'','"'))
                        data_path = info['data'][0]['DATA_PATH']
                        res['update_results']['status'] = 'SUCCESSFULL'
                        res['update_results']['path'] = data_path
        except:
            pass
    # 2 如果完成CCS统计，返回collect_done
    if 'Call_CCS_DONE' in lists:
        res['collect_results']['status'] = 'SUCCESSFULL'
        res['collect_results']['path'] = os.path.join(path, 'Result')
    # 3 如果call 完 ccs, 返回
    if 'Result' in lists:
        res["pb_ccs"]['status'] = 'SUCCESSFULL'
        res['pb_ccs']['path'] = os.path.join(path, movie)
    return res



if __name__ == '__main__':
    #movie = 'm64173_230128_042147'
    #path = '/home/gongshuang/pb/auto/20230128-64173-A01'
    #print(check_done_file(movie, path))
    json_file = sys.argv[1]

    db = json.load(open(json_file))
    for i in db.keys():
        path = db[i]['ccs']['path']
        movie = i
        db[i]['run_info'] = check_done_file(movie, path)
    json.dump(db, open('PB_tmp.json', 'w'), indent=2)
    os.system('cp PB_tmp.json  ' + json_file)
