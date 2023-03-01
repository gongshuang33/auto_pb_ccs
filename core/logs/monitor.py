#coding=utf-8
import os
import sys
import logging
import json
import requests
import datetime

LOG = logging.getLogger()


# 返回文件行数
def getLines(file):
    count = 0
    f = open(file, "r")
    for line in f.readlines():
        count = count + 1
    return count

# 查看目录下的对应文件，判断运行状态
def monitor(db, out_log):
    content = ''
    for m in db.keys():
        info = []
        if db[m]['code'] != 'ccs_running':
            pass
        workdir = db[m]['ccs']['path']
        # 查看【统计目录】是否异常
        if os.path.exists(os.path.join(workdir, 'Result', 'Stat_error')):
            pass
        # 查看【同步lims】是否异常
        if os.path.exists(os.path.join(workdir, 'ii_report_data.log')):
            flag = False
            # 同步log的行数
            ii_lines = getLines(os.path.join(workdir, 'ii_report_data.log'))
            #于小4行认为正在同步
            if ii_lines < 4:
                info.append({"ii_report": "running"})
            else:
                with open(os.path.join(workdir, 'ii_report_data.log')) as ii_log:
                    for i in ii_log:
                        if '回填成功' in i:
                            flag = True
                            break
                # 同步lims错误
                if not flag:
                    info.append({"ii_report": "上传发生异常|路径:" + db[m]['ccs']['path']})
        # 查看【merge ccs】是否正常  #Merge_success Merge_failed
        if os.path.exists(os.path.join(workdir, m, 'Merge_failed')):
            info.append({"merge_ccs": "合并发生异常|路径:" + db[m]['ccs']['path'] + '/'+ m})
        # 检查 info 信息，并输出
        if len(info) > 0:
            LOG.info("[PB] | {} | {}".format(m, db[m]['runwell']) )
            for i in info:
                name = list(i)[0]
                content = content + "{}\n".format(i[name])
                LOG.info(i[name])
    cur_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not content:
        return 0 
    content = cur_time + '\n' + content
    feishu_robot(content)

#飞书机器人提示
def feishu_robot(message):
        """
        飞书机器人报警
        :param message: 报警信息
        :return:
        """
        url = 'https://open.feishu.cn/open-apis/bot/v2/hook/f7e60037-7f38-4c6f-8892-3de0da26d7f6'
        headers = {
            'Content-Type': 'application/json'
        }
        payload = json.dumps({
            "msg_type": "text",
            "content": {
                "text": message
            }
        })
        response = requests.request("POST", url, headers=headers, data=payload)
 

if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="[%(levelname)s] %(asctime)s %(message)s")    
    out_log = 'log'
    pb_json = '/home/gongshuang/pb/logs/Pacbio_data.json'
    db = json.load(open(pb_json))
    monitor(db, out_log)
   
