# coding=UTF-8
import sys
import os
import pandas as pd
import requests
import json
import time
import datetime
from requests_toolbelt import MultipartEncoder

SCRIPT_PATH = os.path.dirname(os.path.split(os.path.realpath(__file__))[0])

#def req(root: object) -> object:
def req(root, subreadspath):    
     ##判断该目录是否同步
    if 'POST_TO_LIMS_DONE' in os.listdir(root):
        print("[WARNING] File \'POST_TO_LIMS_DONE\' exists in the path \'{}\', this Result will be ignored.".format(root))
        return 0

    data_frame = pd.read_table(root + '/HIFI_stat.xls')
    for n in range(data_frame.shape[0]):
        run_well_split = data_frame.iat[n, 0].split('-')
        TGS_BOARDING_DATETIME = run_well_split[0][0:4] + '-' + run_well_split[0][4:6] + '-' + run_well_split[0][6:8]
        run_well_split1 = run_well_split[1].split('_')
        TGS_MACH_CODE = run_well_split1[0]
        TGS_LANE_NO = run_well_split1[1]
        #subreads_path = '/share/erapool/Sequencing/Sequel_ii*/*/*/*/%s.subreads.bam' % data_frame.iat[n, 2]
        subreads_path = subreadspath
        bam = data_frame.iat[n, 19] + '/' + os.path.basename(data_frame.iat[n, 19]) + '.hifi_reads.bam'
        #outdir = '/share/erapool/personal/smrtanalysis/0.call.ccs/HIFI/report/' + data_frame.iat[n, 2]
        outdir = '/pfs/Sequencing/Pacbio/PB_CCS/HIFI_reports/' + data_frame.iat[n, 2]
        print(bam, outdir)
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        try:
            if not os.path.exists(outdir + '/' + data_frame.iat[n, 2]  +'.hifi_report.docx') or not os.path.exists(outdir + '/' + 'report_done'):
                os.system(f'/home/gongshuang/Python3/bin/python3  {SCRIPT_PATH}/sequel2qc/scripts/hifi_report.py --input_bam ' + bam + ' --outdir ' + outdir)
                os.system(f'touch {outdir}/report_done')
            else:
                print('[WARNING] the report may be finished : ' + outdir + '/' + data_frame.iat[n, 2]  +'.hifi_report.docx')
        except:
            print('Can not write to path: ' + outdir)
            pass

        url1 = "http://114.115.211.203/grandomics-java/system/config/upload/fileUpload"

        m = MultipartEncoder(
            fields={'uploadPath': 'default-path',
                    'files': (
                    'ccs_readlength_hist_plot.png', open(outdir + '/ccs_readlength_hist_plot.png', 'rb'),
                    'image/png')
                    }
        )

        headers = {
            'Content-Type': m.content_type,
            'Cookie': 'JSESSIONID=ceqedZJ7xHs8fLd3ceniOmccOAenShMdqjRy6k4X'
        }

        response = requests.request("POST", url1, headers=headers, data=m)
        print(response.text)

        ccs_readlength_hist_plot = json.loads(response.text)['fileNames']['ccs_readlength_hist_plot.png']

        m = MultipartEncoder(
            fields={'uploadPath': 'default-path',
                    'files': ('readlength_qv_hist2d.hexbin.png',
                              open(outdir + '/readlength_qv_hist2d.hexbin.png', 'rb'),
                              'image/png')
                    }
        )

        headers = {
            'Content-Type': m.content_type,
            'Cookie': 'JSESSIONID=ceqedZJ7xHs8fLd3ceniOmccOAenShMdqjRy6k4X'
        }

        response = requests.request("POST", url1, headers=headers, data=m)
        print(response.text)

        readlength_qv_hist2d = json.loads(response.text)['fileNames']['readlength_qv_hist2d.hexbin.png']

        # 按单元格解析
        Sequel_iie = {
            "TGS_BOARDING_DATETIME": TGS_BOARDING_DATETIME,
            "TGS_MACH_CODE": TGS_MACH_CODE,
            "TGS_LANE_NO": TGS_LANE_NO,
            list(data_frame)[0]: data_frame.iat[n, 0],
            list(data_frame)[1]: data_frame.iat[n, 1],
            list(data_frame)[2]: data_frame.iat[n, 2],
            list(data_frame)[3]: data_frame.iat[n, 3].replace(',', ''),
            list(data_frame)[4]: data_frame.iat[n, 4],
            list(data_frame)[5]: data_frame.iat[n, 5],
            list(data_frame)[6]: data_frame.iat[n, 6],
            list(data_frame)[7]: str(data_frame.iat[n, 7]),
            list(data_frame)[8]: data_frame.iat[n, 8],
            list(data_frame)[9]: data_frame.iat[n, 9],
            list(data_frame)[10]: data_frame.iat[n, 10],
            list(data_frame)[11]: data_frame.iat[n, 11],
            list(data_frame)[12]: data_frame.iat[n, 12],
            'ccs_' + list(data_frame)[13]: str(data_frame.iat[n, 13]),
            'ccs_' + list(data_frame)[14]: str(data_frame.iat[n, 14]),
            'ccs_' + list(data_frame)[15]: str(data_frame.iat[n, 15]),
            'ccs_' + list(data_frame)[16]: str(data_frame.iat[n, 16]),
            list(data_frame)[17]: data_frame.iat[n, 17],
            list(data_frame)[18]: data_frame.iat[n, 18],
            list(data_frame)[19]: data_frame.iat[n, 19],
            'ccs_readlength_hist_plot': ccs_readlength_hist_plot,
            'readlength_qv_hist2d': readlength_qv_hist2d,
            'subreads_path': subreads_path
            # 'NEWPATH': path
        }
        print(Sequel_iie)
        url = "http://114.115.211.203/grandomics-java/sx/api/write"

        payload = json.dumps({
            "API_CODE": "PB_WRITE",
            "DATA_LIST": [Sequel_iie]
        })

        headers = {
            'Authorization': '22222',
            'Content-Type': 'application/json',
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        print(json.loads(response.text))
        print(time.strftime("%Y-%m-%d-%H", time.localtime(time.time())))
        os.mknod(os.path.abspath(root) + '/POST_TO_LIMS_DONE')


if __name__=="__main__":
    req(sys.argv[1], sys.argv[2])
    #req('/share/erapool/personal/smrtanalysis/0.call.ccs/HIFI/2022-11-24-18/Result', 'subreads_path')
