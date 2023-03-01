import os
import re
import time
import sys
import subprocess

xml_list = sys.argv[1]

successfull_list = 'SUCCESSFULL'
failed_list = 'FAILED'

with open(xml_list) as xmls:
    for i in xmls:
        xml = i.strip()
        cmd = '/home/smrtanalysis/software/smrtlink_11.1.0.166339/smrtcmds/bin/pbservice import-dataset {xml}'.format(xml=xml)
        movie = re.findall('.*/(.+).subreadset.xml', xml)[0]
        curr_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        try:
            ret = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8", timeout=900)
        except:
            ret = False
            pass
        if ret == False:
            print('[{}]Import Dataset error: {}'.format(curr_time, movie))
            open(failed_list,'a').write('{time}\t{status}\t{movie}\t{xml_path}\n'.format(time=curr_time, status='FAILED', movie=movie, xml_path=xml))
        else:
            try:
                try:
                    status = re.findall(r'state: (.+)', ret.stdout)[0]
                    paths = re.findall(r'path: (.+)', ret.stdout)
                except Exception as e:
                    paths = ['','']
                    print(e)
                open(successfull_list, 'a').write('{time}\t{status}\t{movie}\n\txml_path: {xml_path}\n\tjson_path: {json_path}\n\n'.format(time=curr_time, status=status, movie=movie, xml_path=xml, json_path=paths[1]))
                print('[{}]Import Dataset finished: {}'.format(curr_time, movie))
            except:
                pass
        time.sleep(20)
