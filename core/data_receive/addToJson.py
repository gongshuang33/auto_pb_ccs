#coding=utf-8
import re
import os
import time
import json
import argparse
import xml.dom.minidom

DATA_TEMP = {
        "code": 0,
        "status": "",
        "subreads": "",
        "hifi_reads": "",
        "subreads_mtime": "",
        "hifi_mtime": "",
        "ccs": {
            "code": 0,
            "path": '',
            "chunk_num": 0
        }
    }

# 解析路径，加入json文件
def mod_json_file(bam, data, code):
    d = {}
    # /pfs/Sequencing/Pacbio/Sequel_iie/sequel_iie_r64504e/r64504e_20221114_083434/3_C03/m64504e_221117_030514.hifi_reads.bam
    # /pfs/Sequencing/Pacbio/Sequel_iie/sequel_iie_r64446e/r64446e_20230204_024832/4_D01/m64446e_230208_050358.subreads.bam
    ma, da, hole, movie , typei = re.findall( "/pfs/Sequencing/Pacbio/Sequel_ii.*/sequel_.*_.*/r(.*)_(.*)_.*/.*_(.*)/(.*)\.(.*).bam", bam)[0]
    # 从subreadset.xml文件中获取文库编号
    try:
        if typei == 'subreads':
            xml_path = bam.replace('subreads.bam', 'subreadset.xml')
            library = xml.dom.minidom.parse(xml_path).getElementsByTagName('WellSample')[0].getAttribute("Name").split('-')[-1]
        if typei == 'hifi_reads':
            xml_path = bam.replace('hifi_reads.bam', 'consensusreadset.xml')
            library = xml.dom.minidom.parse(xml_path).getElementsByTagName('pbmeta:WellSample')[0].getAttribute("Name").split('-')[-1]
    except:
        library = 'null'
    
    try:
        if d[movie]:
            pass
    except:
        d[movie] = DATA_TEMP.copy()
        d[movie]["code"] = code
        #raise Exception('Something wrong happend')
    try:
        d[movie]["hifi_ctime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.stat(bam).st_mtime))
    except:
        #print(os.stat(bam).st_mtime)
        pass
    if typei == 'hifi_reads':
        d[movie]["hifi_reads"] = bam
    if typei == 'subreads':
        d[movie]["subreads"] = bam
    d[movie]['runwell'] = "{}-{}-{}-{}".format(da, ma, hole, library)
    if d[movie]["hifi_reads"]  and d[movie]["subreads"]:
        d[movie]["status"] = "both"  
    if d[movie]["hifi_reads"]  and  d[movie]["subreads"] == '':
        d[movie]["status"] = "only_hifi" 
    if d[movie]["hifi_reads"] == '' and d[movie]["subreads"]:
        d[movie]["status"] = "only_subreads"
    if movie in data.keys():
        print("[WARNING] The bam is in jsonfile. Bam: " + movie)
    else:
        data[movie] = d[movie]
    return data


def add_json(inputs, output, code):
    dirname = os.path.dirname(output)
    if not os.path.exists(dirname):
        raise Exception("Path '%s' not exists!" % dirname)
    if not os.path.exists(output):
        open(output, 'w').write("{\n  \"test\": []\n}")
    data = json.load(open(output, 'r', encoding = 'utf-8'))
    #print(data)
    for f in inputs:
        if not os.path.exists(f):
            out.close()
            raise Exception("File '%s' not exists!" % dirname)
        with open(f) as fin:
            for i in fin:
                data = mod_json_file(i.strip(), data, code)
    json.dump(data, open(output, 'w'),indent=2)
 



if __name__ == "__main__":
    parser = argparse.ArgumentParser()                   
    help = "Add subreads' info to a json file."
    parser.add_argument('-i', '--inputs',required=True, nargs='+', help = 'input files, one or more.') 
    parser.add_argument('-o', '--output',required=True, type=str,  help = 'a output json file.')
    parser.add_argument('-c', '--code',required=True, type=int,  help = 'init code: 1  success  , 0 prepare.')
    args = parser.parse_args()
    #print(args)
    add_json(args.inputs, args.output, args.code)
