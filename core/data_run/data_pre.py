#coding=utf-8
import os
import stat
import json

# 为每一个还没有开始质控的cell建立工作目录
# 返回[路径,code]
def pre_work(movie, wd, subreads, chunk_num, run_id, runwell):
    # try:
        workplace = wd + '/' + runwell
        ccs_path = workplace + '/' + movie
        # try:
        os.makedirs(workplace)
        os.makedirs(ccs_path)
        # except:
        #    pass
        for i in range(1, int(chunk_num) + 1):
            chunk_dir = ccs_path + '/ccs.' + str(i)
            try:
                os.mkdir(chunk_dir)
            except:
                pass
            with open(chunk_dir + '/c' + str(run_id) + '_' + str(i) + '.sh', 'w') as sh:
                sh.write('set -vex\n\ncd {chunk_dir}\n\n/home/smrtanalysis/software/smrtlink_11.1.0.166339/install/smrtlink-release_11.1.0.166339/smrtcmds/bin/ccs --hifi-kinetics --min-passes 3 --min-rq 0.99 --max-length 100000 --min-length 100 --chunk {i}/{chunk_num} -j 8  {subreads}  {movie}.{i}.ccs.bam \n\n/home/smrtanalysis/software/smrtlink_11.1.0.166339/install/smrtlink-release_11.1.0.166339/smrtcmds/bin/primrose --min-passes 3 -j 8  {movie}.{i}.ccs.bam {movie}.{i}.hifi.bam \n\ntouch {movie}.{i}.done\n\n'.format(chunk_dir=chunk_dir, i=i, subreads=subreads, movie=movie, chunk_num = chunk_num))
        merge_sh = ccs_path + '/' + 'merge.sh' 
        try:
            with open(merge_sh, 'w', encoding='utf-8') as msh:
                msh.write("dir='" + ccs_path + "'\nmovie='" + movie + "'\nchunknum=" + str(chunk_num) + "\n\ncd $dir\nif [ -f 'merge_start' ] || [ -f 'merge_done' ];then\n\texit 0 \nfi\n\ntouch merge_start\nwhile :\ndo\ncd $dir\nif [ -f 'merge_done' ];then\n\texit 0 \nfi\nnum=`ls */* |grep -c done$ `\n\necho -e `pwd` '节点：' `hostname` '时间：'`date` '完成数量:' $num\nif (( $num == $chunknum  ));then\n\n  set -vex\n\n  cd $dir\n  /home/smrtanalysis/software/smrtlink_11.1.0.166339/install/smrtlink-release_11.1.0.166339/smrtcmds/bin/pbmerge -o ${movie}.hifi_reads.bam ${dir}/ccs.*/${movie}.*.hifi.bam\n\n  rm merge_start && touch merge_done\n\n  echo '合并完成：' `date`\n\nbreak\nexit 0\n\nfi;\n\n\nsleep 300s\n\ndone\n")
        except Exception as e:
            print(1,e)
       # ccs_sh = chunk_dir + '/c' + str(run_id) + '_' + str(i) + '.sh'
    #os.system('cd {chunk_dir} && /opt/sge/bin/lx-amd64/qsub -S /usr/bin/bash -V -cwd -l q=all.q -pe smp 8 -p 20 {ccs_sh}'.format(chunk_dir=chunk_dir, ccs_sh=ccs_sh))
        return [ccs_path, merge_sh, 'ready']
    # except:
        # return ['','', 0]

def pre_ccs(db, chunk_num, workdir):
    print(type(db))
    for m in db.keys():
        if db[m]['code'] == 0 and db[m]['status'] == 'only_subreads':
            slist = pre_work(m, workdir, db[m]['subreads'], chunk_num, m[-6:], db[m]['runwell'])
            db[m]['ccs']['path'] = slist[0]
            db[m]['merge']['path'] = slist[1]
            db[m]['merge']['code'] = db[m]['ccs']['code'] = slist[2]
            db[m]['code'] = 'ready'
            db[m]['ccs']['chunk_num'] = chunk_num
        elif db[m]['ccs']['code'] == 0 and db[m]['status'] == 'both':
            pass
        elif db[m]['ccs']['code'] == 0 and db[m]['status'] == 'only_hifi':
            pass
        else:
            pass
    return db




if __name__ == "__main__":
    #print(pre_work('m64144_230218_221218', '/pfs/Sequencing/Nanopore.temp/check/PB_CCS/auto', '/pfs/Sequencing/Pacbio/Sequel_ii/sequel_ii_r64144/r64144_20230217_111027/2_B01/m64144_230218_221218.subreads.bam', '40', '3333', '1235-64447e-E111'))
    data = json.load(open('/home/gongshuang/pb/core/data_receive/tmp/Pacbio_data.json', 'r'), encoding='utf-8')
    data = pre_ccs(data, 20, '/pfs/Sequencing/Nanopore.temp/check/PB_CCS/auto')
    #print(data)
    json.dump(data, open('/home/gongshuang/pb/core/data_receive/tmp/Pacbio_data.json', 'w'), indent=2)
