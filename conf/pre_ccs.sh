import sys
import os

wd = sys.argv[1]
subreads = sys.argv[2]
chunknum = sys.argv[3]


movie = subreads.split('/')[-1].split('.')[0]

ccs_path = wd + '/' + movie
try:
   os.mkdir(ccs_path)
except:
   pass

for i in range(1, int(chunknum) + 1):
    chunk_dir = ccs_path + '/ccs.' + str(i)
    try:
        os.mkdir(chunk_dir)
    except:
        pass
    with open(chunk_dir + '/run_ccs.' + str(i) + '.sh', 'w') as sh:
        sh.write('set -vex\ncd {chunk_dir}\n/home/smrtanalysis/software/smrtlink_11.1.0.166339/install/smrtlink-release_11.1.0.166339/smrtcmds/bin/ccs --hifi-kinetics --min-passes 3 --min-rq 0.99 --max-length 100000 --min-length 100 --chunk {i}/{chunknum} -j 8  {subreads}  {movie}.{i}.ccs.bam \n/home/smrtanalysis/software/smrtlink_11.1.0.166339/install/smrtlink-release_11.1.0.166339/smrtcmds/bin/primrose --min-passes 3 -j 8  {movie}.{i}.ccs.bam {movie}.{i}.hifi.bam \ntouch {movie}.{i}.done\n'.format(chunk_dir=chunk_dir, i=i, subreads=subreads, movie=movie, chunknum=chunknum))
    ccs_sh = chunk_dir + '/run_ccs.' + str(i) + '.sh'
    if i < 7:
        os.system('cd {chunk_dir} && /opt/sge/bin/lx-amd64/qsub -S /usr/bin/bash -V -cwd -q ccs.q -pe smp 8 {ccs_sh}'.format(chunk_dir=chunk_dir, ccs_sh=ccs_sh))
    else:
        os.system('cd {chunk_dir} && /opt/sge/bin/lx-amd64/qsub -S /usr/bin/bash -V -cwd -q all.q -pe smp 8 {ccs_sh}'.format(chunk_dir=chunk_dir, ccs_sh=ccs_sh))
