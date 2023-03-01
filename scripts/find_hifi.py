import os,sys


if __name__ == "__main__":
    cons_xmls = []
    with open(sys.argv[1]) as fin:
        for i in fin:
            time = i.strip().split('-')[0]
            matchine = i.strip().split('-')[1]
            lane = i.strip().split('-')[2]
            try:
                lib = i.strip().split('-')[3]
            except:
                lib = 'NULL'
            try:
                out = os.popen('ls /pfs/Sequencing/Pacbio/Sequel_ii*/sequel_ii*_r{m}/r{m}_{t}_*/*{lane}/*.hifi_reads.bam'.format(m=matchine, t=time, lane=lane)).read()
            except:
                out = ''
            print("{}\t{}".format(i.strip(), out.strip()))
            if out:
                cons_xmls.append([i.strip(), out.replace('hifi_reads.bam', 'consensusreadset.xml')])
        print('----')
        for i in cons_xmls:
            print('{}\t{}'.format(i[0], i[1].strip()))
