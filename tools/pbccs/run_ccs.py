#!/share/erapool/personal/smrtanalysis/software/miniconda/bin/python3

import os
import sys
from dagflow import Task, ParallelTask, set_tasks_order, DAG, do_dag
from utils import check_file, check_path, RenderShell
from config import CONF


def get_subreads(subreads_info, work_dir):
    sample_name = []

    with open(subreads_info, 'r') as f:

        os.system("mkdir -p %s/Subreads" % work_dir)
        for line in f.readlines():
            if line.startswith('#') or not line.strip():
                continue

            sample = line.strip().split()[0]
            bam = line.strip().split()[1]
            if os.path.getsize(bam) < 50 * 1024 * 1024:
                continue

            sample_name.append(sample)

            if not os.path.exists('%s/Subreads/%s.subreads.bam' % (work_dir, sample)):
                os.system("ln -s %s %s/Subreads/%s.subreads.bam" % (bam, work_dir, sample))
                os.system("ln -s %s.pbi %s/Subreads/%s.subreads.bam.pbi" % (bam, work_dir, sample))

    return sample_name


def get_chunks(chunk_number):
    chunk = []
    for i in  range(1, int(chunk_number)+1):
        chunk.append(i)
    return chunk


def run_ccs_tasks(work_dir, sample, chunk, passes, rq, max_len, min_len):
    chunk_len = len(chunk)
    sampledir = os.path.join(work_dir, sample)

    tasks = ParallelTask(
        id = '%s_ccs' % sample,
        work_dir = os.path.join(sampledir, "ccs.{chunk}"),
        type = "sge",
        option = '-pe smp 8 -q %s' % CONF['queue'],
        script = RenderShell('''
${{CCS_BIN}}/ccs --hifi-kinetics --min-passes {min_passes} --min-rq {min_rq} --max-length {max_length} --min-length {min_length} {work_dir}/Subreads/{samples}.subreads.bam {samples}.ccs.bam --chunk {{chunk}}/{chunk_len} -j 8

${{PrimRose_BIN}}/primrose --min-passes {min_passes} -j 8 {samples}.ccs.bam {samples}.hifi.bam
        '''.format(work_dir = work_dir, samples = sample, chunk_len = chunk_len, min_passes = passes, min_rq = rq, max_length = max_len, min_length = min_len), CONF),
        chunk = chunk)

    return tasks, sampledir


def run_ccs_merge_task(ccs_path, sample):
    task = Task(
        id = '%s_ccs_merge' % sample,
        work_dir = ccs_path,
        type = 'sge',
        option = '-pe smp 2',
        script = RenderShell('''

if [ -f 'Merge_success' ];then
    exit 0
fi

if [ -f 'Merge_failed' ];then
    rm Merge_failed
fi
{{\n
    ${{PYTHON_BIN}}/python3 ${{SCRIPT_BIN}}/combine_ccs_report.py {ccs_path} -p {sample} && \
    ${{CCS_BIN}}/pbmerge -o {sample}.hifi_reads.bam {ccs_path}/*/{sample}.hifi.bam && rm -rf {ccs_path}/*/{sample}.*.bam && \
    ${{CCS_BIN}}/pbindex {sample}.hifi_reads.bam && \
    touch Merge_success
\n}} || {{\n
    touch Merge_failed
\n}}

'''.format(ccs_path = ccs_path, sample = sample), CONF))

    return task


def run_ccs(work_dir, sample_name, chunk_number, passes, rq, max_len, min_len, dag):

    #sample_name = get_subreads(subreads_info = subreads_info, work_dir = work_dir)
    chunk = get_chunks(chunk_number = chunk_number)
    
    for sample in sample_name:

        ccs_tasks, ccs_path = run_ccs_tasks(work_dir = work_dir, sample = sample, chunk = chunk, passes = passes, rq = rq, max_len = max_len, min_len = min_len)
        dag.add_task(*ccs_tasks)

        ccs_merge_task = run_ccs_merge_task(ccs_path = ccs_path, sample = sample)
        dag.add_task(ccs_merge_task)

        ccs_merge_task.set_upstream(*ccs_tasks)

    return ccs_merge_task


def collect_results_task(workdir, smps, nobackup=False):

    sample_string = ' '.join(smps)

    resdir = "%s/Result" % workdir

    cmd = """
mkdir -p {resdir}
if [ -f Stat_error ];then
    rm Stat_error
fi
if [ -f Stat_success ];then
    exit 0
fi

{{\n
    ${{PYTHON_BIN}}/python3 ${{SCRIPT_BIN}}/stat_barcode_V2.py -i {workdir}/m*/*.hifi_reads.bam -p ${{DATA_DISK}} -o {resdir}/ccs_stat.xls && \
    touch Stat_success
\n}} ||  {{\n
    /home/smrtanalysis/software/smrtlink_11.1.0.166339/smrtcmds/bin/pbservice import-dataset /pfs/Sequencing/Pacbio/Sequel_*/sequel_*/*/*/{sample_string}.subreadset.xml && \
    ${{PYTHON_BIN}}/python3 ${{SCRIPT_BIN}}/stat_barcode_V2.py -i {workdir}/m*/*.hifi_reads.bam -p ${{DATA_DISK}} -o {resdir}/ccs_stat.xls && \
    touch Stat_success
\n}} ||  {{
    touch Stat_error && \
    exit 0
}}\n


${{PYTHON_BIN}}/python3 ${{SCRIPT_BIN}}/ccs_arrange.py -r {sample_string} -I {workdir}
""".format(workdir=workdir, resdir=resdir, sample_string=sample_string)

    if not nobackup:
        cmd += """
rsync -r --progress {resdir}/CCS/ ${{DATA_DISK}}/

rm {resdir}/CCS/*/*.hifi_reads.bam
rm {resdir}/CCS/*/*.hifi_reads.bam.pbi
""".format(resdir=resdir)

    task = Task(
        id = 'collect_result',
        work_dir = resdir,
        type = 'sge',
        option = '-pe smp 1 -q %s' % CONF['queue'],
        script = RenderShell(cmd, CONF)
    )

    return task


def main():
    import argparse
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format='[%(levelname)s] %(asctime)s %(message)s')

    parser = argparse.ArgumentParser(description="run ccs analysis")
    parser.add_argument('--workdir', help="project path")
    parser.add_argument('--subreads_info', help="PB file, first column is movie, second column is the path of subreads.bam")
    parser.add_argument('--min_passes', help="minimum pass number, default=3.", default = 3)
    parser.add_argument('--min_rq', help="minimum rq, default=0.99.", default = 0.99)
    parser.add_argument('--max_length', help="Maximum draft length before polishing, default=100000.", default = 100000 )
    parser.add_argument('--min_length', help="Minimum draft length before polishing, default=100.",default = 100)
    parser.add_argument('-c', '--chunk_number', help="set chunk number, default=20.", type = int, default = 20)
    parser.add_argument('--no_back_up', action='store_true', help="Do Not backup hifi_reads.bam to /share")
    args = parser.parse_args()

    sample_name = get_subreads(subreads_info = args.subreads_info, work_dir = args.workdir)

    if len(sample_name) == 0:
        os.system("mkdir -p {}/Result".format(args.workdir))
        os.system("touch {0}/Result/collect_result_done {0}/Result/HIFI_stat.xls".format(args.workdir))
        sys.exit(0)


    dag = DAG('CCS_workflow')
    run_ccs(args.workdir, sample_name, args.chunk_number, args.min_passes, args.min_rq, args.max_length, args.min_length, dag=dag)

    result_dag = DAG('collect_results')
    result_task = collect_results_task(args.workdir, sample_name, nobackup=args.no_back_up)
    result_dag.add_task(result_task)

    dag.add_dag(result_dag)
    do_dag(dag, concurrent_tasks=160, refresh_time=120, stop_on_failure=False)


if __name__ == '__main__':
    main()
