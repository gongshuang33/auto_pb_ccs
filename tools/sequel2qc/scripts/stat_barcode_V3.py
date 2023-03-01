#!/usr/bin/env python
#coding:utf-8


import os
import sys
import json
import logging
import argparse
import collections

from read_file import read_length, n50


LOG = logging.getLogger(__name__)


def stat_reads(files, out, json_file, obspath):

    title1 = ["Sample", "Bases(bp)", "Reads number", "Mean Length(bp)", "N50(bp)", "Longest(bp)"]
    title2 = [
        'run_well', 'library', 'movie',
        'Polymerase_Read_Bases(bp)', 'Polymerase_Reads',
        'Polymerase_mean_len(bp)', 'Polymerase_N50_len(bp)',
        'ZMWs', 'P0', 'P1', 'P2', 
        'ccs_reads', 'ccs_bases(bp)', 'ccs_mean_len(bp)',
        'ccs_N50_len(bp)', 'ccs_Max_len(bp)',
        'mean_pass',
        'Q20', 'Q30',
        'path'
    ]

    outdir = os.path.dirname(os.path.abspath(out))
    fo = open(out, 'w')
    fo.write('\t'.join(title1) + '\n')
    fo_detail = open("%s/HIFI_stat.xls" % outdir, 'w')
    fo_detail.write('\t'.join(title2) + '\n')

    data = collections.OrderedDict()

    db = json.loads(open(json_file).read())

    for file in files:
        name = file.split('/')[-1]
        if '--' in name:
            name = name.split('--')[1].split('.bam')[0]
        else:
            name = name.split('.')[0]

        length, n_pass, quality = read_length(file)

        avg_length = 0 if len(length) == 0 else sum(length)*1.0/len(length)
        n50_len = n50(length)
        max_length = 0 if len(length) == 0 else max(length)
        avg_pass = 0 if len(n_pass) == 0 else int(sum(n_pass)*1.0/len(n_pass))
        n_q20, n_q30, n_q40 = 0,0,0
        for qv in quality:
            if qv >= 0.99:
                n_q20 += 1
            if qv >= 0.999:
                n_q30 += 1
            if qv >= 0.9999:
                n_q40 += 1

        line = [
                name,
                "{:,}".format(sum(length)),
                "{:,}".format(len(length)),
                "{:,.2f}".format(avg_length),
                "{:,}".format(n50_len),
                "{:,}".format(max_length),
                "{}/{}".format(obspath, name)
        ]

        contents = [
            db[name]['run_well'],
            db[name]['library'],
            name,
            db[name]['Polymerase_Read_Bases(bp)'],
            db[name]['Polymerase_Reads'],
            db[name]['Polymerase_mean_len(bp)'],
            db[name]['Polymerase_N50_len(bp)'],
            db[name]['ZMWs'],
            db[name]['P0'],
            db[name]['P1'],
            db[name]['P2'],
            "{:,}".format(len(length)),
            "{:,}".format(sum(length)),
            "{:,.2f}".format(avg_length),
            "{:,}".format(n50_len),
            "{:,}".format(max_length),
            str(avg_pass),
            "{}({:.2%})".format(n_q20, n_q20*1.0/len(length)),
            "{}({:.2%})".format(n_q30, n_q30*1.0/len(length)),
            "{}/{}".format(obspath, name)
        ]

        fo.write('\t'.join(line) + '\n')
        fo_detail.write('\t'.join(contents) + '\n')

        data[name] = collections.OrderedDict()
        for i in range(len(title1)):
            data[name][title1[i]] = line[i]
        with open("{}/data.json".format(os.path.dirname(os.path.abspath(file))), 'w') as out:
            out.write(json.dumps(data[name], indent=4))

    fj = open("%s/reads_stat.json" % outdir, 'w')
    fj.write(json.dumps(data, indent=4))
    fj.close()
    fo.close()
    fo_detail.close()


def add_hlep_args(parser):

    parser.add_argument('-i', '--input', nargs='+', metavar='FILE', type=str, required=True,
        help='Input reads file, format(fasta,fastq,fa.gz,bam and sam).')
    parser.add_argument('-j', '--input_json', metavar='JSON', type=str, required=True,
        help="input json file containing data for each cell")
    parser.add_argument('-p', '--obspath', metavar='PATH', type=str, default='',
        help="Upload obs path")
    parser.add_argument('-o', '--out', metavar='STR', type=str, required=True,
        help='Out xls file')
    #parser.add_argument('--smrtlink', metavar='STR', type=str, 
    #    default='/share/erapool/personal/smrtanalysis/software/smrtlink_10.2.0.133434/userdata/jobs_root/0000/0000000',
    #    help="smrtlink analysis dir[/share/erapool/personal/smrtanalysis/software/smrtlink_10.2.0.133434/userdata/jobs_root/0000/0000000]")

    return parser


def main():

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="[%(levelname)s] %(message)s"
    )
    parser = argparse.ArgumentParser()

    args = add_hlep_args(parser).parse_args()

    stat_reads(args.input, args.out, args.input_json, args.obspath)


if __name__ == "__main__":

    main()
