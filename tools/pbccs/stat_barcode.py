#!/usr/bin/env python
#coding:utf-8

import os
import re
import sys
import gzip
import json
import pysam
import logging
import argparse

import collections

LOG = logging.getLogger(__name__)

__version__ = "1.1.0"
__author__ = ("Xingguo Zhang",)
__email__ = "113178210@qq.com"
__all__ = []


def read_fasta(file):
    '''Read fasta file'''

    if file.endswith(".gz"):
        fp = gzip.open(file)
    elif file.endswith(".fasta") or file.endswith(".fa"):
        fp = open(file)
    else:
        raise Exception("%r file format error" % file)

    seq = ''

    for line in fp:
        line = line.strip()

        if not line:
            continue
        if not seq:
            seq += "%s\n" % line.strip(">").split()[0]
            continue
        if line.startswith(">"):
            line = line.strip(">").split()[0]
            seq = seq.split('\n')

            yield [seq[0], seq[1]]
            seq = ''
            seq += "%s\n" % line
        else:
            seq += line

    seq = seq.split('\n')
    if len(seq)==2:
        yield [seq[0], seq[1]]
    fp.close()


def read_fastq(file):
    '''Read fastq file'''

    if file.endswith(".gz"):
        fp = gzip.open(file)
    elif file.endswith(".fastq") or file.endswith(".fq"):
        fp = open(file)
    else:
        raise Exception("%r file format error" % file)

    seq = []

    for line in fp:
        line = line.strip()

        if not line:
            continue
        if line.startswith('@') and (len(seq)==0 or len(seq)>=5):
            seq.append(line.strip("@").split()[0])
            continue
        if line.startswith('@') and len(seq)==4:
            yield seq
            seq = []
            seq.append(line.strip("@").split()[0])
        else:
            seq.append(line)

    if len(seq)==4:
        yield seq
    fp.close()


def read_bam(file):

    if file.endswith(".bam"):
        fh = pysam.AlignmentFile(file, "rb", check_sq=False)
    elif file.endswith(".sam"):
        fh = pysam.AlignmentFile(file, 'r')
    else:
        raise Exception("%r file format error" % file)

    for line in fh:
        yield [line.qname, line.seq]

    fh.close()


def n50(lengths):

    sum_length = sum(lengths)
    accu_length = 0

    for i in sorted(lengths, reverse=True):
        accu_length += i

        if accu_length >= sum_length*0.5:
            return i


def read_length(file):

    length = []

    if file.endswith(".fasta") or file.endswith(".fa") or file.endswith(".fa.gz") or file.endswith(".fasta.gz"):
        fh = read_fasta(file)
    elif file.endswith(".fastq") or file.endswith(".fq") or file.endswith(".fq.gz") or file.endswith(".fastq.gz"):
        fh = read_fastq(file)
    elif file.endswith(".bam") or file.endswith(".sam"):
        fh = read_bam(file)
    else:
        raise Exception("%r file format error" % file)

    for line in fh:
        length.append(len(line[1]))

    return length


def stat_reads(files, out, obspath):

    title = ["Sample", "Bases(bp)", "Reads number", "Mean Length(bp)", "N50(bp)", "Longest(bp)"]
    data = collections.OrderedDict()
    #sample = collections.OrderedDict()
    fo = open(out, 'w')
    outdir = os.path.dirname(os.path.abspath(out))
    fj = open("%s/reads_stat.json" % outdir, 'w')
    fo.write('Sample\tBases(bp)\tReads number\tMean Length(bp)\tN50(bp)\tLongest(bp)\n')
    for file in files:
        name = file.split('/')[-1]

        if '--' in name:
            name = name.split('--')[1].split('.bam')[0]
        else:
            name = name.split('.')[0]

        data[name] = collections.OrderedDict()
        length = read_length(file)

        line = [
                name,
                "{:,}".format(sum(length)),
                "{:,}".format(len(length)),
                "{:,.2f}".format(sum(length)*1.0/len(length)),
                "{:,}".format(n50(length)),
                "{:,}".format(max(length)),
                "{}/{}".format(obspath, name)
               ]
        fo.write('\t'.join(line) + '\n')

        for i in range(len(title)):
            data[name][title[i]] = line[i]
        with open("{}/data.json".format(os.path.dirname(os.path.abspath(file))), 'w') as out:
            out.write(json.dumps(data[name], indent=4))

    fj.write(json.dumps(data, indent=4))
    fj.close()
    fo.close()
    #print("field = %s" % json.dumps(title))
    #print("summary = %s" % json.dumps(data))
    #print("sample_lib_lane = %s" % json.dumps(sample))


def add_hlep_args(parser):

    parser.add_argument('-i', '--input', nargs='+', metavar='FILE', type=str, required=True,
        help='Input reads file, format(fasta,fastq,fa.gz,bam and sam).')
    parser.add_argument('-p', '--obspath', metavar='PATH', type=str, default='',
        help="Upload obs path")
    parser.add_argument('-o', '--out', metavar='STR', type=str, required=True,
        help='Out xls file')

    return parser


def main():

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="[%(levelname)s] %(message)s"
    )
    parser = argparse.ArgumentParser()

    args = add_hlep_args(parser).parse_args()

    stat_reads(args.input, args.out, args.obspath)


if __name__ == "__main__":

    main()
