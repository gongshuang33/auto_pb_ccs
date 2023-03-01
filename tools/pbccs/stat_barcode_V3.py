#!/usr/bin/env python
#coding:utf-8

from email.policy import default
import os
import sys
import gzip
import json
import glob
import shutil
import pysam
import logging
import argparse
import xml.dom.minidom

import collections
from collections import defaultdict

LOG = logging.getLogger(__name__)

__version__ = "1.1.0"
__author__ = ("Xingguo Zhang",)
__email__ = "113178210@qq.com"
__all__ = []


def make_movie_json_db(smlk_dir):

    db = defaultdict(dict)
    raw_data_jsons = glob.glob("%s/*/outputs/raw_data.report.json" % smlk_dir)
    assert len(raw_data_jsons) > 0

    for raw_data_json in raw_data_jsons:
        outputs_dir = os.path.dirname(raw_data_json)
        loading_json = os.path.join(outputs_dir, "loading.report.json")
        if (not os.path.exists(loading_json)) or (not os.path.exists(raw_data_json)):
            LOG.warning("Loading JSON file not found! skip %s" % outputs_dir)
            continue

        raw_data_dict = read_raw_data_json(raw_data_json)
        loading_dict = read_loading_json(loading_json)
        movie = loading_dict['movie']
        db[movie]['raw_data_dict'] = raw_data_dict
        db[movie]['loading_dict'] = loading_dict

        path = os.path.dirname(os.path.dirname(os.path.realpath(raw_data_json)))
        if glob.glob(os.path.join(path, "inputs/*/{}.consensusreadset.xml".format(movie))):
            LOG.warning("Found IIe movie! skip %s" % outputs_dir)
            continue
        subreadxml_path = os.path.join(path, "inputs/*/*.subreadset.xml")
        subreadxml = glob.glob(subreadxml_path)
        if len(subreadxml) != 1:
            LOG.error("subreadset.xml file not found! Exit")
            raise Exception
        
        subreadxml = subreadxml[0]
        db[movie]['library'] = parse_consensusreadset_xlm(subreadxml)

    return db


def read_raw_data_json(jsfile):
    d = json.loads(open(jsfile).read())
    poly_bases, poly_reads, poly_mean, poly_n50 = 0,0,0,0

    for item in d['attributes']:
        if item['id'] == "raw_data_report.nbases":
            poly_bases = item["value"]
        if item['id'] == "raw_data_report.nreads":
            poly_reads = item["value"]
        if item['id'] == "raw_data_report.read_length":
            poly_mean = item["value"]
        if item['id'] == "raw_data_report.read_n50":
            poly_n50 = item["value"]

    res = {
        'Polymerase_Read_Bases(bp)' : poly_bases,
        'Polymerase_Reads': poly_reads,
        'Polymerase_mean_len(bp)' : poly_mean,
        'Polymerase_N50_len(bp)' : poly_n50
    }

    return res


def read_loading_json(jsfile):
    d = json.loads(open(jsfile).read())

    for colu in d['tables'][0]['columns']:
        if colu['id'] == "loading_xml_report.loading_xml_table.collection_context":
            movie = colu['values'][0]
        if colu['id'] == "loading_xml_report.loading_xml_table.productive_zmws":
            total_zmws = colu['values'][0]
        if colu['id'] == "loading_xml_report.loading_xml_table.productivity_0_n":
            p0 = colu['values'][0]
        if colu['id'] == "loading_xml_report.loading_xml_table.productivity_1_n":
            p1 = colu['values'][0]
        if colu['id'] == "loading_xml_report.loading_xml_table.productivity_2_n":
            p2 = colu['values'][0]

    res = {
        'movie' : movie,
        'zmws' : total_zmws,
        'p0' : p0,
        'p1' : p1,
        'p2' : p2
    }

    return res


def read_hifi_json(jsfile):

    d = json.loads(open(jsfile).read())

    d_stat = {}
    for col in d['attributes']:
        d_stat[col['id']] = col['value']

    d_length = {}
    length_keys = d['tables'][0]['columns'][0]['values']
    length_reads = d['tables'][0]['columns'][1]['values']
    length_bases = d['tables'][0]['columns'][3]['values']
    for (k, v1, v2) in zip(length_keys, length_reads, length_bases):
        d_length[k] = [v1, v2]

    d_qual = {}
    qual_keys = d['tables'][1]['columns'][0]['values']
    qual_reads = d['tables'][1]['columns'][1]['values']
    qual_reads_pct = d['tables'][1]['columns'][2]['values']
    for (k, v1, v2) in zip(qual_keys, qual_reads, qual_reads_pct):
        d_qual[k] = [v1, v2]

    res = {
        "basic_stat": {
            "reads": d_stat['ccs2.number_of_ccs_reads'],
            "bases": d_stat['ccs2.total_number_of_ccs_bases'],
            "mean_len": d_stat['ccs2.mean_ccs_readlength'],
            "median_qual": d_stat['ccs2.median_accuracy'],
            "mean_pass": d_stat['ccs2.mean_npasses']
        },
        "length_stat": d_length,
        "quality_stat": d_qual
    }

    return res


def copy_pics(indir, outdir):
    pngfiles = glob.glob(os.path.join(indir, "*.png"))
    for png in pngfiles:
        if not png.endswith('thumb.png'):
            shutil.copy(png, outdir)


def parse_consensusreadset_xlm(xlmfile):
    dom = xml.dom.minidom.parse(xlmfile)
    root = dom.documentElement
    time_stamp = root.getElementsByTagName('pbmeta:Name')[0].childNodes[0].data
    well_name = root.getElementsByTagName('pbmeta:WellName')[0].childNodes[0].data
    library_name = root.getElementsByTagName('pbmeta:WellSample')[0].getAttribute('Name')

    return time_stamp, well_name, library_name


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
        np = line.get_tag('np')
        rq = line.get_tag('rq')
        yield [line.query_name, line.seq, np, rq]

    fh.close()


def n50(lengths):

    if len(lengths) == 0:
        return 0

    sum_length = sum(lengths)
    accu_length = 0

    for i in sorted(lengths, reverse=True):
        accu_length += i

        if accu_length >= sum_length*0.5:
            return i


def read_length(file):

    length = []
    n_pass = []
    quality = []

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
        n_pass.append(line[2])
        quality.append(line[3])

    return length, n_pass, quality


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
        n_q20, n_q30 = 0,0
        for qv in quality:
            if qv >= 0.99:
                n_q20 += 1
            if qv >= 0.999:
                n_q30 += 1

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
