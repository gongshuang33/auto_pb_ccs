#!/usr/bin/env python

import os
import argparse
import glob


def safemove(source_file, target_path):
    cmd = "mv {} {}".format(source_file, target_path)
    if (not os.path.isfile(source_file)) or (not os.path.exists(target_path)):
        raise Exception("No such file or directory! %s" % cmd)
    if os.system(cmd):
        raise Exception("Error executing command: %s" % cmd)


def mkdir(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def makemd5(path):
    for root, dirs, files in os.walk(os.path.abspath(path)):
        if len(files) == 0:
            continue
        os.chdir(root)
        for file in files:
            cmd = 'md5sum %s >> md5.txt' % file
            if os.system(cmd):
                raise Exception("ERROR md5sum for file: %s" % file)
    return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--barcodes', type=str, nargs='+', help="barcode names, blank space seperated, eg: dT_BC1 dT_BC2 dT_BC3")
    parser.add_argument('-I', '--input_cell_path', help="workdir")
    args = parser.parse_args()

    rootdir = os.path.abspath(args.input_cell_path)
    res_dir = os.path.join(rootdir, 'Result')

    ccs_path = mkdir(os.path.join(res_dir, 'CCS'))
    for barcode in args.barcodes:
        target_path = mkdir(os.path.join(ccs_path, barcode))
        source_bam = os.path.join(rootdir, barcode, "{}.hifi_reads.bam".format(barcode))
        source_index = source_bam + '.pbi'
        source_json = os.path.join(rootdir, barcode, "data.json")
        safemove(source_bam, target_path)
        safemove(source_index, target_path)
        safemove(source_json, target_path)
    
    

    #ccs_stat = os.path.join(rootdir, '02.lima', 'ccs_stat.xls')

    #safemove(ccs_stat, res_dir)
    #safemove(subreads_stat, res_dir)

    makemd5(res_dir)


if __name__ == "__main__":
    main()
    
