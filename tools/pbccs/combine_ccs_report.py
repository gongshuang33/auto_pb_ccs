#!/usr/bin/env python
# by fengli on 2022.3.5, for ccs version >= 6.3.0

import os
import glob
import argparse
from collections import OrderedDict


def get_line(report_txt):
    template = []

    for line in open(report_txt):
        template.append(line.strip())

    return template


def store_data(d_field, report_txt):
    for line in open(report_txt):
        line = line.strip()
        if (not line) or (':' not in line):
            continue

        field = line.split(':')[0]
        data = int(line.split(':')[-1].strip().split()[0])
        d_field.setdefault(field, []).append(data)

    return d_field


def main():
    parser = argparse.ArgumentParser(description="combine chunk ccs report")
    parser.add_argument('path', help="input ccs chunk dir")
    parser.add_argument('-p', '--prefix')
    args = parser.parse_args()

    path = os.path.abspath(args.path)
    file_list = glob.glob('%s/*/*ccs_report.txt' % path)

    template = get_line(file_list[0])
    d_field = OrderedDict()
    
    for txt in file_list:
        d_field = store_data(d_field, txt)
    for k,v in d_field.items():
        d_field[k] = sum(v)

    total_input = d_field["ZMWs input               "]
    total_pass = d_field["ZMWs pass filters        "]
    total_fail = d_field["ZMWs fail filters        "]

    with open('%s/%s.ccs_report_summary.txt' % (path, args.prefix), 'w') as out:
        for field in template:
            if field == "" or ':' not in field:
                out.write("{}\n".format(field))
            else:
                key = field.split(':')[0]
                if key == "ZMWs input               ":
                    value = total_input
                elif key in [
                    "ZMWs pass filters        ",
                    "ZMWs fail filters        ",
                    "ZMWs shortcut filters    "
                ]:
                    value = "{} ({:.3%})".format(
                        d_field[key],
                        float(d_field[key]) / total_input
                    )
                elif key == "ZMWs missing adapters    ":
                    value = "{} ({:.3%})".format(
                        d_field[key],
                        float(d_field[key]) / total_pass
                    )
                else:
                    value = "{} ({:.3%})".format(
                        d_field[key],
                        float(d_field[key]) / total_fail
                    )
                out.write("{}: {}\n".format(key, value))
        

if __name__ == "__main__":
    main()
