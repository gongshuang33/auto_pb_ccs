#!/share/erapool/personal/smrtanalysis/software/miniconda/bin/python3

import os
import math
import argparse
from time import localtime
from collections import defaultdict

from matplotlib import pyplot as plt
from docxtpl import DocxTemplate

from read_file import read_length, n50


TEMPLATE = os.path.join(os.path.dirname(__file__), "template_s2.docx")


def stat_length(length):
    d = defaultdict(lambda : [0, 0])

    for leng in length:
        if leng > 0:
            d['>0'][0] += 1
            d['>0'][1] += leng
        if leng >= 5000:
            d['>=5,000'][0] += 1
            d['>=5,000'][1] += leng
        if leng >= 10000:
            d['>=10,000'][0] += 1
            d['>=10,000'][1] += leng
        if leng >= 15000:
            d['>=15,000'][0] += 1
            d['>=15,000'][1] += leng
        if leng >= 20000:
            d['>=20,000'][0] += 1
            d['>=20,000'][1] += leng
        if leng >= 25000:
            d['>=25,000'][0] += 1
            d['>=25,000'][1] += leng
        if leng >= 30000:
            d['>=30,000'][0] += 1
            d['>=30,000'][1] += leng
        if leng >= 35000:
            d['>=35,000'][0] += 1
            d['>=35,000'][1] += leng
        if leng >= 40000:
            d['>=40,000'][0] += 1
            d['>=40,000'][1] += leng

    return d


def stat_bam(movie, bam):

    length, n_pass, quality = read_length(bam)

    avg_length = 0 if len(length) == 0 else sum(length)*1.0/len(length)
    n50_len = n50(length)
    max_length = 0 if len(length) == 0 else max(length)
    n_q20, n_q30, n_q40 = 0,0,0
    for qv in quality:
        if qv >= 0.99:
            n_q20 += 1
        if qv >= 0.999:
            n_q30 += 1
        if qv >= 0.9999:
            n_q40 += 1

    render_dict = defaultdict(list)

    render_dict['table_qc'] = [
        [
            movie,
            "{:,}".format(len(length)),
            "{:,}".format(sum(length)),
            "{:,.2f}".format(avg_length),
            "{:,}".format(n50_len),
            "{:,}".format(max_length)
        ]
    ]

    len_stat = stat_length(length)
    for key in ['>0', '>=5,000', '>=10,000', '>=15,000', '>=20,000',
                '>=25,000', '>=30,000', '>=35,000', '>=40,000']:
        render_dict['table_length'].append(
            [
                movie,
                key,
                len_stat[key][0],
                int(len_stat[key][0] / len(length) * 100),
                len_stat[key][1],
                int(len_stat[key][1] / sum(length) * 100)
            ]
        )

    render_dict['table_quality'] = [
        [movie, 'Q20', n_q20, int(n_q20 / len(length) * 100)],
        [movie, 'Q30', n_q30, int(n_q30 / len(length) * 100)],
        [movie, 'Q40', n_q40, int(n_q40 / len(length) * 100)]
    ]

    render_dict["report_time"] = "{}年{}月".format(localtime()[0], localtime()[1])

    return render_dict, length, quality


def plot_replot(length, rq, outdir):

    # length hist
    plt.hist(length, bins=50, rwidth=0.8)
    plt.xlabel("HiFi Read Length, bp")
    plt.ylabel("Number of Reads")
    plt.title("HiFi Read Length Distribution")
    plt.savefig(os.path.join(outdir, "ccs_readlength_hist_plot.png"))

    # hist quality pair
    qv = []
    for i in rq:
        if i >= 0.99999:
            phred = 50
        else:
            phred = -10 * math.log10(1-i)
        qv.append(phred)

    fig, ax = plt.subplots()
    hb = ax.hexbin(length, qv, bins='log', gridsize=50, cmap="Spectral_r", mincnt=1)
    cb = fig.colorbar(hb, ax = ax)
    cb.set_label('Counts')
    cb.set_ticks([1, 10, 100, 1000, 10000])
    cb.set_ticklabels([1, 10, 100, 1000, 10000])

    ax.set_xlabel("Read Length, bp")
    yt = list(range(20, 49, 4))
    ytl = ["Q{}".format(i) for i in yt]
    ax.set_yticks(yt)
    ax.set_yticklabels(ytl)
    ax.set_ylabel("Predicted Accuracy (Phred Scale)")
    ax.set_title("Predicted Accuracy vs. Read Length")
    plt.savefig(os.path.join(outdir, "readlength_qv_hist2d.hexbin.png"))


def hifi_report(rend_dict, outfile, tpl):

    doc = DocxTemplate(tpl)
    doc.render(rend_dict)

    doc.replace_pic("example_length_dis.png", rend_dict['length_dis'])
    doc.replace_pic("example_len_qv_dis.png", rend_dict['len_qv_dis'])

    doc.save(outfile)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--input_bam', nargs='+', type=str, required=True, help="input ccs bam file")
    parser.add_argument('--outdir', default='./', help="output report dir, default: ./")
    args = parser.parse_args()

    for bam in args.input_bam:
        bam = os.path.abspath(bam)
        movie = os.path.basename(bam).split('.')[0]

        outdir = os.path.abspath(args.outdir)
        report = os.path.join(outdir, '{}.hifi_report.docx'.format(movie))

        render_dict, length, quality = stat_bam(movie, bam)
        plot_replot(length, quality, outdir)

        render_dict['length_dis'] = os.path.join(outdir, "ccs_readlength_hist_plot.png")
        render_dict['len_qv_dis'] = os.path.join(outdir, "readlength_qv_hist2d.hexbin.png")
        hifi_report(render_dict, report, tpl=TEMPLATE)


if __name__ == "__main__":
    main()



