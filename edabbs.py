import os
import sys
from datetime import timedelta
import re
from itertools import islice
import csv

from libraries.acdDict import acdDict
from libraries.acdTime import acdTime
from libraries.files import uncompress, remove_files, remove_old_files


root_path = os.path.dirname(os.path.abspath(sys.argv[0]))
output_dir = os.path.join(root_path, "output")
temp_dir = os.path.join(root_path, "temp")
kpi360_dir = os.path.abspath("/var/cassandra/data/kpi360/")

dt_reg = re.compile(
    r'\d{4}-\d{2}-\d{2}_\d{2}.\d{2}.\d{2}.+\d{4}-\d{2}-\d{2}_\d{2}.\d{2}.\d{2}'
)
job = None
for root, dirs, files in os.walk(os.path.abspath(kpi360_dir)):
    for d in dirs:
        if dt_reg.match(d):
            job = d

cur_arc_dir = os.path.join(kpi360_dir, job)
job_file = os.path.join(output_dir, job + ".csv")

cur_unarc_dir = os.path.join(temp_dir, job)
if not os.path.isdir(cur_unarc_dir):
    os.mkdir(cur_unarc_dir)
else:
    remove_files(cur_unarc_dir)
    os.mkdir(cur_unarc_dir)

remove_old_files(output_dir, 7)

# Exclusion list
excl = [
    "FNSUB",
    "HLRSUB",
    "AUCSUB",
    "NPSUB",
    "FGNTC",
    "FGNTE",
    "FGNTI",
    "FGNTP",
    "{http://schemas.ericsson.com/pg/hlr/13.5/}PGSPI",
    "{http://schemas.ericsson.com/pg/hlr/13.5/}HESPLSP",
    "{http://schemas.ericsson.com/pg/hlr/13.5/}LOCATION"
]

r = acdDict()

line_reg = re.compile(r',(?=")')
for root, dirs, files in os.walk(os.path.abspath(cur_arc_dir)):
    for f in files:
        arc_f = os.path.join(root, f)
        arc_f_spl = os.path.splitext(arc_f)
        basename = os.path.basename(arc_f_spl[0])
        extension = os.path.splitext(arc_f_spl[1])[1]
        unarc_f = os.path.join(cur_unarc_dir, basename + extension)
        uncompress(arc_f, unarc_f)
        with open(unarc_f) as fh:
            while True:
                next_n_lines = list(islice(fh, 200000))
                if not next_n_lines:
                    break
                for i, s in enumerate(next_n_lines):
                    if "\"northbound\"" in s:
                        l = line_reg.split(next_n_lines[i])
                        trg = l[10]
                        if not ',' in trg:
                            trg = l[10].replace("\"", "")
                            if trg not in excl:
                                meth = l[5].replace("\"", "")
                                status = l[6].replace("\"", "")
                                r[trg][meth]['status'][status] += 1
                                err = int(l[13].replace("\"", ""))
                                if err != 0:
                                    r[trg][meth]['errors'][err] += 1
                                usr = l[7].replace("\"", "")
                                if usr != "":
                                    r[trg][meth]['users'][usr] += 1
                                else:
                                    r[trg][meth]['users']["Invalid session"] += 1
                                if not isinstance(r[trg][meth]['execTime'], list):
                                    r[trg][meth]['execTime'] = []
                                hrs = int(l[12].replace("\"", "")[3:5])
                                mnt = int(l[12].replace("\"", "")[6:8])
                                sec = int(l[12].replace("\"", "")[9:11])
                                msec = int(l[12].replace("\"", "")[12:])
                                r[trg][meth]['execTime'].append(
                                    timedelta(
                                        hours=hrs,
                                        minutes=mnt,
                                        seconds=sec,
                                        microseconds=msec
                                    )
                                )
        os.remove(unarc_f)
os.rmdir(cur_unarc_dir)

header = ["Target", "Type", "OK", "NOK", "Average Time",
          "Maximum Time", "Error Codes", "Users"]
with open(job_file, 'wb') as f:
    writer = csv.writer(f)
    writer.writerow(header)
    for ok, ov in r.iteritems():
        for ik, iv in ov.iteritems():
            t = acdTime(r[ok][ik]['execTime'])
            writer.writerow([
                ok,
                ik,
                r[ok][ik]['status']['SUCCESSFUL'] if r[ok][ik]['status']['SUCCESSFUL'] else 0,
                r[ok][ik]['status']['FAILED'] if r[ok][ik]['status']['FAILED'] else 0,
                t.average(),
                t.maximum(),
                '; '.join(['{0} - {1}'.format(k,v) for k,v in r[ok][ik]['errors'].iteritems()]),
                '; '.join(['{0} - {1}'.format(k,v) for k,v in r[ok][ik]['users'].iteritems()])
            ])
