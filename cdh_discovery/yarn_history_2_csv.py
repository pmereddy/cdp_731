#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CDH 6.3.3 discovery. Python 3.6+ compatible. Stdlib only.
Parse MapReduce job history conf.xml files from HDFS for rich dependency info.

Reads *_conf.xml from /user/history/done/YYYY/MM/DD/*/ via a single
  hdfs dfs -cat <glob>
per day, so it needs only one HDFS RPC per day regardless of job count.

Extracts:
  - Job metadata (name, user, queue, submit time, tags)
  - Oozie workflow / coordinator linkage
  - Input / output HDFS paths  (data lineage!)
  - Hive queries (SQL text)
  - Pig, Spark, Cascading app names

Output:
  yarn_objects.csv   -- job metadata (dependency_objects schema)
  yarn_access.csv    -- job submissions (dependency_access schema)
  yarn_lineage.csv   -- input->job->output data flow edges

Requires: hdfs CLI on PATH.
"""

from __future__ import print_function

import argparse
import csv
import datetime as dt
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET

OBJECT_FIELDS = ["service", "object_type", "object_id", "owner", "group", "extra"]
ACCESS_FIELDS = [
    "window_start", "service", "user", "do_as", "client_ip",
    "app_name", "op", "object_type", "object_id", "cnt",
]
LINEAGE_FIELDS = [
    "from_object_type", "from_object_id",
    "to_object_type", "to_object_id", "relation",
]

LOG = "[yarn_hist]"

PROP_KEYS = {
    "mapreduce.job.id",
    "mapreduce.job.name",
    "mapred.job.name",
    "mapreduce.job.user.name",
    "user.name",
    "mapreduce.job.queuename",
    "mapreduce.job.submithostname",
    "mapreduce.job.tags",
    "mapreduce.input.fileinputformat.inputdir",
    "mapreduce.output.fileoutputformat.outputdir",
    "oozie.job.id",
    "oozie.action.id",
    "hive.query.string",
    "hive.query.id",
    "pig.script.id",
    "spark.app.name",
    "spark.app.id",
    "cascading.app.name",
}


def date_range(since, until):
    d = since
    while d <= until:
        yield d
        d += dt.timedelta(days=1)


def hdfs_dir_exists(path):
    r = subprocess.call(
        ["hdfs", "dfs", "-test", "-d", path],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    return r == 0


def parse_conf_xml(xml_str):
    """Parse a Hadoop conf.xml string, return dict of interesting properties."""
    props = {}
    try:
        root = ET.fromstring(xml_str)
        for prop_el in root.findall("property"):
            name_el = prop_el.find("name")
            val_el = prop_el.find("value")
            if name_el is None or val_el is None:
                continue
            name = (name_el.text or "").strip()
            if name in PROP_KEYS:
                props[name] = (val_el.text or "").strip()
    except ET.ParseError:
        pass
    return props


def split_paths(path_str):
    """Split comma-separated HDFS paths, strip globs."""
    if not path_str:
        return []
    paths = []
    for p in path_str.split(","):
        p = p.strip()
        if not p:
            continue
        p = re.sub(r'\{[^}]*\}', '', p)
        p = re.sub(r'/\*.*', '', p)
        p = p.rstrip("/")
        if p:
            paths.append(p)
    return list(set(paths))


def truncate_path(path, depth=5):
    if not path:
        return ""
    parts = path.strip().split("/")
    if len(parts) <= depth + 1:
        return path.strip()
    return "/".join(parts[:depth + 1])


def stream_conf_xmls(hdfs_glob):
    """Stream individual conf.xml documents from a glob cat.

    hdfs dfs -cat concatenates all matching files. We split on
    </configuration> boundaries to recover individual documents.
    """
    proc = subprocess.Popen(
        ["hdfs", "dfs", "-cat", hdfs_glob],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    buf = []
    for raw_line in proc.stdout:
        line = raw_line.decode("utf-8", errors="replace")
        buf.append(line)
        if "</configuration>" in line:
            xml_str = "".join(buf)
            end = xml_str.rfind("</configuration>")
            doc = xml_str[: end + len("</configuration>")]
            start = doc.find("<?xml")
            if start < 0:
                start = doc.find("<configuration")
            if start >= 0:
                doc = doc[start:]
            yield doc
            remainder = xml_str[end + len("</configuration>"):]
            buf = [remainder] if remainder.strip() else []

    proc.wait()


def process_day(hdfs_base, day, path_depth, obj_w, acc_w, lin_w):
    """Process all conf.xml files for a single day. Returns (jobs, lineage_edges)."""
    yyyy = str(day.year)
    mm = "{:02d}".format(day.month)
    dd = "{:02d}".format(day.day)
    hdfs_day = "{}/{}/{}/{}".format(hdfs_base, yyyy, mm, dd)

    if not hdfs_dir_exists(hdfs_day):
        return 0, 0

    glob_pattern = "{}/*/*_conf.xml".format(hdfs_day)
    jobs = 0
    lineage = 0

    for xml_doc in stream_conf_xmls(glob_pattern):
        p = parse_conf_xml(xml_doc)
        if not p:
            continue

        job_id = p.get("mapreduce.job.id", "")
        if not job_id:
            continue

        user = p.get("mapreduce.job.user.name") or p.get("user.name", "")
        job_name = p.get("mapreduce.job.name") or p.get("mapred.job.name", "")
        queue = p.get("mapreduce.job.queuename", "")
        submit_host = p.get("mapreduce.job.submithostname", "")
        tags = p.get("mapreduce.job.tags", "")
        input_dirs = p.get("mapreduce.input.fileinputformat.inputdir", "")
        output_dir = p.get("mapreduce.output.fileoutputformat.outputdir", "")
        oozie_wf = p.get("oozie.job.id", "")
        oozie_action = p.get("oozie.action.id", "")
        hive_query = p.get("hive.query.string", "")
        hive_qid = p.get("hive.query.id", "")
        pig_id = p.get("pig.script.id", "")
        spark_app = p.get("spark.app.name", "")
        spark_id = p.get("spark.app.id", "")
        cascading = p.get("cascading.app.name", "")

        iso_time = day.strftime("%Y-%m-%dT00:00:00Z")

        extras = []
        if queue:
            extras.append("queue={}".format(queue))
        if oozie_wf:
            extras.append("oozie_wf={}".format(oozie_wf))
        if oozie_action:
            extras.append("oozie_action={}".format(oozie_action))
        if hive_query:
            extras.append("hive_query={}".format(
                hive_query.replace("\n", " ").replace("|", " ")[:500]))
        if hive_qid:
            extras.append("hive_qid={}".format(hive_qid))
        if pig_id:
            extras.append("pig={}".format(pig_id))
        if spark_app:
            extras.append("spark_app={}".format(spark_app))
        if spark_id:
            extras.append("spark_id={}".format(spark_id))
        if cascading:
            extras.append("cascading={}".format(cascading))
        if tags:
            extras.append("tags={}".format(tags[:200]))
        if input_dirs:
            extras.append("input={}".format(input_dirs[:500]))
        if output_dir:
            extras.append("output={}".format(output_dir[:500]))

        obj_w.writerow({
            "service": "yarn",
            "object_type": "mapreduce_job",
            "object_id": job_id,
            "owner": user,
            "group": queue,
            "extra": "|".join(extras),
        })

        acc_w.writerow({
            "window_start": iso_time,
            "service": "yarn",
            "user": user,
            "do_as": "",
            "client_ip": submit_host,
            "app_name": job_name[:200],
            "op": "SUBMIT",
            "object_type": "mapreduce_job",
            "object_id": job_id,
            "cnt": 1,
        })

        # Lineage: input HDFS paths -> job
        for inp in split_paths(input_dirs):
            t_inp = truncate_path(inp, path_depth)
            if t_inp:
                lin_w.writerow({
                    "from_object_type": "hdfs_path",
                    "from_object_id": t_inp,
                    "to_object_type": "mapreduce_job",
                    "to_object_id": job_id,
                    "relation": "INPUT",
                })
                lineage += 1

        # Lineage: job -> output HDFS path
        if output_dir:
            t_out = truncate_path(output_dir.strip(), path_depth)
            if t_out:
                lin_w.writerow({
                    "from_object_type": "mapreduce_job",
                    "from_object_id": job_id,
                    "to_object_type": "hdfs_path",
                    "to_object_id": t_out,
                    "relation": "OUTPUT",
                })
                lineage += 1

        # Lineage: oozie workflow -> job
        if oozie_wf:
            lin_w.writerow({
                "from_object_type": "oozie_workflow",
                "from_object_id": oozie_wf,
                "to_object_type": "mapreduce_job",
                "to_object_id": job_id,
                "relation": "LAUNCHES",
            })
            lineage += 1

        # Lineage: oozie action -> job
        if oozie_action:
            lin_w.writerow({
                "from_object_type": "oozie_action",
                "from_object_id": oozie_action,
                "to_object_type": "mapreduce_job",
                "to_object_id": job_id,
                "relation": "LAUNCHES",
            })
            lineage += 1

        jobs += 1

    return jobs, lineage


def main():
    ap = argparse.ArgumentParser(
        description="Extract YARN job history from HDFS conf.xml files."
    )
    ap.add_argument("--hdfs-base", default="/user/history/done",
                    help="HDFS base path (default: /user/history/done)")
    ap.add_argument("--since", required=True, help="Start date YYYY-MM-DD")
    ap.add_argument("--until", default="",
                    help="End date YYYY-MM-DD (default: today)")
    ap.add_argument("--path-depth", type=int, default=5,
                    help="Truncate I/O paths to N levels for lineage (default: 5)")
    ap.add_argument("--out-objects", default="yarn_objects.csv")
    ap.add_argument("--out-access", default="yarn_access.csv")
    ap.add_argument("--out-lineage", default="yarn_lineage.csv")
    args = ap.parse_args()

    since = dt.datetime.strptime(args.since, "%Y-%m-%d").date()
    until = (dt.datetime.strptime(args.until, "%Y-%m-%d").date()
             if args.until else dt.date.today())

    print("{} Date range: {} to {}".format(LOG, since, until), file=sys.stderr)
    print("{} HDFS base: {}".format(LOG, args.hdfs_base), file=sys.stderr)

    total_jobs = 0
    total_lineage = 0

    with open(args.out_objects, "w", newline="") as f_obj, \
         open(args.out_access, "w", newline="") as f_acc, \
         open(args.out_lineage, "w", newline="") as f_lin:

        obj_w = csv.DictWriter(f_obj, fieldnames=OBJECT_FIELDS)
        acc_w = csv.DictWriter(f_acc, fieldnames=ACCESS_FIELDS)
        lin_w = csv.DictWriter(f_lin, fieldnames=LINEAGE_FIELDS)
        obj_w.writeheader()
        acc_w.writeheader()
        lin_w.writeheader()

        for day in date_range(since, until):
            j, l = process_day(
                args.hdfs_base, day, args.path_depth, obj_w, acc_w, lin_w)
            total_jobs += j
            total_lineage += l
            if j > 0:
                print("{} {}: {:,} jobs, {:,} lineage edges  (total: {:,} / {:,})".format(
                    LOG, day.strftime("%Y-%m-%d"), j, l,
                    total_jobs, total_lineage), file=sys.stderr)
            f_obj.flush()
            f_acc.flush()
            f_lin.flush()

    print("{} Done.".format(LOG), file=sys.stderr)
    print("{}   {:,} jobs     -> {}".format(LOG, total_jobs, args.out_objects), file=sys.stderr)
    print("{}   {:,} access   -> {}".format(LOG, total_jobs, args.out_access), file=sys.stderr)
    print("{}   {:,} lineage  -> {}".format(LOG, total_lineage, args.out_lineage), file=sys.stderr)


if __name__ == "__main__":
    main()
