#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CDH 6.3.3 discovery. Python 3.6+ compatible. Stdlib only.
Parse MapReduce job history conf.xml files from HDFS for rich dependency info.

Reads *_conf.xml from /user/history/done/YYYY/MM/DD/*/ via
  hdfs dfs -cat <glob>
per day (one HDFS client per day when using multiple workers).

Parallelism: use --workers N to process multiple calendar days at once.

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
import io
import multiprocessing
import os
import re
import shutil
import subprocess
import sys
import tempfile
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
    "mapred.job.id",
    "mapreduce.job.name",
    "mapred.job.name",
    "mapreduce.job.user.name",
    "user.name",
    "mapreduce.job.queuename",
    "mapred.job.queue.name",
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
    # YARN 2.x / CDH 6.3 often omit mapreduce.job.id; id still appears here:
    "mapreduce.job.dir",
    "mapreduce.job.jar",
}

# Fuzzy key matching (lowercase) when exact key not in PROP_KEYS
USER_KEY_SUBSTR = ("mapreduce.job.user.name", "job.user.name", "user.name")
NAME_KEY_SUBSTR = ("job.name",)
INPUT_KEY_SUBSTR = ("fileinputformat.inputdir", "input.dir")
OUTPUT_KEY_SUBSTR = ("fileoutputformat.outputdir", "output.dir")

CHUNK_SIZE = 1024 * 1024
# Closing tag may vary in case / whitespace
CONF_END_RE = re.compile(br"</configuration\s*>", re.IGNORECASE)

# Canonical property name for each lowercase key (XML name casing variants)
_PROP_BY_LOWER = {k.lower(): k for k in PROP_KEYS}


def _next_configuration_doc(buf):
    """Find first </configuration> in buf; return (doc_bytes, rest_after) or None."""
    m = CONF_END_RE.search(buf)
    if not m:
        return None
    end = m.end()
    piece = buf[:end]
    rest = buf[end:]
    return piece, rest


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


def _local_tag(tag):
    """Strip XML namespace: {http://...}property -> property."""
    if not tag:
        return ""
    if tag[0] == "{" and "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _elem_text(el):
    if el is None:
        return ""
    parts = []
    if el.text:
        parts.append(el.text)
    for sub in el:
        if sub.tail:
            parts.append(sub.tail)
    return "".join(parts).strip()


def parse_conf_xml(xml_str):
    """Parse a Hadoop conf.xml string, return dict of interesting properties.

    Hadoop conf.xml almost always declares xmlns on <configuration>.  ElementTree's
    findall('property') then matches nothing unless you register namespaces.
    We iterate children and strip {namespace} from tags so properties parse.
    """
    props = {}
    if not xml_str or not xml_str.strip():
        return props
    # Strip BOM if present
    if xml_str.startswith("\ufeff"):
        xml_str = xml_str[1:]
    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return props

    all_kv = []

    for prop_el in root:
        if _local_tag(prop_el.tag) != "property":
            continue
        name_el = None
        val_el = None
        for sub in prop_el:
            ln = _local_tag(sub.tag)
            if ln == "name":
                name_el = sub
            elif ln == "value":
                val_el = sub
        if name_el is None:
            continue
        name = (_elem_text(name_el) or (name_el.text or "")).strip()
        val = ""
        if val_el is not None:
            val = (_elem_text(val_el) or (val_el.text or "")).strip()
        if not name:
            continue
        all_kv.append((name, val))
        canon = _PROP_BY_LOWER.get(name.lower())
        if canon:
            props[canon] = val

    def pick(substr_tuple, dest_keys):
        for sk, sv in all_kv:
            sl = sk.lower()
            for sub in substr_tuple:
                if sub in sl:
                    for dk in dest_keys:
                        if dk not in props or not props[dk]:
                            props[dk] = sv
                    return

    if not props.get("mapreduce.job.id") and not props.get("mapred.job.id"):
        for sk, sv in all_kv:
            sl = sk.lower()
            if "job.id" in sl or sl.endswith("jobid") or sl == "yarn.app.id":
                if sv and re.match(r"^(job_|application_)\d", sv):
                    props.setdefault("mapreduce.job.id", sv)
                    break
            if sl == "mapreduce.job.id" or sl == "mapred.job.id":
                if sv:
                    props.setdefault("mapreduce.job.id", sv)
                    break

    if not props.get("mapreduce.job.user.name") and not props.get("user.name"):
        pick(USER_KEY_SUBSTR, ("mapreduce.job.user.name", "user.name"))

    if not props.get("mapreduce.job.name") and not props.get("mapred.job.name"):
        pick(NAME_KEY_SUBSTR, ("mapreduce.job.name", "mapred.job.name"))

    if not props.get("mapreduce.input.fileinputformat.inputdir"):
        pick(INPUT_KEY_SUBSTR, ("mapreduce.input.fileinputformat.inputdir",))

    if not props.get("mapreduce.output.fileoutputformat.outputdir"):
        pick(OUTPUT_KEY_SUBSTR, ("mapreduce.output.fileoutputformat.outputdir",))

    # CDH 6.3 / MR on YARN: mapreduce.job.id is often absent; staging path has it
    if not props.get("mapreduce.job.id") and not props.get("mapred.job.id"):
        for _k, _v in (
            ("mapreduce.job.dir", props.get("mapreduce.job.dir", "")),
            ("mapreduce.job.jar", props.get("mapreduce.job.jar", "")),
        ):
            if not _v:
                continue
            m = re.search(r"(job_\d+_\d+)", _v)
            if m:
                props["mapreduce.job.id"] = m.group(1)
                break

    return props


def job_id_from_props(p):
    return (
        p.get("mapreduce.job.id", "")
        or p.get("mapred.job.id", "")
    )


_JOB_ID_IN_PATH = re.compile(r"(job_\d+_\d+)")


def derive_job_id(p, xml_doc, filename_hint):
    """Resolve MapReduce job id (YARN 2 uses job_<epoch>_<seq>, often not in mapreduce.job.id)."""
    jid = job_id_from_props(p)
    if jid:
        return jid

    if filename_hint:
        base = os.path.basename(filename_hint)
        m = re.match(r"(job_\d+_\d+)_conf\.xml$", base, re.I)
        if m:
            return m.group(1)

    for v in (p.get("mapreduce.job.dir", ""), p.get("mapreduce.job.jar", "")):
        if v:
            m = _JOB_ID_IN_PATH.search(v)
            if m:
                return m.group(1)

    # Full document: id repeats in classpath, cache files, etc. — pick mode
    cands = re.findall(r"\bjob_\d+_\d+\b", xml_doc)
    if cands:
        return max(set(cands), key=cands.count)

    return ""


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


def stream_conf_xmls(hdfs_glob, log_prefix=""):
    """Stream individual conf.xml documents from hdfs dfs -cat <glob>.

    Reads stdout in binary chunks and splits on </configuration> so we
    never miss a boundary when the closing tag is not on its own line.

    stderr is sent to DEVNULL to avoid PIPE deadlocks on long runs.
    """
    proc = subprocess.Popen(
        ["hdfs", "dfs", "-cat", hdfs_glob],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    buf = b""
    try:
        while True:
            chunk = proc.stdout.read(CHUNK_SIZE)
            if not chunk:
                break
            buf += chunk
            while True:
                found = _next_configuration_doc(buf)
                if found is None:
                    break
                piece, buf = found
                try:
                    text = piece.decode("utf-8", errors="replace")
                except Exception:
                    continue
                start = text.find("<?xml")
                if start < 0:
                    start = text.find("<configuration")
                if start >= 0:
                    yield text[start:]
    finally:
        proc.stdout.close()
        rc = proc.wait()
        if rc != 0 and log_prefix:
            print("{} WARN: hdfs dfs -cat exit={} glob={}".format(
                log_prefix, rc, hdfs_glob), file=sys.stderr)


def hdfs_ls_conf_paths(hdfs_day):
    """Fallback: list *conf*.xml under hdfs_day (recursive)."""
    try:
        out = subprocess.check_output(
            ["hdfs", "dfs", "-ls", "-R", hdfs_day],
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        return []

    paths = []
    for line in out.decode("utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("Found "):
            continue
        parts = line.split(None, 7)
        if len(parts) < 8:
            continue
        pth = parts[-1].strip()
        base = os.path.basename(pth)
        if not pth.endswith(".xml"):
            continue
        bn = base.lower()
        # Typical: job_*_conf.xml; some sites use other *.xml in the job dir
        if "conf" in bn or bn.startswith("job_"):
            paths.append(pth)
    return paths


def cat_paths_to_xml_stream(paths, batch_size=200):
    """Yield XML documents by batching hdfs dfs -cat on explicit paths."""
    if not paths:
        return
    for i in range(0, len(paths), batch_size):
        batch = paths[i:i + batch_size]
        proc = subprocess.Popen(
            ["hdfs", "dfs", "-cat"] + batch,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        buf = b""
        try:
            while True:
                chunk = proc.stdout.read(CHUNK_SIZE)
                if not chunk:
                    break
                buf += chunk
                while True:
                    found = _next_configuration_doc(buf)
                    if found is None:
                        break
                    piece, buf = found
                    text = piece.decode("utf-8", errors="replace")
                    start = text.find("<?xml")
                    if start < 0:
                        start = text.find("<configuration")
                    if start >= 0:
                        yield text[start:]
        finally:
            proc.stdout.close()
            proc.wait()


def process_day_core(hdfs_base, day, path_depth, obj_w, acc_w, lin_w, log_prefix):
    """Process one day; writers must be ready. Returns (jobs, lineage, used_fallback)."""
    yyyy = str(day.year)
    mm = "{:02d}".format(day.month)
    dd = "{:02d}".format(day.day)
    hdfs_day = "{}/{}/{}/{}".format(hdfs_base, yyyy, mm, dd)

    if not hdfs_dir_exists(hdfs_day):
        return 0, 0, False

    glob_pattern = "{}/*/*_conf.xml".format(hdfs_day)
    jobs = 0
    lineage = 0
    used_fallback = False
    doc_count = 0

    for xml_doc in stream_conf_xmls(glob_pattern, log_prefix=log_prefix):
        doc_count += 1
        j, l = _write_one_job_xml(
            xml_doc, day, path_depth, obj_w, acc_w, lin_w, None)
        jobs += j
        lineage += l

    # If glob returned no XML at all, try explicit listing (some HDFS builds
    # expand globs poorly for -cat).
    if doc_count == 0:
        listed = hdfs_ls_conf_paths(hdfs_day)
        if listed:
            used_fallback = True
            if log_prefix:
                print("{} {}: glob returned 0 docs; trying -ls -R ({} xml paths)".format(
                    log_prefix, day.strftime("%Y-%m-%d"), len(listed)), file=sys.stderr)
            for xml_doc in cat_paths_to_xml_stream(listed):
                doc_count += 1
                j, l = _write_one_job_xml(
                    xml_doc, day, path_depth, obj_w, acc_w, lin_w, None)
                jobs += j
                lineage += l

    if doc_count > 0 and jobs == 0 and log_prefix:
        print("{} {}: parsed {} XML doc(s) but 0 jobs (check mapreduce.job.dir / full XML for job_*_* )".format(
            log_prefix, day.strftime("%Y-%m-%d"), doc_count), file=sys.stderr)

    return jobs, lineage, used_fallback


def _write_one_job_xml(xml_doc, day, path_depth, obj_w, acc_w, lin_w, filename_hint):
    p = parse_conf_xml(xml_doc)
    job_id = derive_job_id(p, xml_doc, filename_hint)

    if not job_id:
        return 0, 0

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

    lineage = 0

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

    if oozie_wf:
        lin_w.writerow({
            "from_object_type": "oozie_workflow",
            "from_object_id": oozie_wf,
            "to_object_type": "mapreduce_job",
            "to_object_id": job_id,
            "relation": "LAUNCHES",
        })
        lineage += 1

    if oozie_action:
        lin_w.writerow({
            "from_object_type": "oozie_action",
            "from_object_id": oozie_action,
            "to_object_type": "mapreduce_job",
            "to_object_id": job_id,
            "relation": "LAUNCHES",
        })
        lineage += 1

    return 1, lineage


def _process_day_worker(args_tuple):
    """Multiprocessing entry: write one day's rows to three temp files (no headers)."""
    (hdfs_base, day_iso, path_depth, tmpdir) = args_tuple
    day = dt.datetime.strptime(day_iso, "%Y-%m-%d").date()
    prefix = os.path.join(tmpdir, day_iso.replace("-", "_"))
    obj_p = prefix + ".objects.csv"
    acc_p = prefix + ".access.csv"
    lin_p = prefix + ".lineage.csv"
    log_prefix = "{} {}".format(LOG, day_iso)

    with io.open(obj_p, "w", encoding="utf-8", newline="") as f_obj, \
         io.open(acc_p, "w", encoding="utf-8", newline="") as f_acc, \
         io.open(lin_p, "w", encoding="utf-8", newline="") as f_lin:
        obj_w = csv.DictWriter(f_obj, fieldnames=OBJECT_FIELDS, extrasaction="ignore")
        acc_w = csv.DictWriter(f_acc, fieldnames=ACCESS_FIELDS, extrasaction="ignore")
        lin_w = csv.DictWriter(f_lin, fieldnames=LINEAGE_FIELDS, extrasaction="ignore")
        jobs, lineage, fb = process_day_core(
            hdfs_base, day, path_depth, obj_w, acc_w, lin_w, log_prefix)

    return {
        "day": day_iso,
        "jobs": jobs,
        "lineage": lineage,
        "fallback": fb,
        "obj": obj_p,
        "acc": acc_p,
        "lin": lin_p,
    }


def merge_raw_csv_files(part_paths, out_path, fieldnames):
    """Concatenate CSV body files that have no header (DictWriter rows only)."""
    with io.open(out_path, "w", encoding="utf-8", newline="") as out:
        w = csv.DictWriter(out, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for p in part_paths:
            if not p or not os.path.isfile(p) or os.path.getsize(p) == 0:
                continue
            with io.open(p, "r", encoding="utf-8", newline="") as inf:
                shutil.copyfileobj(inf, out)


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
    ap.add_argument("--workers", type=int, default=0,
                    help="Parallel worker processes (0 = auto, 1 = serial)")
    ap.add_argument("--out-objects", default="yarn_objects.csv")
    ap.add_argument("--out-access", default="yarn_access.csv")
    ap.add_argument("--out-lineage", default="yarn_lineage.csv")
    args = ap.parse_args()

    since = dt.datetime.strptime(args.since, "%Y-%m-%d").date()
    until = (dt.datetime.strptime(args.until, "%Y-%m-%d").date()
             if args.until else dt.date.today())

    ncpu = multiprocessing.cpu_count() or 4
    workers = args.workers
    if workers <= 0:
        workers = min(8, max(1, ncpu))
    else:
        workers = max(1, workers)

    print("{} Date range: {} to {}".format(LOG, since, until), file=sys.stderr)
    print("{} HDFS base: {}".format(LOG, args.hdfs_base), file=sys.stderr)
    print("{} Workers: {}".format(LOG, workers), file=sys.stderr)

    days = list(date_range(since, until))
    if not days:
        print("{} No days in range.".format(LOG), file=sys.stderr)
        return

    total_jobs = 0
    total_lineage = 0

    tmpdir = tempfile.mkdtemp(prefix="yarn_hist_")
    try:
        if workers == 1:
            # Serial path (same process, write final files directly)
            with io.open(args.out_objects, "w", encoding="utf-8", newline="") as f_obj, \
                 io.open(args.out_access, "w", encoding="utf-8", newline="") as f_acc, \
                 io.open(args.out_lineage, "w", encoding="utf-8", newline="") as f_lin:
                obj_w = csv.DictWriter(f_obj, fieldnames=OBJECT_FIELDS, extrasaction="ignore")
                acc_w = csv.DictWriter(f_acc, fieldnames=ACCESS_FIELDS, extrasaction="ignore")
                lin_w = csv.DictWriter(f_lin, fieldnames=LINEAGE_FIELDS, extrasaction="ignore")
                obj_w.writeheader()
                acc_w.writeheader()
                lin_w.writeheader()
                for day in days:
                    j, l, fb = process_day_core(
                        args.hdfs_base, day, args.path_depth,
                        obj_w, acc_w, lin_w, LOG)
                    total_jobs += j
                    total_lineage += l
                    msg = "{} {}: {:,} jobs, {:,} lineage".format(
                        LOG, day.strftime("%Y-%m-%d"), j, l)
                    if fb:
                        msg += " (used -ls -R fallback)"
                    if j > 0 or fb:
                        print(msg, file=sys.stderr)
        else:
            work = [
                (args.hdfs_base, d.strftime("%Y-%m-%d"), args.path_depth, tmpdir)
                for d in days
            ]
            pool = multiprocessing.Pool(processes=workers)
            try:
                results = pool.map(_process_day_worker, work)
            finally:
                pool.close()
                pool.join()

            results.sort(key=lambda r: r["day"])
            for r in results:
                total_jobs += r["jobs"]
                total_lineage += r["lineage"]
                msg = "{} {}: {:,} jobs, {:,} lineage".format(
                    LOG, r["day"], r["jobs"], r["lineage"])
                if r.get("fallback"):
                    msg += " (used -ls -R fallback)"
                if r["jobs"] > 0 or r.get("fallback"):
                    print(msg, file=sys.stderr)

            obj_parts = [r["obj"] for r in results]
            acc_parts = [r["acc"] for r in results]
            lin_parts = [r["lin"] for r in results]

            merge_raw_csv_files(obj_parts, args.out_objects, OBJECT_FIELDS)
            merge_raw_csv_files(acc_parts, args.out_access, ACCESS_FIELDS)
            merge_raw_csv_files(lin_parts, args.out_lineage, LINEAGE_FIELDS)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    print("{} Done.".format(LOG), file=sys.stderr)
    print("{}   {:,} jobs     -> {}".format(LOG, total_jobs, args.out_objects), file=sys.stderr)
    print("{}   {:,} access   -> {}".format(LOG, total_jobs, args.out_access), file=sys.stderr)
    print("{}   {:,} lineage  -> {}".format(LOG, total_lineage, args.out_lineage), file=sys.stderr)

    if total_jobs == 0:
        print("{} TIP: Verify a day path exists, e.g. hdfs dfs -ls {}/YYYY/MM/DD".format(
            LOG, args.hdfs_base), file=sys.stderr)
        print("{} TIP: Check for *conf*.xml: hdfs dfs -ls -R <that_day> | head".format(
            LOG), file=sys.stderr)


if __name__ == "__main__":
    # Required for fork safety on some platforms when using Pool
    multiprocessing.freeze_support()
    main()
