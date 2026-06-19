"""
===============================================================================
AWS Glue Job : Data Vault 2.0 — Data Quality Validation Report Generator
===============================================================================
Purpose
-------
Auto-discovers every HUB_*, LINK_*, SAT_* table in a Glue Catalog database
(your RAW_VAULT layer), runs a standard battery of Data-Vault-aware DQ checks
against each one, and renders a single self-contained HTML report in the exact
visual format of the OrderLens "DV Validation Report" (hero banner, 6 KPI
tiles, Results-by-Category table, Per-Table Summary chips, Issues-That-Need-
Fixing cards, and a full grouped All-Check-Results table) — then uploads the
HTML to S3.

No checks are hard-coded to specific table/column names. Everything is
derived from:
  1. Glue Catalog table names (HUB_ / LINK_ / SAT_ prefix convention)
  2. Glue Catalog column lists (to find *_HK columns, LOAD_DTS, RECORD_SOURCE,
     HASH_DIFF — with common synonym fallbacks)
  3. Standard Data Vault 2.0 naming convention:
       HUB_<Entity>           -> own hash key = <Entity>_HK
       LINK_<Entity1_Entity2> -> own hash key = <Entity1_Entity2>_HK
       SAT_<Entity>           -> parent hash key = <Entity>_HK
     A LINK/SAT foreign-key column is matched to its parent HUB/LINK by exact
     hash-key-name equality, so orphan/referential checks are generated
     automatically — exactly like the source report.

Run as a Glue job (Glue 4.0 / Spark 3.x, Python 3) with these arguments:

  --JOB_NAME                 dv_dq_validation_report
  --DATABASE_NAME             orderlens                        (optional — this is the default; Glue DB w/ HUB/LINK/SAT tables)
  --SCHEMA_LABEL              "ORDERLENS.RAW_VAULT"            (optional — this is the default; label shown in the report header)
  --OUTPUT_S3_PATH            s3://yignite-orderlens-miniature-landing/validation_report/  (optional — this is the default)
  --REPORT_TITLE              "ORDERLENS"                       (optional, defaults to DATABASE_NAME upper)
  --FRESHNESS_STALE_HOURS     48                                (optional, default 48)
  --SPIKE_MULTIPLIER          5                                 (optional, default 5)
  --MART_DATABASE_NAME        sales_opsplanning_information_mart (optional, informational only)

Output:
  s3://yignite-orderlens-miniature-landing/validation_report/dv_validation_report_<database>.html
  s3://yignite-orderlens-miniature-landing/validation_report/dv_validation_report_<database>.json
===============================================================================
"""

import sys
import json
import datetime
import re

import boto3

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# ──────────────────────────────────────────────────────────────────────────────
# 0. JOB BOOTSTRAP & ARGS
# ──────────────────────────────────────────────────────────────────────────────
ARG_NAMES = [
    "JOB_NAME",
]
OPTIONAL_ARG_DEFAULTS = {
    "DATABASE_NAME": "orderlens_rawvault",
    "SCHEMA_LABEL": "ORDERLENS.RAWVAULT",
    "OUTPUT_S3_PATH": "s3://yignite-orderlens-miniature-landing/validation_report/",
    "REPORT_TITLE": None,
    "FRESHNESS_STALE_HOURS": "48",
    "SPIKE_MULTIPLIER": "5",
    "MART_DATABASE_NAME": "",
}

# getResolvedOptions only raises for args you actually request, so pull the
# required ones strictly and the optional ones permissively.
args = getResolvedOptions(sys.argv, ARG_NAMES)
for opt, default in OPTIONAL_ARG_DEFAULTS.items():
    flag = f"--{opt}"
    if flag in sys.argv:
        args[opt] = getResolvedOptions(sys.argv, [opt])[opt]
    else:
        args[opt] = default

DATABASE_NAME         = args["DATABASE_NAME"]
SCHEMA_LABEL          = args["SCHEMA_LABEL"]
OUTPUT_S3_PATH        = args["OUTPUT_S3_PATH"].rstrip("/")
REPORT_TITLE          = (args["REPORT_TITLE"] or DATABASE_NAME).upper()
FRESHNESS_STALE_HOURS = int(args["FRESHNESS_STALE_HOURS"])
SPIKE_MULTIPLIER      = int(args["SPIKE_MULTIPLIER"])
MART_DATABASE_NAME    = args["MART_DATABASE_NAME"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

glue_client = boto3.client("glue")
s3_client = boto3.client("s3")

RUN_TS = datetime.datetime.utcnow()

# ──────────────────────────────────────────────────────────────────────────────
# 1. CATALOG DISCOVERY — list HUB / LINK / SAT tables & columns
# ──────────────────────────────────────────────────────────────────────────────
def list_glue_tables(database):
    out = []
    paginator = glue_client.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=database):
        out.extend(page["TableList"])
    return out


def classify_table(name):
    n = name.upper()
    if n.startswith("HUB_") or n.startswith("H_"):
        return "HUB"
    if n.startswith("LINK_") or n.startswith("L_"):
        return "LINK"
    if n.startswith("SAT_") or n.startswith("S_"):
        return "SAT"
    return "OTHER"


# Common synonyms for the standard Data Vault audit columns, in priority order.
SYNONYMS = {
    "LOAD_DTS":      ["LOAD_DTS", "LOAD_DATE", "LOAD_TS", "DV_LOAD_DTS"],
    "RECORD_SOURCE": ["RECORD_SOURCE", "RECORD_SRC", "REC_SRC", "DV_RECORD_SOURCE"],
    "HASH_DIFF":     ["HASH_DIFF", "HASHDIFF", "HASH_DIFF_COL", "DV_HASHDIFF"],
}


def resolve_column(columns_upper, logical_name):
    for cand in SYNONYMS.get(logical_name, [logical_name]):
        if cand in columns_upper:
            return cand
    return None


raw_tables = list_glue_tables(DATABASE_NAME)
catalog = []
for t in raw_tables:
    name = t["Name"]
    cols = [c["Name"].upper() for c in t.get("StorageDescriptor", {}).get("Columns", [])]
    ttype = classify_table(name)
    if ttype == "OTHER":
        continue
    catalog.append({
        "name": name.upper(),
        "glue_name": name,
        "type": ttype,
        "columns": cols,
        "load_dts_col": resolve_column(cols, "LOAD_DTS"),
        "record_source_col": resolve_column(cols, "RECORD_SOURCE"),
        "hash_diff_col": resolve_column(cols, "HASH_DIFF"),
        "hk_columns": [c for c in cols if c.endswith("_HK")],
    })

hub_tables  = [t for t in catalog if t["type"] == "HUB"]
link_tables = [t for t in catalog if t["type"] == "LINK"]
sat_tables  = [t for t in catalog if t["type"] == "SAT"]

if not catalog:
    all_names = [t["Name"] for t in raw_tables]
    sample = ", ".join(all_names[:25]) if all_names else "(database is empty / not found)"
    raise Exception(
        f"No HUB_*/LINK_*/SAT_* tables found in Glue database '{DATABASE_NAME}'. "
        f"This database currently has {len(all_names)} table(s). First few: {sample}. "
        f"Check: (1) DATABASE_NAME is the correct Glue Catalog database for your RAW_VAULT layer "
        f"(it may be different from '{DATABASE_NAME}', e.g. 'orderlens_raw_vault'), "
        f"(2) your tables actually follow the HUB_/LINK_/SAT_ prefix naming convention."
    )


PREFIXES = {
    "HUB":  ["HUB_", "H_"],
    "LINK": ["LINK_", "L_"],
    "SAT":  ["SAT_", "S_"],
}


def strip_known_prefix(name, ttype):
    for p in PREFIXES.get(ttype, []):
        if name.startswith(p):
            return name[len(p):]
    return name


def own_hk_column(t):
    """The hash key that THIS table is keyed on, derived from naming convention."""
    if t["type"] not in ("HUB", "LINK", "SAT"):
        return None
    suffix = strip_known_prefix(t["name"], t["type"])
    candidate = f"{suffix}_HK"
    # fall back to whichever _HK column is present if convention guess is wrong
    if candidate in t["hk_columns"]:
        return candidate
    return t["hk_columns"][0] if t["hk_columns"] else None


# Map: hash-key column name -> parent (HUB or LINK) table dict, used to
# auto-wire orphan / referential-integrity checks for LINK foreign keys and
# SAT parent keys — exactly the convention used in the reference report.
PARENT_BY_HK = {}
for t in hub_tables + link_tables:
    hk = own_hk_column(t)
    if hk:
        PARENT_BY_HK[hk] = t

for t in catalog:
    t["own_hk"] = own_hk_column(t)

# ──────────────────────────────────────────────────────────────────────────────
# 2. CHECK CATALOGUE — build the list of checks for every table
# ──────────────────────────────────────────────────────────────────────────────
# Each check is a dict:
#   category : Technical | Freshness | Volume | Source
#   table    : table friendly name (upper)
#   check    : short check name (matches the reference report's wording)
#   impact   : business-impact sentence shown in the UI
#   sql      : the SQL actually executed (Spark SQL dialect)
#   kind     : 'count'  -> PASS if v == 0 else FAIL(v)
#              'bool'   -> PASS if v == 0 else FAIL(v)   (predicate, v in {0,1})
#              'info'   -> always INFO, displays v as-is

CATEGORY_COLORS = {
    "Technical": "#4f46e5",
    "Freshness": "#0891b2",
    "Volume":    "#7c3aed",
    "Source":    "#6b7280",
}
CATEGORY_ORDER = ["Technical", "Freshness", "Volume", "Source"]

IMPACT = {
    "null_hk_hub":        "NULL hash keys break all joins to satellites and links — entity disappears from dashboard.",
    "dup_hk_hub":         "Hash key collisions cause incorrect joins and can swap data between records.",
    "null_load_dts":      "Records without a load timestamp cannot be audited or version-tracked.",
    "future_dated":       "Future-dated loads suggest a timezone or ETL clock issue.",
    "null_record_source": "Without a source tag, data lineage is broken.",
    "row_count":          "Baseline.",
    "null_hub_key_link":  "A NULL foreign key in a link means a dangling relationship.",
    "orphan_link":        "Link records with no matching hub break downstream joins.",
    "dup_hk_combo":       "Duplicate link entries double-count relationships.",
    "null_parent_hk_sat": "NULL HK breaks all joins — satellite data becomes completely unreachable.",
    "dup_hk_load_dts":    "Identical snapshots waste storage and distort change-detection.",
    "orphan_sat":         "Satellite rows without a parent are invisible in all joins.",
    "null_hashdiff":      "NULL hashdiff disables change detection — every reload treated as new version.",
    "dup_hk_hashdiff":    "Duplicate hashdiff for same HK indicates a pipeline retry failure.",
    "stale_48h":          "ETL pipeline likely stopped. Dashboard KPIs from this table show stale data.",
    "future_skew":        "Future-dated load timestamp means ETL server clock is wrong — breaks time-based queries.",
    "oldest_age":         "Shows data history depth — useful for spotting truncation or historic gaps.",
    "zero_today":         "Table received rows yesterday but zero today — upstream feed likely failed silently.",
    "load_spike":         f"Load {SPIKE_MULTIPLIER}x larger than normal suggests a duplicate or runaway ETL retry.",
    "today_count":        "Records loaded today.",
    "single_source":      "One source feeding everything suggests a backup/fallback took over — may be partial/lower quality.",
    "distinct_sources":   "Shows all source systems feeding this table — for lineage audit.",
}


def fq(table_glue_name):
    return f"{DATABASE_NAME}.{table_glue_name}"


def build_checks_for_table(t):
    checks = []
    db_tbl = fq(t["glue_name"])
    name = t["name"]
    load_dts = t["load_dts_col"]
    rec_src = t["record_source_col"]
    hash_diff = t["hash_diff_col"]
    own_hk = t["own_hk"]

    # ---- TECHNICAL ----------------------------------------------------------
    if t["type"] == "HUB":
        if own_hk:
            checks.append(dict(category="Technical", table=name,
                check=f"NULL hash key ({own_hk})", impact=IMPACT["null_hk_hub"], kind="count",
                sql=f"SELECT COUNT(*) AS v FROM {db_tbl} WHERE {own_hk} IS NULL"))
            checks.append(dict(category="Technical", table=name,
                check=f"Duplicate hash key ({own_hk})", impact=IMPACT["dup_hk_hub"], kind="count",
                sql=f"SELECT COUNT(*) AS v FROM (SELECT {own_hk} FROM {db_tbl} GROUP BY {own_hk} HAVING COUNT(*) > 1) t"))

    elif t["type"] == "LINK":
        if own_hk:
            checks.append(dict(category="Technical", table=name,
                check=f"NULL hub key ({own_hk})", impact=IMPACT["null_hub_key_link"], kind="count",
                sql=f"SELECT COUNT(*) AS v FROM {db_tbl} WHERE {own_hk} IS NULL"))
        fk_cols = [c for c in t["hk_columns"] if c != own_hk]
        for fk in fk_cols:
            checks.append(dict(category="Technical", table=name,
                check=f"NULL hub key ({fk})", impact=IMPACT["null_hub_key_link"], kind="count",
                sql=f"SELECT COUNT(*) AS v FROM {db_tbl} WHERE {fk} IS NULL"))
            parent = PARENT_BY_HK.get(fk)
            if parent:
                p_tbl = fq(parent["glue_name"])
                checks.append(dict(category="Technical", table=name,
                    check=f"Orphan records — {fk} not in {parent['name']}",
                    impact=IMPACT["orphan_link"], kind="count",
                    sql=(f"SELECT COUNT(*) AS v FROM {db_tbl} l "
                         f"LEFT JOIN {p_tbl} p ON l.{fk} = p.{fk} "
                         f"WHERE p.{fk} IS NULL")))
        if t["hk_columns"]:
            combo = ",".join(t["hk_columns"])
            checks.append(dict(category="Technical", table=name,
                check="Duplicate HK combination", impact=IMPACT["dup_hk_combo"], kind="count",
                sql=f"SELECT COUNT(*) AS v FROM (SELECT {combo} FROM {db_tbl} GROUP BY {combo} HAVING COUNT(*) > 1) t"))

    elif t["type"] == "SAT":
        if own_hk:
            checks.append(dict(category="Technical", table=name,
                check=f"NULL parent hash key ({own_hk})", impact=IMPACT["null_parent_hk_sat"], kind="count",
                sql=f"SELECT COUNT(*) AS v FROM {db_tbl} WHERE {own_hk} IS NULL"))
            if load_dts:
                checks.append(dict(category="Technical", table=name,
                    check="Duplicate HK + LOAD_DTS", impact=IMPACT["dup_hk_load_dts"], kind="count",
                    sql=f"SELECT COUNT(*) AS v FROM (SELECT {own_hk},{load_dts} FROM {db_tbl} GROUP BY {own_hk},{load_dts} HAVING COUNT(*) > 1) t"))
            parent = PARENT_BY_HK.get(own_hk)
            if parent:
                p_tbl = fq(parent["glue_name"])
                checks.append(dict(category="Technical", table=name,
                    check=f"Orphan records — {own_hk} not in {parent['name']}",
                    impact=IMPACT["orphan_sat"], kind="count",
                    sql=(f"SELECT COUNT(*) AS v FROM {db_tbl} s "
                         f"LEFT JOIN {p_tbl} p ON s.{own_hk} = p.{own_hk} "
                         f"WHERE p.{own_hk} IS NULL")))
            if hash_diff:
                checks.append(dict(category="Technical", table=name,
                    check=f"NULL hashdiff ({hash_diff})", impact=IMPACT["null_hashdiff"], kind="count",
                    sql=f"SELECT COUNT(*) AS v FROM {db_tbl} WHERE {hash_diff} IS NULL"))
                checks.append(dict(category="Technical", table=name,
                    check="Duplicate HK + hashdiff", impact=IMPACT["dup_hk_hashdiff"], kind="count",
                    sql=f"SELECT COUNT(*) AS v FROM (SELECT {own_hk},{hash_diff} FROM {db_tbl} GROUP BY {own_hk},{hash_diff} HAVING COUNT(*) > 1) t"))

    # shared Technical checks for every Data Vault table type
    if load_dts:
        checks.append(dict(category="Technical", table=name,
            check="NULL load date", impact=IMPACT["null_load_dts"], kind="count",
            sql=f"SELECT COUNT(*) AS v FROM {db_tbl} WHERE {load_dts} IS NULL"))
        checks.append(dict(category="Technical", table=name,
            check="Future-dated records", impact=IMPACT["future_dated"], kind="count",
            sql=f"SELECT COUNT(*) AS v FROM {db_tbl} WHERE {load_dts} > CURRENT_TIMESTAMP()"))
    if rec_src:
        checks.append(dict(category="Technical", table=name,
            check="NULL record source", impact=IMPACT["null_record_source"], kind="count",
            sql=f"SELECT COUNT(*) AS v FROM {db_tbl} WHERE {rec_src} IS NULL"))
    checks.append(dict(category="Technical", table=name,
        check="Total row count", impact=IMPACT["row_count"], kind="info",
        sql=f"SELECT COUNT(*) AS v FROM {db_tbl}"))

    # ---- FRESHNESS (needs LOAD_DTS) -----------------------------------------
    if load_dts:
        checks.append(dict(category="Freshness", table=name,
            check=f"No new records in last {FRESHNESS_STALE_HOURS}h (stale pipeline)",
            impact=IMPACT["stale_48h"], kind="bool",
            sql=(f"SELECT CASE WHEN MAX({load_dts}) < "
                 f"(CURRENT_TIMESTAMP() - INTERVAL {FRESHNESS_STALE_HOURS} HOURS) "
                 f"THEN 1 ELSE 0 END AS v FROM {db_tbl}")))
        checks.append(dict(category="Freshness", table=name,
            check="Future LOAD_DTS detected (clock skew)", impact=IMPACT["future_skew"], kind="count",
            sql=f"SELECT COUNT(*) AS v FROM {db_tbl} WHERE {load_dts} > CURRENT_TIMESTAMP()"))
        checks.append(dict(category="Freshness", table=name,
            check="Oldest record age (days)", impact=IMPACT["oldest_age"], kind="info",
            sql=f"SELECT DATEDIFF(CURRENT_DATE(), MIN({load_dts})) AS v FROM {db_tbl}"))

        # ---- VOLUME (needs LOAD_DTS) -----------------------------------------
        checks.append(dict(category="Volume", table=name,
            check="Zero records loaded today (silent drop)", impact=IMPACT["zero_today"], kind="bool",
            sql=(f"SELECT CASE WHEN "
                 f"(SELECT COUNT(*) FROM {db_tbl} WHERE TO_DATE({load_dts}) = CURRENT_DATE()) = 0 "
                 f"AND (SELECT COUNT(*) FROM {db_tbl} WHERE TO_DATE({load_dts}) = DATE_SUB(CURRENT_DATE(),1)) > 0 "
                 f"THEN 1 ELSE 0 END AS v")))
        checks.append(dict(category="Volume", table=name,
            check=f"Load spike today vs 7-day avg (>{SPIKE_MULTIPLIER}x)", impact=IMPACT["load_spike"], kind="bool",
            sql=(f"WITH daily AS ("
                 f"  SELECT TO_DATE({load_dts}) AS d, COUNT(*) AS n FROM {db_tbl} "
                 f"  WHERE {load_dts} >= DATE_SUB(CURRENT_DATE(), 8) GROUP BY TO_DATE({load_dts})"
                 f"), avg7 AS ("
                 f"  SELECT AVG(n) AS avg_n FROM daily WHERE d < CURRENT_DATE()"
                 f"), today AS ("
                 f"  SELECT COALESCE(MAX(n),0) AS n FROM daily WHERE d = CURRENT_DATE()"
                 f") "
                 f"SELECT CASE WHEN today.n > {SPIKE_MULTIPLIER} * COALESCE(avg7.avg_n,0) AND COALESCE(avg7.avg_n,0) > 0 "
                 f"THEN 1 ELSE 0 END AS v FROM today CROSS JOIN avg7")))
        checks.append(dict(category="Volume", table=name,
            check="Today's load count", impact=IMPACT["today_count"], kind="info",
            sql=f"SELECT COUNT(*) AS v FROM {db_tbl} WHERE TO_DATE({load_dts}) = CURRENT_DATE()"))

    # ---- SOURCE (needs RECORD_SOURCE) ---------------------------------------
    if rec_src:
        checks.append(dict(category="Source", table=name,
            check="Single source feeding 100% of records", impact=IMPACT["single_source"], kind="bool",
            sql=(f"SELECT CASE WHEN MAX(pct) >= 99 THEN 1 ELSE 0 END AS v FROM ("
                 f"  SELECT {rec_src}, COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct "
                 f"  FROM {db_tbl} GROUP BY {rec_src}) t")))
        checks.append(dict(category="Source", table=name,
            check="Distinct record sources count", impact=IMPACT["distinct_sources"], kind="info",
            sql=f"SELECT COUNT(DISTINCT {rec_src}) AS v FROM {db_tbl}"))

    return checks


all_checks = []
for t in catalog:
    all_checks.extend(build_checks_for_table(t))

# ──────────────────────────────────────────────────────────────────────────────
# 3. EXECUTION — run every check via Spark SQL against the Glue Catalog
# ──────────────────────────────────────────────────────────────────────────────
def run_check(check):
    try:
        row = spark.sql(check["sql"]).collect()[0]
        v = row[0]
        v = 0 if v is None else v
    except Exception as e:
        check["status"] = "ERROR"
        check["value"] = None
        check["error"] = str(e)
        return check

    if check["kind"] == "info":
        check["status"] = "INFO"
        check["value"] = v
    else:  # 'count' or 'bool' -> PASS if v == 0 else FAIL
        if v and v != 0:
            check["status"] = "FAIL"
            check["value"] = v
        else:
            check["status"] = "PASS"
            check["value"] = 0
    check["error"] = None
    return check


for c in all_checks:
    run_check(c)

# ──────────────────────────────────────────────────────────────────────────────
# 4. ROW COUNTS PER TABLE (re-used in the Per-Table Summary cards)
# ──────────────────────────────────────────────────────────────────────────────
ROW_COUNT_BY_TABLE = {}
for c in all_checks:
    if c["check"] == "Total row count" and c["status"] == "INFO":
        ROW_COUNT_BY_TABLE[c["table"]] = c["value"]

# ──────────────────────────────────────────────────────────────────────────────
# 5. AGGREGATIONS
# ──────────────────────────────────────────────────────────────────────────────
def pct(pass_n, fail_n):
    denom = pass_n + fail_n
    return round(pass_n * 100.0 / denom) if denom else 100

TOTAL_CHECKS = len(all_checks)
PASS_N  = sum(1 for c in all_checks if c["status"] == "PASS")
FAIL_N  = sum(1 for c in all_checks if c["status"] == "FAIL")
ERROR_N = sum(1 for c in all_checks if c["status"] == "ERROR")
WARN_N  = 0  # reserved for future WARNING-severity checks
INFO_N  = sum(1 for c in all_checks if c["status"] == "INFO")
DQ_SCORE = pct(PASS_N, FAIL_N)

categories_present = [cat for cat in CATEGORY_ORDER if any(c["category"] == cat for c in all_checks)]

category_summary = []
for cat in categories_present:
    rows = [c for c in all_checks if c["category"] == cat]
    p = sum(1 for c in rows if c["status"] == "PASS")
    f = sum(1 for c in rows if c["status"] == "FAIL")
    e = sum(1 for c in rows if c["status"] == "ERROR")
    category_summary.append({
        "category": cat, "checks": len(rows), "pass": p, "fail": f, "error": e,
        "score": pct(p, f),
    })

table_summary = []
for t in catalog:
    rows = [c for c in all_checks if c["table"] == t["name"]]
    p = sum(1 for c in rows if c["status"] == "PASS")
    f = sum(1 for c in rows if c["status"] == "FAIL")
    e = sum(1 for c in rows if c["status"] == "ERROR")
    i = sum(1 for c in rows if c["status"] == "INFO")
    table_summary.append({
        "table": t["name"], "rows": ROW_COUNT_BY_TABLE.get(t["name"], 0),
        "pass": p, "fail": f, "error": e, "info": i, "has_fail": f > 0 or e > 0,
    })
table_summary.sort(key=lambda x: x["table"])

failures = [c for c in all_checks if c["status"] in ("FAIL", "ERROR")]
failures.sort(key=lambda c: (CATEGORY_ORDER.index(c["category"]) if c["category"] in CATEGORY_ORDER else 99, c["table"]))

results_grouped = {cat: [c for c in all_checks if c["category"] == cat] for cat in categories_present}
for cat in results_grouped:
    results_grouped[cat].sort(key=lambda c: c["table"])

# ──────────────────────────────────────────────────────────────────────────────
# 6. HTML RENDERING — same visual format as dv_validation_report_*.html
# ──────────────────────────────────────────────────────────────────────────────
def esc(s):
    return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace('"', "&quot;"))


def fmt_num(n):
    try:
        return f"{int(n):,}"
    except (ValueError, TypeError):
        return str(n)


def status_badge(c):
    if c["status"] == "PASS":
        return ('<span style="background:#dcfce7;color:#15803d;border-radius:20px;'
                'padding:3px 11px;font-size:11px;font-weight:700;">&#10003; PASS</span>')
    if c["status"] == "FAIL":
        return ('<span style="background:#fee2e2;color:#dc2626;border-radius:20px;'
                f'padding:3px 11px;font-size:11px;font-weight:700;">&#10007; FAIL ({fmt_num(c["value"])})</span>')
    if c["status"] == "ERROR":
        return ('<span style="background:#fffbeb;color:#d97706;border-radius:20px;'
                'padding:3px 11px;font-size:11px;font-weight:700;">&#9888; ERROR</span>')
    return ('<span style="background:#eff6ff;color:#1d4ed8;border-radius:20px;'
            f'padding:3px 11px;font-size:11px;font-weight:700;">&#8505; {fmt_num(c["value"])}</span>')


def category_chip(cat):
    color = CATEGORY_COLORS.get(cat, "#6b7280")
    return (f'<span style="background:{color}22;color:{color};border-radius:6px;'
            f'padding:3px 10px;font-size:12px;font-weight:700;">{esc(cat)}</span>')


def category_chip_small(cat):
    color = CATEGORY_COLORS.get(cat, "#6b7280")
    return (f'<span style="background:{color}22;color:{color};border-radius:6px;padding:2px 8px;'
            f'font-size:10px;font-weight:700;">{esc(cat)}</span>')


def bar_color(score):
    if score >= 90: return "#16a34a"
    if score >= 70: return "#d97706"
    return "#dc2626"


CSS = """
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,-apple-system,sans-serif;background:#f8fafc;color:#111827}
.page{max-width:1440px;margin:0 auto;padding:32px 24px}
table{width:100%;border-collapse:collapse}
thead th{background:#1e293b;color:#e2e8f0;padding:11px 14px;font-size:11px;font-weight:700;
          text-transform:uppercase;letter-spacing:0.5px;text-align:left}
tbody tr:hover td{background:#f0f9ff!important}
"""

# ---- header / hero -----------------------------------------------------------
mart_suffix = f" &amp; {esc(MART_DATABASE_NAME)}" if MART_DATABASE_NAME else ""
html_parts = []
html_parts.append(f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>{esc(REPORT_TITLE)} — DQ Validation Report</title>
<style>{CSS}</style></head><body>
<div class="page">

<div style="background:linear-gradient(135deg,#0f172a 0%,#1e293b 50%,#0f4c81 100%);
            border-radius:18px;padding:30px 36px;margin-bottom:24px;color:white;">
  <div style="display:flex;align-items:center;gap:16px;">
    <div style="width:52px;height:52px;border-radius:14px;flex-shrink:0;
                background:linear-gradient(135deg,#4f46e5,#7c3aed);
                display:flex;align-items:center;justify-content:center;font-size:26px;">&#128737;</div>
    <div>
      <div style="font-size:22px;font-weight:900;">{esc(REPORT_TITLE)} — Data Vault Validation Report</div>
      <div style="font-size:13px;opacity:0.65;margin-top:3px;">
        {esc(SCHEMA_LABEL)}{mart_suffix} &nbsp;&middot;&nbsp;
        {RUN_TS.strftime('%Y-%m-%d %H:%M:%S')} UTC &nbsp;&middot;&nbsp;
        {TOTAL_CHECKS} checks &middot; {len(categories_present)} categories
      </div>
    </div>
  </div>
</div>
""")

# ---- 6 KPI tiles --------------------------------------------------------------
html_parts.append(f"""
<div style="display:grid;grid-template-columns:repeat(6,1fr);gap:14px;margin-bottom:24px;">
  <div style="background:white;border-radius:12px;padding:18px;text-align:center;border-top:3px solid {bar_color(DQ_SCORE)};box-shadow:0 1px 4px rgba(0,0,0,0.07);">
    <div style="font-size:34px;font-weight:900;color:{bar_color(DQ_SCORE)};">{DQ_SCORE}%</div>
    <div style="font-size:11px;color:#6b7280;font-weight:600;margin-top:3px;">DQ Score</div>
  </div>
  <div style="background:#f0fdf4;border:1.5px solid #86efac;border-radius:12px;padding:18px;text-align:center;">
    <div style="font-size:34px;font-weight:900;color:#16a34a;">{PASS_N}</div>
    <div style="font-size:11px;color:#15803d;font-weight:600;margin-top:3px;">PASSED</div>
  </div>
  <div style="background:#fff5f5;border:1.5px solid #fca5a5;border-radius:12px;padding:18px;text-align:center;">
    <div style="font-size:34px;font-weight:900;color:#dc2626;">{FAIL_N}</div>
    <div style="font-size:11px;color:#dc2626;font-weight:600;margin-top:3px;">FAILED</div>
  </div>
  <div style="background:#fffbeb;border:1.5px solid #fde68a;border-radius:12px;padding:18px;text-align:center;">
    <div style="font-size:34px;font-weight:900;color:#d97706;">{ERROR_N}</div>
    <div style="font-size:11px;color:#d97706;font-weight:600;margin-top:3px;">ERRORS</div>
  </div>
  <div style="background:#fff7ed;border:1.5px solid #fed7aa;border-radius:12px;padding:18px;text-align:center;">
    <div style="font-size:34px;font-weight:900;color:#c2410c;">{WARN_N}</div>
    <div style="font-size:11px;color:#c2410c;font-weight:600;margin-top:3px;">WARNINGS</div>
  </div>
  <div style="background:#eff6ff;border:1.5px solid #bfdbfe;border-radius:12px;padding:18px;text-align:center;">
    <div style="font-size:34px;font-weight:900;color:#1d4ed8;">{INFO_N}</div>
    <div style="font-size:11px;color:#1d4ed8;font-weight:600;margin-top:3px;">INFO</div>
  </div>
</div>
""")

# ---- Results by Category table ------------------------------------------------
cat_rows_html = ""
for cs in category_summary:
    color = bar_color(cs["score"])
    cat_rows_html += f"""
        <tr>
          <td style="padding:10px 16px;">{category_chip(cs['category'])}</td>
          <td style="padding:10px 16px;font-size:13px;font-weight:600;">{cs['checks']}</td>
          <td style="padding:10px 16px;">
            <span style="background:#dcfce7;color:#15803d;border-radius:20px;padding:2px 9px;font-size:11px;font-weight:700;">{cs['pass']} PASS</span>
            {f'<span style="background:#fee2e2;color:#dc2626;border-radius:20px;padding:2px 9px;font-size:11px;font-weight:700;margin-left:4px;">{cs["fail"]} FAIL</span>' if cs['fail'] else ''}
            {f'<span style="background:#fffbeb;color:#d97706;border-radius:20px;padding:2px 9px;font-size:11px;font-weight:700;margin-left:4px;">{cs["error"]} ERROR</span>' if cs['error'] else ''}
          </td>
          <td style="padding:10px 16px;">
            <div style="background:#e5e7eb;border-radius:4px;height:7px;width:110px;">
              <div style="width:{cs['score']}%;background:{color};border-radius:4px;height:7px;"></div>
            </div>
            <div style="font-size:11px;color:#6b7280;margin-top:3px;">{cs['score']}%</div>
          </td>
        </tr>"""

html_parts.append(f"""
<div style="background:white;border-radius:12px;padding:20px;box-shadow:0 1px 4px rgba(0,0,0,0.07);margin-bottom:24px;">
  <div style="font-size:15px;font-weight:700;margin-bottom:16px;">Results by Category</div>
  <table><thead><tr><th>Category</th><th>Checks</th><th>Results</th><th>Score</th></tr></thead>
  <tbody>{cat_rows_html}</tbody></table>
</div>
""")

# ---- Per-Table Summary chips ---------------------------------------------------
chips_html = ""
for ts in table_summary:
    border = "#fca5a5" if ts["has_fail"] else "#bbf7d0"
    bg = "#fff5f5" if ts["has_fail"] else "#f0fdf4"
    dot = "#dc2626" if ts["has_fail"] else "#16a34a"
    badges = f'<span style="background:#dcfce7;color:#15803d;border-radius:20px;padding:2px 7px;font-size:10px;font-weight:700;">{ts["pass"]} PASS</span>'
    if ts["fail"]:
        badges += f'<span style="background:#fee2e2;color:#dc2626;border-radius:20px;padding:2px 7px;font-size:10px;font-weight:700;">{ts["fail"]} FAIL</span>'
    if ts["error"]:
        badges += f'<span style="background:#fffbeb;color:#d97706;border-radius:20px;padding:2px 7px;font-size:10px;font-weight:700;">{ts["error"]} ERROR</span>'
    if ts["info"]:
        badges += f'<span style="background:#eff6ff;color:#1d4ed8;border-radius:20px;padding:2px 7px;font-size:10px;font-weight:700;">{ts["info"]} INFO</span>'
    chips_html += f"""
        <div style="background:{bg};border:1.5px solid {border};border-radius:12px;
                    padding:14px;min-width:185px;flex:1 1 185px;">
          <div style="display:flex;align-items:center;gap:6px;margin-bottom:6px;">
            <div style="width:8px;height:8px;border-radius:50%;background:{dot};flex-shrink:0;"></div>
            <div style="font-size:12px;font-weight:700;color:#111;overflow:hidden;
                        text-overflow:ellipsis;white-space:nowrap;">{esc(ts['table'])}</div>
          </div>
          <div style="font-size:11px;color:#6b7280;margin-bottom:6px;">
            Rows: <b>{fmt_num(ts['rows'])}</b>
          </div>
          <div style="display:flex;gap:4px;flex-wrap:wrap;">{badges}</div>
        </div>"""

html_parts.append(f"""
<div style="background:white;border-radius:12px;padding:20px;box-shadow:0 1px 4px rgba(0,0,0,0.07);margin-bottom:24px;">
  <div style="font-size:15px;font-weight:700;margin-bottom:14px;">Per-Table Summary</div>
  <div style="display:flex;flex-wrap:wrap;gap:10px;">{chips_html}</div>
</div>
""")

# ---- Issues That Need Fixing ----------------------------------------------------
issues_html = ""
for c in failures:
    color = CATEGORY_COLORS.get(c["category"], "#6b7280")
    sql_preview = esc(c["sql"])
    issues_html += f"""
            <div style="background:#fff5f5;border:1px solid #fca5a5;border-left:4px solid {color};
                        border-radius:0 8px 8px 0;padding:14px 16px;margin-bottom:8px;">
              <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;">
                {category_chip_small(c['category'])}
                <span style="font-size:13px;font-weight:700;color:#dc2626;">{esc(c['table'])}</span>
              </div>
              <div style="font-size:13px;font-weight:600;color:#374151;margin-bottom:3px;">{esc(c['check'])}</div>
              <div style="font-size:12px;color:#dc2626;font-weight:600;margin-bottom:4px;">{fmt_num(c['value']) if c['status']=='FAIL' else 'query'} issue(s) found</div>
              <div style="font-size:11px;color:#6b7280;margin-bottom:4px;font-style:italic;">{esc(c['impact'])}</div>
              <div style="font-size:10px;color:#9ca3af;font-family:monospace;">{sql_preview}</div>
            </div>"""

html_parts.append(f"""
<div style="background:white;border-radius:12px;padding:20px;box-shadow:0 1px 4px rgba(0,0,0,0.07);margin-bottom:24px;">
  <div style="font-size:15px;font-weight:700;color:#dc2626;margin-bottom:14px;">Issues That Need Fixing — {len(failures)} failures</div>
  {issues_html if failures else '<div style="font-size:13px;color:#16a34a;font-weight:700;">&#10003; No failures found — all checks passed.</div>'}
</div>
""")

# ---- All Check Results (grouped by category) ------------------------------------
all_rows_html = ""
for cat in categories_present:
    color = CATEGORY_COLORS.get(cat, "#6b7280")
    all_rows_html += (f'<tr><td colspan="5" style="background:{color};color:white;font-weight:700;'
                       f'font-size:12px;padding:8px 16px;letter-spacing:0.5px;">&#9632; {cat.upper()}</td></tr>')
    for c in results_grouped[cat]:
        title = esc(c["sql"])
        preview = esc(c["sql"][:65] + ("…" if len(c["sql"]) > 65 else ""))
        all_rows_html += f"""
        <tr style="background:#fff;border-bottom:1px solid #f1f5f9;">
          <td style="padding:9px 13px;font-size:11px;font-weight:600;color:#374151;">{esc(c['table'])}</td>
          <td style="padding:9px 13px;font-size:11px;color:#374151;">{esc(c['check'])}</td>
          <td style="padding:9px 13px;font-size:10px;color:#9ca3af;font-style:italic;max-width:220px;">{esc(c['impact'])}</td>
          <td style="padding:9px 13px;font-size:10px;color:#9ca3af;font-family:monospace;max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="{title}">{preview}</td>
          <td style="padding:9px 13px;">{status_badge(c)}</td>
        </tr>"""

html_parts.append(f"""
<div style="background:white;border-radius:12px;box-shadow:0 1px 4px rgba(0,0,0,0.07);overflow:hidden;margin-bottom:24px;">
  <div style="padding:20px 20px 12px;font-size:15px;font-weight:700;">All Check Results ({TOTAL_CHECKS})</div>
  <table><thead><tr><th>Table / View</th><th>Check</th><th>Business Impact</th><th>Query Preview</th><th>Result</th></tr></thead>
  <tbody>{all_rows_html}</tbody></table>
</div>
""")

html_parts.append("""
</div>
</body></html>
""")

report_html = "".join(html_parts)

# ──────────────────────────────────────────────────────────────────────────────
# 7. WRITE OUTPUT TO S3
# ──────────────────────────────────────────────────────────────────────────────
def split_s3_path(s3_path):
    assert s3_path.startswith("s3://"), "OUTPUT_S3_PATH must start with s3://"
    rest = s3_path[len("s3://"):]
    bucket, _, prefix = rest.partition("/")
    return bucket, prefix


bucket, prefix = split_s3_path(OUTPUT_S3_PATH)
db_slug = re.sub(r"[^a-zA-Z0-9_]+", "_", DATABASE_NAME.lower())
html_key = f"{prefix}/dv_validation_report_{db_slug}.html".lstrip("/")
json_key = f"{prefix}/dv_validation_report_{db_slug}.json".lstrip("/")

s3_client.put_object(Bucket=bucket, Key=html_key, Body=report_html.encode("utf-8"),
                      ContentType="text/html")

summary_json = {
    "report_title": REPORT_TITLE,
    "schema_label": SCHEMA_LABEL,
    "run_ts_utc": RUN_TS.isoformat(),
    "total_checks": TOTAL_CHECKS,
    "pass": PASS_N, "fail": FAIL_N, "error": ERROR_N, "warning": WARN_N, "info": INFO_N,
    "dq_score": DQ_SCORE,
    "categories": category_summary,
    "tables": table_summary,
    "failures": [
        {"category": c["category"], "table": c["table"], "check": c["check"],
         "value": c["value"], "sql": c["sql"]}
        for c in failures
    ],
}
s3_client.put_object(Bucket=bucket, Key=json_key,
                      Body=json.dumps(summary_json, indent=2, default=str).encode("utf-8"),
                      ContentType="application/json")

print(f"DQ report written to s3://{bucket}/{html_key}")
print(f"DQ summary written to s3://{bucket}/{json_key}")
print(f"DQ Score: {DQ_SCORE}%  |  PASS={PASS_N}  FAIL={FAIL_N}  ERROR={ERROR_N}  INFO={INFO_N}  "
      f"out of {TOTAL_CHECKS} checks across {len(catalog)} tables "
      f"({len(hub_tables)} HUB / {len(link_tables)} LINK / {len(sat_tables)} SAT)")

job.commit()
