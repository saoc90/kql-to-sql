import sys, json, re, collections, glob, os

mode = sys.argv[1] if len(sys.argv) > 1 else "sqlerr"
vdir = sys.argv[2] if len(sys.argv) > 2 else "verdicts"
files = glob.glob(os.path.join(os.path.dirname(__file__), vdir, "*.jsonl"))

verdicts = []
for f in files:
    if "smoke" in f:
        continue
    for line in open(f, encoding="utf-8"):
        line = line.strip()
        if not line:
            continue
        try:
            v = json.loads(line)
        except Exception:
            continue
        if v.get("Family") == "probe":
            continue
        verdicts.append(v)

def sig_sqlerr(v):
    err = (v.get("Duck") or {}).get("Error") or v.get("Detail") or ""
    m = re.search(r"No function matches the given name and argument types '([^']+)'", err)
    if m:
        return "NoFunction: " + m.group(1)
    if "syntax error at or near" in err:
        mm = re.search(r'near "([^"]+)"', err)
        return "Syntax near: " + (mm.group(1) if mm else err[:40])
    if "Binder Error" in err:
        return "Binder: " + err.split("Binder Error:")[1][:70].strip()
    if "Conversion Error" in err:
        return "Conversion: " + err.split("Conversion Error:")[1][:70].strip()
    if "Parser Error" in err:
        return "Parser: " + err.split("Parser Error:")[1][:70].strip()
    if "Catalog Error" in err:
        return "Catalog: " + err.split("Catalog Error:")[1][:70].strip()
    return err[:80]

# Extract KQL functions/operators used, to cluster MismatchRows by feature.
FUNC_RE = re.compile(r"([a-z_][a-z0-9_]*)\s*\(")
OPS = ["has_cs","!has","hasprefix","hassuffix","has_any","has_all","has ","contains","startswith",
       "endswith","matches regex"," join "," lookup "," union "," mv-expand"," mv-apply"," summarize",
       " make-series"," parse-kv"," parse ","parse-where"," search "," scan "," serialize"," sort ",
       " top","getschema"," distinct"," range "]

def sig_mismatch(v):
    kql = v.get("Kql","")
    low = " " + kql.lower() + " "
    feats = [o.strip() for o in OPS if o in low]
    # also pull scalar functions
    fns = set(FUNC_RE.findall(kql.lower()))
    interesting = fns & set([
        "make_bag","make_set","make_list","make_list_with_nulls","substring","split","trim","trim_start",
        "trim_end","countof","indexof","extract","extract_all","replace_string","replace_regex","format_timespan",
        "format_datetime","startofweek","endofweek","bin","gettype","toint","tolong","todouble","todecimal",
        "totimespan","todatetime","tostring","parse_json","todynamic","array_length","array_concat","array_slice",
        "bag_keys","bag_merge","dynamic_to_json","strcat","strlen","string_size","isnotempty","isempty","percentile",
        "dcount","arg_max","arg_min","datetime_add","datetime_diff","strcmp","reverse","dayofweek","row_cumsum"])
    key = ",".join(sorted(set(feats)) + sorted(interesting))
    return key or "(other)"

clusters = collections.Counter()
examples = collections.defaultdict(list)
target = "SqlExecError" if mode == "sqlerr" else ("MismatchRows" if mode=="mismatch" else mode)
for v in verdicts:
    if v.get("Outcome") != target:
        continue
    sig = sig_sqlerr(v) if mode == "sqlerr" else sig_mismatch(v)
    clusters[sig] += 1
    if len(examples[sig]) < 2:
        examples[sig].append(v.get("Kql","")[:100])

print(f"=== {target}: {sum(clusters.values())} total, {len(clusters)} clusters ===")
for sig, n in clusters.most_common(45):
    print(f"{n:4}  {sig}")
    for ex in examples[sig]:
        print(f"      e.g. {ex}")
