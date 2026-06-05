import sys, json, glob, os, collections, re

vdir = sys.argv[1] if len(sys.argv) > 1 else "verdicts3"
files = [f for f in glob.glob(os.path.join(os.path.dirname(__file__), vdir, "*.jsonl")) if "smoke" not in f]
V = []
for f in files:
    for line in open(f, encoding="utf-8"):
        line = line.strip()
        if line:
            try: V.append(json.loads(line))
            except: pass

# Must-fix: translator threw, or emitted invalid SQL (translation robustness).
translate_err = [v for v in V if v.get("Outcome") == "TranslateError"]
sql_err = [v for v in V if v.get("Outcome") == "SqlExecError"]

print(f"=== TRANSLATE ERRORS (translator threw on Kusto-valid KQL): {len(translate_err)} ===")
seen = set()
for v in translate_err:
    d = (v.get("Duck") or {}).get("Error") or v.get("Detail") or ""
    key = d[:70]
    if key in seen: continue
    seen.add(key)
    print(f"  • {d[:90]}")
    print(f"      e.g. {v.get('Kql','')[:95]}")

print(f"\n=== SQL EXEC ERRORS (invalid generated SQL): {len(sql_err)} — distinct error heads ===")
c = collections.Counter()
ex = {}
for v in sql_err:
    d = (v.get("Duck") or {}).get("Error") or v.get("Detail") or ""
    m = re.search(r"(No function matches[^.]*?'[^']+'|syntax error at or near \"[^\"]+\"|[A-Z][a-z]+ Error: [^.\n]{0,55})", d)
    head = m.group(1) if m else d[:55]
    c[head] += 1
    ex.setdefault(head, v.get("Kql","")[:90])
for head, n in c.most_common(25):
    print(f"  {n:3}  {head}")
    print(f"        e.g. {ex[head]}")
