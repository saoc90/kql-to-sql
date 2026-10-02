//! `kql-oracle`: differential harness that replays the stored Kusto oracle corpus (verdict JSONL
//! produced by the C# fuzzer, `tests/KqlToSql.Fuzzer`) through the Rust translator and DuckDB.
//!
//! ```text
//! kql-oracle run [--in <file-or-dir>]... [--filter <substr>] [--verbose] [--failures-only]
//!                [--compare-csharp] [--record-sql] [--out <file.jsonl>]
//! kql-oracle one --kql "<KQL>"
//! kql-oracle one --id <Id | source/Id>
//! ```

mod analyzer;
mod compare;
mod duck;
mod rows;
mod value;

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use serde::{Deserialize, Serialize};

use compare::{DuckResult, KustoSample, Options, Outcome, Verdict};
use value::{format_row, Class, ColumnInfo};

/// One line of the C# verdict JSONL (only the fields we use).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Record {
    id: String,
    #[serde(default)]
    family: String,
    kql: String,
    /// The C# translator's SQL (for reference).
    #[serde(default)]
    sql: Option<String>,
    /// The C# verdict.
    outcome: String,
    #[serde(default)]
    kusto: Option<ResultSummary>,
    /// Set by us: the file stem the record came from (Ids repeat across files).
    #[serde(skip)]
    source: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ResultSummary {
    #[serde(default)]
    columns: Vec<String>,
    #[serde(default)]
    row_count: usize,
    #[serde(default)]
    sample_rows: Vec<String>,
    #[serde(default)]
    error: Option<String>,
}

impl Record {
    fn key(&self) -> String {
        format!("{}/{}", self.source, self.id)
    }

    fn kusto_rejected(&self) -> bool {
        self.outcome == "KustoError" || self.kusto.as_ref().is_some_and(|k| k.error.is_some())
    }
}

/// Our verdict for one record, as written to `--out`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct OutRecord<'a> {
    id: &'a str,
    source: &'a str,
    family: &'a str,
    kql: &'a str,
    sql: Option<&'a str>,
    outcome: &'static str,
    detail: Option<&'a str>,
    sub_verdicts: &'a [String],
    c_sharp_outcome: &'a str,
}

/// Everything we learned running one record.
struct RunResult {
    sql: Option<String>,
    verdict: Verdict,
    duck: Option<DuckResult>,
}

// ---- running ---------------------------------------------------------------------------------

/// Translates `kql`, catching translator panics.
fn translate(kql: &str) -> Result<kql_to_sql::Translation, String> {
    let catalog = kql_to_sql::Catalog::default();
    match panic::catch_unwind(AssertUnwindSafe(|| kql_to_sql::translate(kql, &catalog, kql_to_sql::Dialect::DuckDb))) {
        Ok(Ok(t)) => Ok(t),
        Ok(Err(e)) => Err(e.message),
        Err(payload) => Err(format!("panic: {}", panic_message(&payload))),
    }
}

/// Executes, catching panics from value conversion.
fn execute(sql: &str, declared: &[Class]) -> Result<DuckResult, String> {
    panic::catch_unwind(AssertUnwindSafe(|| duck::execute(sql, declared)))
        .unwrap_or_else(|p| Err(format!("panic during execution: {}", panic_message(&p))))
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".into())
}

fn kusto_sample(summary: &ResultSummary) -> Result<KustoSample, String> {
    let columns: Vec<ColumnInfo> = summary.columns.iter().map(|c| ColumnInfo::parse(c)).collect();
    let classes: Vec<Class> = columns.iter().map(|c| c.class).collect();
    let rows = summary
        .sample_rows
        .iter()
        .map(|r| rows::parse_row(r, &classes).map_err(|e| e.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(KustoSample { columns, row_count: summary.row_count, rows })
}

/// Mirrors the C# `Comparator.Compare` decision order.
fn run_record(rec: &Record, use_record_sql: bool) -> RunResult {
    let analysis = analyzer::analyze(&rec.kql);
    let rejected = rec.kusto_rejected();
    let done = |sql, outcome, detail: Option<String>| RunResult { sql, verdict: Verdict::new(outcome, detail), duck: None };

    if rec.outcome == "SkippedNondeterministic" && !rejected {
        return done(None, Outcome::SkippedNondeterministic, None);
    }

    let translated = if use_record_sql {
        // Harness self-check: replay the C# translator's SQL (no declared schema).
        rec.sql.clone().map(|sql| kql_to_sql::Translation { sql, columns: Vec::new() }).ok_or_else(|| "record has no Sql".to_string())
    } else {
        translate(&rec.kql)
    };
    let translation = match translated {
        Ok(t) => t,
        Err(e) if rejected => return done(None, Outcome::KustoRejectedOurError, Some(e)),
        Err(e) => return done(None, Outcome::TranslateError, Some(e)),
    };
    let sql = Some(translation.sql.clone());
    let declared: Vec<Class> = translation.columns.iter().map(|c| Class::from_kql(c.ty)).collect();

    let duck = match execute(&translation.sql, &declared) {
        Ok(d) => d,
        Err(e) if rejected => return done(sql, Outcome::KustoRejectedOurError, Some(e)),
        Err(e) if compare::is_engine_domain_error(&e) => return done(sql, Outcome::SkippedEngineError, Some(e)),
        Err(e) => return done(sql, Outcome::SqlExecError, Some(e)),
    };
    let with_duck = |outcome, detail: Option<String>, duck| RunResult { sql: sql.clone(), verdict: Verdict::new(outcome, detail), duck };

    if rejected {
        let err = rec.kusto.as_ref().and_then(|k| k.error.clone());
        return with_duck(Outcome::KustoRejectedWeAccepted, err.map(|e| first_line(&e)), Some(duck));
    }
    if analysis.nondeterministic {
        return with_duck(Outcome::SkippedNondeterministic, None, Some(duck));
    }
    let kusto = match rec.kusto.as_ref().ok_or("record has no Kusto result".to_string()).and_then(kusto_sample) {
        Ok(k) => k,
        Err(e) => return with_duck(Outcome::OracleUnreadable, Some(e), Some(duck)),
    };

    let mut verdict = compare::compare(&analysis, &kusto, &duck, Options::default());
    let declared_names: Vec<&str> = translation.columns.iter().map(|c| c.name.as_str()).collect();
    let actual_names: Vec<&str> = duck.columns.iter().map(|c| c.name.as_str()).collect();
    if !use_record_sql && declared_names != actual_names {
        verdict.sub_verdicts.push(format!("DECLARED_SCHEMA_MISMATCH[declared={declared_names:?} actual={actual_names:?}]"));
    }
    RunResult { sql, verdict, duck: Some(duck) }
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").to_string()
}

// ---- loading ---------------------------------------------------------------------------------

fn default_corpus() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../fuzzing/verdicts3")
}

fn load(inputs: &[PathBuf]) -> Result<Vec<Record>, String> {
    let mut files = Vec::new();
    for input in inputs {
        if input.is_dir() {
            let mut entries: Vec<PathBuf> = fs::read_dir(input)
                .map_err(|e| format!("{}: {e}", input.display()))?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.extension().is_some_and(|x| x == "jsonl"))
                .collect();
            entries.sort();
            files.extend(entries);
        } else {
            files.push(input.clone());
        }
    }
    let mut records = Vec::new();
    for file in files {
        let source = file.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
        let reader = BufReader::new(fs::File::open(&file).map_err(|e| format!("{}: {e}", file.display()))?);
        for (n, line) in reader.lines().enumerate() {
            let line = line.map_err(|e| format!("{}: {e}", file.display()))?;
            if line.trim().is_empty() {
                continue;
            }
            let mut rec: Record =
                serde_json::from_str(&line).map_err(|e| format!("{}:{}: {e}", file.display(), n + 1))?;
            rec.source = source.clone();
            records.push(rec);
        }
    }
    Ok(records)
}

// ---- CLI -------------------------------------------------------------------------------------

#[derive(Default)]
struct RunArgs {
    inputs: Vec<PathBuf>,
    filter: Option<String>,
    verbose: bool,
    failures_only: bool,
    compare_csharp: bool,
    record_sql: bool,
    out: Option<PathBuf>,
}

const USAGE: &str = "usage:
  kql-oracle run [--in <file-or-dir>]... [--filter <substr of Id/Family/source>] [--verbose]
                 [--failures-only] [--compare-csharp] [--record-sql] [--out <file.jsonl>]
  kql-oracle one --kql \"<KQL>\"
  kql-oracle one --id <Id | source/Id> [--in <file-or-dir>]...

  --verbose        print details (KQL, our SQL, error/diff) for every non-Match
  --failures-only  print details only for regressions (C# Match, ours not)
  --compare-csharp cross-tabulate our outcome against the recorded C# outcome
  --record-sql     harness self-check: execute the C# translator's recorded SQL instead of
                   translating (validates parser/comparator against the C# verdicts)
  default corpus: fuzzing/verdicts3";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("run") => parse_run_args(&args[1..]).and_then(cmd_run),
        Some("one") => cmd_one(&args[1..]),
        _ => Err(USAGE.to_string()),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}

fn parse_run_args(args: &[String]) -> Result<RunArgs, String> {
    let mut out = RunArgs::default();
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let mut value = || it.next().cloned().ok_or_else(|| format!("{a} needs a value\n{USAGE}"));
        match a.as_str() {
            "--in" => out.inputs.push(PathBuf::from(value()?)),
            "--filter" => out.filter = Some(value()?),
            "--out" => out.out = Some(PathBuf::from(value()?)),
            "--verbose" | "-v" => out.verbose = true,
            "--failures-only" => out.failures_only = true,
            "--compare-csharp" => out.compare_csharp = true,
            "--record-sql" => out.record_sql = true,
            other => return Err(format!("unknown argument {other}\n{USAGE}")),
        }
    }
    if out.inputs.is_empty() {
        out.inputs.push(default_corpus());
    }
    Ok(out)
}

fn matches_filter(rec: &Record, filter: &Option<String>) -> bool {
    filter.as_ref().is_none_or(|f| rec.key().contains(f.as_str()) || rec.family.contains(f.as_str()))
}

fn cmd_run(args: RunArgs) -> Result<(), String> {
    let records: Vec<Record> = load(&args.inputs)?.into_iter().filter(|r| matches_filter(r, &args.filter)).collect();
    if records.is_empty() {
        return Err("no records matched".into());
    }

    let mut writer = match &args.out {
        Some(p) => Some(std::io::BufWriter::new(fs::File::create(p).map_err(|e| format!("{}: {e}", p.display()))?)),
        None => None,
    };

    // Silence the default panic printout; panics are reported as verdicts.
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));

    let mut totals: BTreeMap<Outcome, usize> = BTreeMap::new();
    let mut by_family: BTreeMap<String, BTreeMap<Outcome, usize>> = BTreeMap::new();
    let mut cross: BTreeMap<(String, Outcome), usize> = BTreeMap::new();
    let (mut regressions, mut improvements, mut csharp_matches, mut kept) = (0, 0, 0, 0);

    for rec in &records {
        let res = run_record(rec, args.record_sql);
        let outcome = res.verdict.outcome;
        *totals.entry(outcome).or_default() += 1;
        *by_family.entry(rec.family.clone()).or_default().entry(outcome).or_default() += 1;
        *cross.entry((rec.outcome.clone(), outcome)).or_default() += 1;

        let csharp_match = rec.outcome == "Match";
        let regression = csharp_match && outcome != Outcome::Match;
        csharp_matches += usize::from(csharp_match);
        kept += usize::from(csharp_match && outcome == Outcome::Match);
        regressions += usize::from(regression);
        improvements += usize::from(!csharp_match && outcome == Outcome::Match);

        let show = if args.failures_only { regression } else { args.verbose && outcome != Outcome::Match };
        if show {
            print_detail(rec, &res);
        }
        if let Some(w) = writer.as_mut() {
            let line = OutRecord {
                id: &rec.id,
                source: &rec.source,
                family: &rec.family,
                kql: &rec.kql,
                sql: res.sql.as_deref(),
                outcome: outcome.name(),
                detail: res.verdict.detail.as_deref(),
                sub_verdicts: &res.verdict.sub_verdicts,
                c_sharp_outcome: &rec.outcome,
            };
            let json = serde_json::to_string(&line).map_err(|e| e.to_string())?;
            writeln!(w, "{json}").map_err(|e| e.to_string())?;
        }
    }
    panic::set_hook(default_hook);
    if let Some(mut w) = writer {
        w.flush().map_err(|e| e.to_string())?;
    }

    print_summary(records.len(), &totals, &by_family);
    let agree: usize = totals.iter().filter(|(o, _)| o.is_good()).map(|(_, n)| n).sum();
    println!("\nagree with Kusto (Match + KustoRejected-OurError): {agree}/{}", records.len());
    if args.compare_csharp {
        print_cross(&cross);
    }
    println!(
        "\nvs C#: kept {kept}/{csharp_matches} C# matches, regressions {regressions}, improvements {improvements}"
    );
    Ok(())
}

fn print_detail(rec: &Record, res: &RunResult) {
    let v = &res.verdict;
    println!("=== {} [{}] {} (C#: {})", rec.key(), rec.family, v.outcome, rec.outcome);
    println!("KQL:  {}", rec.kql);
    if let Some(sql) = &res.sql {
        println!("SQL:  {sql}");
    }
    if let Some(d) = &v.detail {
        println!("detail: {d}");
    }
    if !v.sub_verdicts.is_empty() {
        println!("subs: {}", v.sub_verdicts.join(" "));
    }
    if let Some(k) = &rec.kusto {
        if k.error.is_none() && matches!(v.outcome, Outcome::MismatchRows | Outcome::MismatchOrder | Outcome::MismatchColumns) {
            println!("kusto: {} rows {:?}", k.row_count, k.columns);
            for r in &k.sample_rows {
                println!("  {r}");
            }
            if let Some(d) = &res.duck {
                let cols: Vec<String> = d.columns.iter().map(|c| format!("{}:{}", c.name, c.class)).collect();
                println!("duck:  {} rows {:?}", d.rows.len(), cols);
                for r in d.rows.iter().take(8) {
                    println!("  {}", format_row(r));
                }
            }
        }
    }
    println!();
}

fn print_summary(total: usize, totals: &BTreeMap<Outcome, usize>, by_family: &BTreeMap<String, BTreeMap<Outcome, usize>>) {
    println!("\n{total} records");
    println!("{:<28} {:>6} {:>7}", "outcome", "count", "%");
    for (o, n) in totals {
        println!("{:<28} {:>6} {:>6.1}%", o.name(), n, 100.0 * *n as f64 / total as f64);
    }

    let outcomes: Vec<Outcome> = totals.keys().copied().collect();
    let abbrev = |o: Outcome| o.short();
    println!("\nby family ({})", outcomes.iter().map(|o| format!("{}={}", abbrev(*o), o.name())).collect::<Vec<_>>().join(", "));
    print!("{:<28}", "family");
    for o in &outcomes {
        print!(" {:>6}", abbrev(*o));
    }
    println!(" {:>6}", "total");
    for (family, counts) in by_family {
        print!("{family:<28}");
        for o in &outcomes {
            print!(" {:>6}", counts.get(o).copied().unwrap_or(0));
        }
        println!(" {:>6}", counts.values().sum::<usize>());
    }
}

fn print_cross(cross: &BTreeMap<(String, Outcome), usize>) {
    println!("\nC# outcome -> our outcome");
    let mut current = "";
    for ((cs, ours), n) in cross {
        if cs != current {
            println!("{cs}");
            current = cs;
        }
        println!("    -> {:<28} {n:>6}", ours.name());
    }
}

fn cmd_one(args: &[String]) -> Result<(), String> {
    let mut kql = None;
    let mut id = None;
    let mut inputs = Vec::new();
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let mut value = || it.next().cloned().ok_or_else(|| format!("{a} needs a value\n{USAGE}"));
        match a.as_str() {
            "--kql" => kql = Some(value()?),
            "--id" => id = Some(value()?),
            "--in" => inputs.push(PathBuf::from(value()?)),
            other => return Err(format!("unknown argument {other}\n{USAGE}")),
        }
    }
    if let Some(id) = id {
        if inputs.is_empty() {
            inputs.push(default_corpus());
        }
        let records = load(&inputs)?;
        let hits: Vec<&Record> = records.iter().filter(|r| r.id == id || r.key() == id).collect();
        if hits.is_empty() {
            return Err(format!("no record with id {id}"));
        }
        for rec in hits {
            let res = run_record(rec, false);
            print_detail(rec, &res);
            if let Some(d) = &res.duck {
                if !matches!(res.verdict.outcome, Outcome::MismatchRows | Outcome::MismatchOrder | Outcome::MismatchColumns) {
                    print_rows(d);
                }
            }
        }
        return Ok(());
    }

    let kql = kql.ok_or_else(|| USAGE.to_string())?;
    let t = translate(&kql).map_err(|e| format!("translate error: {e}"))?;
    println!("SQL:\n{}\n", t.sql);
    let declared: Vec<String> = t.columns.iter().map(|c| format!("{}:{}", c.name, c.ty)).collect();
    println!("declared columns: [{}]", declared.join(", "));
    let classes: Vec<Class> = t.columns.iter().map(|c| Class::from_kql(c.ty)).collect();
    let d = execute(&t.sql, &classes).map_err(|e| format!("execution error: {e}"))?;
    print_rows(&d);
    Ok(())
}

fn print_rows(d: &DuckResult) {
    let cols: Vec<String> = d.columns.iter().map(|c| format!("{}:{}", c.name, c.class)).collect();
    println!("result columns: [{}]", cols.join(", "));
    println!("{} rows", d.rows.len());
    for r in &d.rows {
        println!("  {}", format_row(r));
    }
}
