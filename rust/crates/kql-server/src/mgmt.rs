//! Built-in `.show` management commands answered from the catalog (never by splicing user input
//! into SQL).

use kql_to_sql::{Column, KqlType};
use serde_json::{json, Value as Json};

use crate::error::ApiError;
use crate::protocol::{v1_string_table, v1_table};
use crate::AppState;

/// A command token: a lower-cased word, a bracketed/quoted name (kept verbatim), or a symbol.
#[derive(Debug, Clone, PartialEq)]
enum Word {
    Word(String),
    Name(String),
    Sym(char),
}

fn strip_comments(mut t: &str) -> &str {
    loop {
        t = t.trim_start();
        match t.strip_prefix("//") {
            Some(rest) => t = rest.find('\n').map(|i| &rest[i + 1..]).unwrap_or(""),
            None => return t,
        }
    }
}

fn words(text: &str) -> Result<Vec<Word>, ApiError> {
    let chars: Vec<char> = strip_comments(text).trim().chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    let is_word = |c: char| c.is_alphanumeric() || c == '_' || c == '-' || c == '.';
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if is_word(c) {
            let start = i;
            while i < chars.len() && is_word(chars[i]) {
                i += 1;
            }
            out.push(Word::Word(chars[start..i].iter().collect()));
        } else if c == '[' && matches!(chars.get(i + 1), Some('\'') | Some('"')) {
            // ['name'] / ["name"] with doubled-quote escapes
            let q = chars[i + 1];
            i += 2;
            let mut s = String::new();
            loop {
                match chars.get(i) {
                    None => return Err(ApiError::bad_request("syntax error: unterminated quoted name")),
                    Some(&ch) if ch == q && chars.get(i + 1) == Some(&q) => {
                        s.push(q);
                        i += 2;
                    }
                    Some(&ch) if ch == q => {
                        i += 1;
                        break;
                    }
                    Some(&ch) => {
                        s.push(ch);
                        i += 1;
                    }
                }
            }
            if chars.get(i) != Some(&']') {
                return Err(ApiError::bad_request("syntax error: expected ']' after a quoted name"));
            }
            i += 1;
            out.push(Word::Name(s));
        } else {
            out.push(Word::Sym(c));
            i += 1;
        }
    }
    Ok(out)
}

fn kw(w: &Word) -> Option<String> {
    match w {
        Word::Word(s) => Some(s.to_ascii_lowercase()),
        _ => None,
    }
}

fn name(w: &Word) -> Option<String> {
    match w {
        Word::Word(s) => Some(s.clone()),
        Word::Name(s) => Some(s.clone()),
        Word::Sym(_) => None,
    }
}

/// Answers a built-in `.show` command, or returns `None` if `csl` is not one of them.
pub fn builtin_show(state: &AppState, csl: &str, db_name: &str) -> Result<Option<Json>, ApiError> {
    let w = words(csl)?;
    let k: Vec<String> = w.iter().map(|x| kw(x).unwrap_or_default()).collect();
    let ks: Vec<&str> = k.iter().map(String::as_str).collect();
    let r = match ks.as_slice() {
        [".show", "version"] => show_version(),
        [".show", "cluster", "monitoring", ..] => show_cluster_monitoring(),
        [".show", "tables"] => show_tables(state, db_name)?,
        [".show", "databases"] | [".show", "database"] => show_databases(state, db_name),
        // the C# server's shape: the schema object itself, not wrapped in a v1 table
        [".show", "databases", "as", "json"] => databases_schema(state, db_name)?,
        // Kusto's shape: one row whose DatabaseSchema column holds the JSON text
        [".show", "databases", "schema", "as", "json"]
        | [".show", "database", "schema", "as", "json"]
        | [".show", "schema", "as", "json"]
        | [".show", "database", _, "schema", "as", "json"] => {
            let schema = databases_schema(state, db_name)?;
            json!({"Tables": [v1_string_table("DatabaseSchema", &["DatabaseSchema"], vec![vec![json!(schema.to_string())]])]})
        }
        [".show", "table", _, rest @ ..] => {
            let t = name(&w[2])
                .ok_or_else(|| ApiError::bad_request("syntax error: expected a table name after '.show table'"))?;
            match rest {
                ["schema"] => table_schema_rows(state, &t)?,
                ["schema", "as", "json"] => table_schema_json(state, &t, db_name, false)?,
                ["cslschema"] => table_schema_json(state, &t, db_name, true)?,
                _ => return Ok(None),
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(r))
}

fn lookup(state: &AppState, table: &str) -> Result<(String, Vec<Column>), ApiError> {
    let cat = state.db.catalog()?;
    cat.table(table)
        .map(|(n, c)| (n.to_string(), c.clone()))
        .ok_or_else(|| ApiError::bad_request(format!("Table '{table}' was not found")))
}

fn show_version() -> Json {
    json!({"Tables": [v1_string_table(
        "Version",
        &["BuildVersion", "BuildTime", "ServiceType", "ProductVersion", "ServiceOffering"],
        vec![vec![
            json!(format!("kql-server {}", env!("CARGO_PKG_VERSION"))),
            json!(""),
            json!("Engine"),
            json!(format!("kql-server {} (DuckDB)", env!("CARGO_PKG_VERSION"))),
            json!("")
        ]],
    )]})
}

fn show_cluster_monitoring() -> Json {
    json!({"Tables": [v1_string_table(
        "ClusterMonitoring",
        &["KustoAccount", "ClusterAlias", "GenevaMonitoringAccount", "DataCenter", "CloudName", "CloudResourceId", "VirtualClusterName"],
        vec![vec![json!("N/A"), json!("N/A"), json!("N/A"), json!("N/A"), json!("DevPublicCloud"), json!("N/A"), json!("N/A")]],
    )]})
}

fn show_tables(state: &AppState, db_name: &str) -> Result<Json, ApiError> {
    let rows = state
        .db
        .base_tables()?
        .into_iter()
        .map(|(t, _)| vec![json!(t), json!(db_name), json!(""), json!("")])
        .collect();
    Ok(json!({"Tables": [v1_string_table("Tables", &["TableName", "DatabaseName", "Folder", "DocString"], rows)]}))
}

fn show_databases(state: &AppState, db_name: &str) -> Json {
    let row = vec![
        json!(db_name),
        json!(""),
        json!("v1.0"),
        json!("TRUE"),
        json!(access_mode(state)),
        json!(""),
        json!(""),
        json!(uuid::Uuid::new_v4().to_string()),
        json!(""),
        json!(""),
    ];
    json!({"Tables": [v1_string_table(
        "Databases",
        &["DatabaseName", "PersistentStorage", "Version", "IsCurrent", "DatabaseAccessMode", "PrettyName", "ReservedSlot1", "DatabaseId", "InTransitionTo", "SuspensionState"],
        vec![row],
    )]})
}

fn access_mode(state: &AppState) -> &'static str {
    if state.config.read_only {
        "ReadOnly"
    } else {
        "ReadWrite"
    }
}

fn ordered_columns(cols: &[Column]) -> Json {
    Json::Array(cols.iter().map(|c| json!({"Name": c.name, "Type": c.ty.clr_name(), "CslType": c.ty.name()})).collect())
}

fn databases_schema(state: &AppState, db_name: &str) -> Result<Json, ApiError> {
    let mut tables = serde_json::Map::new();
    for (t, cols) in state.db.base_tables()? {
        tables.insert(
            t.clone(),
            json!({"Name": t, "OrderedColumns": ordered_columns(&cols), "Folder": "", "DocString": ""}),
        );
    }
    let access = access_mode(state);
    let mut dbs = serde_json::Map::new();
    dbs.insert(
        db_name.to_string(),
        json!({
            "Name": db_name,
            "Tables": tables,
            "MajorVersion": 2,
            "MinorVersion": 0,
            "Functions": {},
            "DatabaseAccessMode": access,
            "ExternalTables": {},
            "MaterializedViews": {},
            "EntityGroups": {},
            "Graphs": {},
            "StoredQueryResults": {}
        }),
    );
    Ok(json!({ "Databases": dbs }))
}

/// `.show table T schema` — one row per column (C# server shape plus the Kusto type).
fn table_schema_rows(state: &AppState, table: &str) -> Result<Json, ApiError> {
    let (_, cols) = lookup(state, table)?;
    let rows = cols.iter().map(|c| vec![json!(c.name), json!(c.ty.clr_name()), json!(c.ty.name())]).collect();
    let tcols = [
        Column::new("ColumnName", KqlType::String),
        Column::new("DataType", KqlType::String),
        Column::new("ColumnType", KqlType::String),
    ];
    Ok(json!({"Tables": [v1_table("TableSchema", &tcols, rows)]}))
}

/// `.show table T schema as json` / `.show table T cslschema` — Kusto's
/// `TableName, Schema, DatabaseName, Folder, DocString` row.
fn table_schema_json(state: &AppState, table: &str, db_name: &str, csl: bool) -> Result<Json, ApiError> {
    let (t, cols) = lookup(state, table)?;
    let schema = if csl {
        cols.iter().map(|c| format!("{}:{}", c.name, c.ty.name())).collect::<Vec<_>>().join(", ")
    } else {
        json!({"Name": t, "OrderedColumns": ordered_columns(&cols)}).to_string()
    };
    let row = vec![json!(t), json!(schema), json!(db_name), json!(""), json!("")];
    Ok(
        json!({"Tables": [v1_string_table("Table_0", &["TableName", "Schema", "DatabaseName", "Folder", "DocString"], vec![row])]}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenizes_names() {
        assert_eq!(
            words(".show table ['a''b'] schema").unwrap(),
            vec![
                Word::Word(".show".into()),
                Word::Word("table".into()),
                Word::Name("a'b".into()),
                Word::Word("schema".into())
            ]
        );
        assert!(words(".show table ['x").is_err());
    }
}
