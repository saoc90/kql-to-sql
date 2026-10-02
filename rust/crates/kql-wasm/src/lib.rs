//! WebAssembly bindings: `translate` and `validate`, returning JSON strings.
//!
//! ```js
//! import init, { translate, validate } from './kql_wasm.js';
//! await init();
//! const schema = { StormEvents: { State: "string", StartTime: "datetime" } }; // or SQL types
//! JSON.parse(translate("StormEvents | take 5", "duckdb", JSON.stringify(schema)));
//! // { success: true, sql: "...", columns: [{name, type}], render: {...} | null }
//! ```

use kql_to_sql::{Catalog, Column, Dialect, KqlType, RenderInfo, Translation};
use serde_json::{json, Value};
use wasm_bindgen::prelude::*;

/// Parses `{"Table": {"col": "type", ...}}`; types may be Kusto (`long`) or SQL (`BIGINT`) names.
/// Columns of unknown SQL types are omitted.
pub fn parse_schema(schema_json: &str) -> Result<Catalog, String> {
    if schema_json.trim().is_empty() {
        return Ok(Catalog::new());
    }
    let v: Value = serde_json::from_str(schema_json).map_err(|e| format!("invalid schema JSON: {e}"))?;
    let tables = v.as_object().ok_or("schema must be an object mapping table names to columns")?;
    let mut cat = Catalog::new();
    for (name, cols) in tables {
        let mut out = Vec::new();
        let entries: Vec<(String, String)> = match cols {
            Value::Object(m) => m.iter().map(|(c, t)| (c.clone(), t.as_str().unwrap_or("").to_string())).collect(),
            // also accept [{"name": .., "type": ..}] (DuckDB DESCRIBE rows)
            Value::Array(a) => a
                .iter()
                .map(|c| {
                    let n = c
                        .get("name")
                        .or_else(|| c.get("column_name"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    let t = c
                        .get("type")
                        .or_else(|| c.get("column_type"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    (n, t)
                })
                .collect(),
            _ => return Err(format!("columns of '{name}' must be an object or an array")),
        };
        for (c, t) in entries {
            if let Some(ty) = KqlType::from_name(&t).or_else(|| KqlType::from_sql_type(&t)) {
                out.push(Column::new(c, ty));
            }
        }
        cat = cat.with_table(name.clone(), out);
    }
    Ok(cat)
}

fn dialect(name: &str) -> Dialect {
    match name.to_ascii_lowercase().as_str() {
        "pglite" | "postgres" | "postgresql" | "pg" => Dialect::Postgres,
        _ => Dialect::DuckDb,
    }
}

fn render_json(r: &RenderInfo) -> Value {
    json!({
        "visualization": r.visualization, "title": r.title, "xColumn": r.x_column, "series": r.series,
        "yColumns": r.y_columns, "anomalyColumns": r.anomaly_columns, "xTitle": r.x_title, "yTitle": r.y_title,
        "xAxis": r.x_axis, "yAxis": r.y_axis, "legend": r.legend, "ySplit": r.y_split, "accumulate": r.accumulate,
        "kind": r.kind, "ymin": r.ymin, "ymax": r.ymax, "xmin": r.xmin, "xmax": r.xmax,
    })
}

pub fn translation_json(t: &Translation) -> Value {
    json!({
        "success": true,
        "sql": t.sql,
        "error": null,
        "columns": t.columns.iter().map(|c| json!({"name": c.name, "type": c.ty.name()})).collect::<Vec<_>>(),
        "render": t.render.as_ref().map(render_json),
    })
}

/// Translates KQL to SQL. Returns `{success, sql, error, columns, render}` as JSON.
#[wasm_bindgen]
pub fn translate(kql: &str, dialect_name: &str, schema_json: &str) -> String {
    let result = parse_schema(schema_json)
        .and_then(|cat| kql_to_sql::translate(kql, &cat, dialect(dialect_name)).map_err(|e| e.message));
    match result {
        Ok(t) => translation_json(&t).to_string(),
        Err(e) => json!({"success": false, "sql": null, "error": e, "columns": [], "render": null}).to_string(),
    }
}

/// Checks KQL syntax. Returns `{success, valid, errors: [{message, start, length}]}` as JSON.
#[wasm_bindgen]
pub fn validate(kql: &str) -> String {
    match kql_parser::parse_query(kql) {
        Ok(_) => json!({"success": true, "valid": true, "errors": []}).to_string(),
        Err(e) => json!({
            "success": true,
            "valid": false,
            "errors": [{"message": e.message, "start": e.span.start, "length": e.span.end.saturating_sub(e.span.start).max(1)}]
        })
        .to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translates_with_sql_types() {
        let out: Value = serde_json::from_str(&translate(
            "StormEvents | where State == 'TEXAS' | count | render barchart",
            "duckdb",
            r#"{"StormEvents": [{"column_name": "State", "column_type": "VARCHAR"}]}"#,
        ))
        .unwrap();
        assert_eq!(out["success"], true);
        assert!(out["sql"].as_str().unwrap().contains("COUNT(*)"));
        assert_eq!(out["render"]["visualization"], "barchart");
    }

    #[test]
    fn reports_errors() {
        let out: Value = serde_json::from_str(&translate("Nope | take 1", "duckdb", "{}")).unwrap();
        assert_eq!(out["success"], false);
        let v: Value = serde_json::from_str(&validate("T | where")).unwrap();
        assert_eq!(v["valid"], false);
    }
}
