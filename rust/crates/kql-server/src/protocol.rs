//! Kusto v1 / v2 response shapes.

use kql_to_sql::{Column, KqlType, RenderInfo};
use serde_json::{json, Value as Json};

use crate::db::{ResultSet, Truncation};
use crate::values::{now_iso, v1_data_type};

fn guid() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// v1 column descriptor: `{ColumnName, DataType, ColumnType}`.
pub fn v1_columns(cols: &[Column]) -> Json {
    Json::Array(
        cols.iter()
            .map(|c| json!({"ColumnName": c.name, "DataType": v1_data_type(c.ty), "ColumnType": c.ty.name()}))
            .collect(),
    )
}

/// v2 column descriptor: `{ColumnName, ColumnType}`.
pub fn v2_columns(cols: &[Column]) -> Json {
    Json::Array(cols.iter().map(|c| json!({"ColumnName": c.name, "ColumnType": c.ty.name()})).collect())
}

/// A v1 table.
pub fn v1_table(name: &str, cols: &[Column], rows: Vec<Vec<Json>>) -> Json {
    json!({"TableName": name, "Columns": v1_columns(cols), "Rows": rows})
}

/// A v1 table of string columns.
pub fn v1_string_table(name: &str, cols: &[&str], rows: Vec<Vec<Json>>) -> Json {
    let cols: Vec<Column> = cols.iter().map(|c| Column::new(*c, KqlType::String)).collect();
    v1_table(name, &cols, rows)
}

fn partial_failure(t: &Truncation) -> Json {
    json!({
        "error": {
            "code": "LimitsExceeded",
            "message": "Request is invalid and cannot be executed.",
            "@type": "Kusto.Data.Exceptions.KustoServicePartialQueryFailureLimitsExceededException",
            "@message": t.message(),
            "@permanent": true,
        }
    })
}

/// v1 query response. On truncation a `QueryStatus` table with the error row is appended, the way
/// Kusto reports a partial query failure in v1.
pub fn v1_query(rs: ResultSet) -> Json {
    let mut tables = vec![v1_table("PrimaryResult", &rs.columns, rs.rows)];
    if let Some(t) = &rs.truncated {
        let cols = [
            Column::new("Timestamp", KqlType::DateTime),
            Column::new("Severity", KqlType::Int),
            Column::new("SeverityName", KqlType::String),
            Column::new("StatusCode", KqlType::Int),
            Column::new("StatusDescription", KqlType::String),
            Column::new("Count", KqlType::Int),
            Column::new("RequestId", KqlType::Guid),
            Column::new("ActivityId", KqlType::Guid),
            Column::new("SubActivityId", KqlType::Guid),
            Column::new("ClientActivityId", KqlType::String),
        ];
        let row = vec![
            json!(now_iso()),
            json!(2),
            json!("Error"),
            json!(-2133196797),
            json!(t.message()),
            json!(1),
            json!(guid()),
            json!(guid()),
            json!(guid()),
            json!(""),
        ];
        tables.push(v1_table("QueryStatus", &cols, vec![row]));
    }
    json!({ "Tables": tables })
}

fn axis_bound(s: &Option<String>, default: Json) -> Json {
    match s {
        None => default,
        Some(s) => s
            .parse::<f64>()
            .ok()
            .and_then(|f| serde_json::Number::from_f64(f).map(Json::Number))
            .unwrap_or_else(|| json!(s)),
    }
}

/// The "Visualization" annotation (a JSON string) Kusto emits in @ExtendedProperties: populated
/// from `| render`, otherwise all null (Ymin/Ymax "NaN").
pub fn visualization(render: Option<&RenderInfo>) -> String {
    let r = render.cloned().unwrap_or_default();
    let present = render.is_some();
    let opt = |s: &Option<String>| s.clone().map(Json::String).unwrap_or(Json::Null);
    let list = |v: &Vec<String>| if v.is_empty() { Json::Null } else { json!(v) };
    json!({
        "Visualization": if present { json!(r.visualization) } else { Json::Null },
        "Title": opt(&r.title),
        "XColumn": opt(&r.x_column),
        "Series": list(&r.series),
        "YColumns": list(&r.y_columns),
        "AnomalyColumns": list(&r.anomaly_columns),
        "XTitle": opt(&r.x_title),
        "YTitle": opt(&r.y_title),
        "XAxis": opt(&r.x_axis),
        "YAxis": opt(&r.y_axis),
        "Legend": opt(&r.legend),
        "YSplit": opt(&r.y_split),
        "Accumulate": r.accumulate,
        "IsQuerySorted": false,
        "Kind": opt(&r.kind),
        "Ymin": axis_bound(&r.ymin, json!("NaN")),
        "Ymax": axis_bound(&r.ymax, json!("NaN")),
        "Xmin": axis_bound(&r.xmin, Json::Null),
        "Xmax": axis_bound(&r.xmax, Json::Null),
    })
    .to_string()
}

fn extended_properties(render: Option<&RenderInfo>) -> Json {
    json!({
        "FrameType": "DataTable",
        "TableId": 0,
        "TableKind": "QueryProperties",
        "TableName": "@ExtendedProperties",
        "Columns": [
            {"ColumnName": "TableId", "ColumnType": "int"},
            {"ColumnName": "Key", "ColumnType": "string"},
            {"ColumnName": "Value", "ColumnType": "dynamic"}
        ],
        "Rows": [[1, "Visualization", visualization(render)]]
    })
}

fn completion_information(client_request_id: &str, truncated: Option<&Truncation>) -> Json {
    let crid = if client_request_id.is_empty() {
        format!("KqlServer.Query;{}", guid())
    } else {
        client_request_id.to_string()
    };
    let mut rows = vec![
        json!([
            now_iso(),
            crid,
            guid(),
            guid(),
            guid(),
            4,
            "Info",
            0,
            "S_OK (0)",
            4,
            "QueryInfo",
            "{\"Count\":2,\"Text\":\"Query completed successfully\"}"
        ]),
        json!([
            now_iso(),
            crid,
            guid(),
            guid(),
            guid(),
            5,
            "WorkloadGroup",
            0,
            "S_OK (0)",
            0,
            "QueryResourceConsumption",
            "{\"Count\":1,\"Text\":\"default\"}"
        ]),
    ];
    if let Some(t) = truncated {
        rows.push(json!([
            now_iso(),
            crid,
            guid(),
            guid(),
            guid(),
            2,
            "Error",
            -2133196797,
            "E_QUERY_RESULT_SET_TOO_LARGE (0x80DA0003)",
            2,
            "QueryError",
            json!({"Count": 1, "Text": t.message()}).to_string()
        ]));
    }
    json!({
        "FrameType": "DataTable",
        "TableId": 2,
        "TableKind": "QueryCompletionInformation",
        "TableName": "QueryCompletionInformation",
        "Columns": [
            {"ColumnName": "Timestamp", "ColumnType": "datetime"},
            {"ColumnName": "ClientRequestId", "ColumnType": "string"},
            {"ColumnName": "ActivityId", "ColumnType": "guid"},
            {"ColumnName": "SubActivityId", "ColumnType": "guid"},
            {"ColumnName": "ParentActivityId", "ColumnType": "guid"},
            {"ColumnName": "Level", "ColumnType": "int"},
            {"ColumnName": "LevelName", "ColumnType": "string"},
            {"ColumnName": "StatusCode", "ColumnType": "int"},
            {"ColumnName": "StatusCodeName", "ColumnType": "string"},
            {"ColumnName": "EventType", "ColumnType": "int"},
            {"ColumnName": "EventTypeName", "ColumnType": "string"},
            {"ColumnName": "Payload", "ColumnType": "string"}
        ],
        "Rows": rows
    })
}

fn dataset_completion(truncated: Option<&Truncation>) -> Json {
    match truncated {
        None => json!({"FrameType": "DataSetCompletion", "HasErrors": false, "Cancelled": false}),
        Some(t) => {
            json!({"FrameType": "DataSetCompletion", "HasErrors": true, "Cancelled": false, "OneApiErrors": [partial_failure(t)]})
        }
    }
}

/// v2 query response (frame array).
pub fn v2_query(rs: ResultSet, render: Option<&RenderInfo>, progressive: bool, client_request_id: &str) -> Json {
    let truncated = rs.truncated.clone();
    let header = json!({"FrameType": "DataSetHeader", "IsProgressive": progressive, "Version": "v2.0", "IsFragmented": false, "ErrorReportingPlacement": "InData"});
    let mut frames = vec![header];
    if progressive {
        if render.is_some() {
            frames.push(extended_properties(render));
        }
        let count = rs.rows.len();
        frames.push(json!({"FrameType": "TableHeader", "TableId": 1, "TableKind": "PrimaryResult", "TableName": "PrimaryResult", "Columns": v2_columns(&rs.columns)}));
        frames.push(json!({"FrameType": "TableFragment", "TableId": 1, "FieldCount": rs.columns.len(), "TableFragmentType": "DataAppend", "Rows": rs.rows}));
        let mut completion = json!({"FrameType": "TableCompletion", "TableId": 1, "RowCount": count});
        if let Some(t) = &truncated {
            completion["OneApiErrors"] = json!([partial_failure(t)]);
        }
        frames.push(completion);
    } else {
        frames.push(extended_properties(render));
        frames.push(json!({"FrameType": "DataTable", "TableId": 1, "TableKind": "PrimaryResult", "TableName": "PrimaryResult", "Columns": v2_columns(&rs.columns), "Rows": rs.rows}));
        frames.push(completion_information(client_request_id, truncated.as_ref()));
    }
    frames.push(dataset_completion(truncated.as_ref()));
    Json::Array(frames)
}
