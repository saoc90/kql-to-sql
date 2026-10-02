//! In-process API tests (axum Router + tower::ServiceExt::oneshot, no network).

use std::path::Path;
use std::time::Duration;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use kql_server::{router, AppState, Database, DbOptions, ServerConfig};
use serde_json::{json, Value};
use tower::ServiceExt;

fn storm_csv() -> &'static Path {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../../src/WebDemo/wwwroot/StormEvents.csv.gz"))
}

/// In-memory server with a small `T` table; `storm` also loads StormEvents.
fn app_with(config: ServerConfig, storm: bool) -> Router {
    let db =
        Database::open(&DbOptions { storm_events_csv: storm.then(|| storm_csv().to_path_buf()), ..Default::default() })
            .unwrap();
    let app = AppState::new(config, db);
    let conn = app.db.connect().unwrap();
    conn.execute_batch(
        "CREATE TABLE T (Id BIGINT, Name VARCHAR, Score DOUBLE, Ts TIMESTAMP, Flag BOOLEAN, Bag JSON, G UUID);
         INSERT INTO T VALUES
           (1, 'alpha', 1.5, TIMESTAMP '2007-09-29 08:11:00', true, '{\"a\":[1,2]}', '6f0b1c4e-8d36-4b58-9a43-2d1f0c9b1e01'),
           (2, 'beta', NULL, TIMESTAMP '2007-01-01 00:00:00.1234567', false, NULL, NULL),
           (3, 'gamma', 3.0, NULL, NULL, '[1]', NULL);
         CREATE TABLE Big AS SELECT range AS n FROM range(1000);",
    )
    .unwrap();
    router(app)
}

fn app() -> Router {
    app_with(ServerConfig::default(), false)
}

async fn send(app: &Router, req: Request<Body>) -> (StatusCode, axum::http::HeaderMap, Value) {
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let v = serde_json::from_slice(&bytes).unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into()));
    (status, headers, v)
}

fn post(path: &str, body: Value) -> Request<Body> {
    Request::post(path)
        .header(header::CONTENT_TYPE, "application/json; charset=utf-8")
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn call(app: &Router, path: &str, body: Value) -> (StatusCode, Value) {
    let (s, _, v) = send(app, post(path, body)).await;
    (s, v)
}

fn frame<'a>(frames: &'a Value, kind: &str) -> &'a Value {
    frames
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["TableKind"] == kind)
        .unwrap_or_else(|| panic!("no {kind} frame in {frames}"))
}

#[tokio::test]
async fn v1_query_shape_and_values() {
    let app = app();
    let (s, v) = call(&app, "/v1/rest/query", json!({"db": "x", "csl": "T | order by Id asc"})).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let t = &v["Tables"][0];
    assert_eq!(t["TableName"], "PrimaryResult");
    let cols: Vec<(String, String, String)> = t["Columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["ColumnName"].as_str().unwrap().into(),
                c["DataType"].as_str().unwrap().into(),
                c["ColumnType"].as_str().unwrap().into(),
            )
        })
        .collect();
    assert_eq!(cols[0], ("Id".into(), "Int64".into(), "long".into()));
    assert_eq!(cols[1], ("Name".into(), "String".into(), "string".into()));
    assert_eq!(cols[2], ("Score".into(), "Double".into(), "real".into()));
    assert_eq!(cols[3], ("Ts".into(), "DateTime".into(), "datetime".into()));
    assert_eq!(cols[5], ("Bag".into(), "Object".into(), "dynamic".into()));
    assert_eq!(cols[6], ("G".into(), "Guid".into(), "guid".into()));
    let rows = &t["Rows"];
    assert_eq!(
        rows[0],
        json!([1, "alpha", 1.5, "2007-09-29T08:11:00.0000000Z", true, {"a": [1, 2]}, "6f0b1c4e-8d36-4b58-9a43-2d1f0c9b1e01"])
    );
    assert_eq!(rows[1][2], Value::Null);
    assert_eq!(rows[1][3], "2007-01-01T00:00:00.1234560Z");
    assert_eq!(rows[2][3], Value::Null);
}

#[tokio::test]
async fn timespans_and_case_insensitive_fields() {
    let app = app();
    let (s, v) = call(&app, "/v1/rest/query", json!({"CSL": "print a = 1d + 2h + 3s, b = -1500ms, c = 0s"})).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["Tables"][0]["Columns"][0]["ColumnType"], "timespan");
    assert_eq!(v["Tables"][0]["Columns"][0]["DataType"], "TimeSpan");
    assert_eq!(v["Tables"][0]["Rows"][0], json!(["1.02:00:03", "-00:00:01.5000000", "00:00:00"]));
}

/// Port of QueryApiTests.QueryPrintReturnsResult plus the frame structure.
#[tokio::test]
async fn v2_frames() {
    let app = app();
    let (s, v) =
        call(&app, "/v2/rest/query", json!({"db": "StormEvents", "csl": "print Test=\"Hello, World!\""})).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v[1]["Rows"].is_array(), true);
    // root[1] is the @ExtendedProperties table; the primary result follows it
    let kinds: Vec<&str> = v.as_array().unwrap().iter().map(|f| f["FrameType"].as_str().unwrap()).collect();
    assert_eq!(kinds, ["DataSetHeader", "DataTable", "DataTable", "DataTable", "DataSetCompletion"]);
    assert_eq!(v[0]["Version"], "v2.0");
    assert_eq!(v[0]["IsProgressive"], false);
    assert_eq!(v[1]["TableName"], "@ExtendedProperties");
    let primary = frame(&v, "PrimaryResult");
    assert_eq!(primary["TableId"], 1);
    assert_eq!(primary["Columns"], json!([{"ColumnName": "Test", "ColumnType": "string"}]));
    assert_eq!(primary["Rows"][0][0], "Hello, World!");
    let qci = frame(&v, "QueryCompletionInformation");
    assert_eq!(qci["Columns"].as_array().unwrap().len(), 12);
    assert_eq!(v[4]["HasErrors"], false);
    // no render: all-null visualization with NaN y-bounds
    let vis: Value = serde_json::from_str(v[1]["Rows"][0][2].as_str().unwrap()).unwrap();
    assert_eq!(vis["Visualization"], Value::Null);
    assert_eq!(vis["Ymin"], "NaN");
    assert_eq!(vis["Xmin"], Value::Null);
}

#[tokio::test]
async fn v2_progressive() {
    let app = app();
    let body = json!({"csl": "T | count", "properties": json!({"Options": {"results_progressive_enabled": true}}).to_string()});
    let (s, v) = call(&app, "/v2/rest/query", body).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let kinds: Vec<&str> = v.as_array().unwrap().iter().map(|f| f["FrameType"].as_str().unwrap()).collect();
    assert_eq!(kinds, ["DataSetHeader", "TableHeader", "TableFragment", "TableCompletion", "DataSetCompletion"]);
    assert_eq!(v[0]["IsProgressive"], true);
    assert_eq!(v[2]["Rows"], json!([[3]]));
    assert_eq!(v[3]["RowCount"], 1);
}

#[tokio::test]
async fn v2_render_metadata_with_storm_events() {
    let app = app_with(ServerConfig::default(), true);
    let kql = "StormEvents | summarize count() by State | top 5 by count_ | render columnchart with (title='Top states', ycolumns=count_)";
    let (s, v) = call(&app, "/v2/rest/query", json!({"csl": kql})).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let props = &v[1];
    assert_eq!(props["TableKind"], "QueryProperties");
    assert_eq!(props["Rows"][0][1], "Visualization");
    let vis: Value = serde_json::from_str(props["Rows"][0][2].as_str().unwrap()).unwrap();
    assert_eq!(vis["Visualization"], "columnchart");
    assert_eq!(vis["Title"], "Top states");
    assert_eq!(vis["YColumns"], json!(["count_"]));
    let primary = frame(&v, "PrimaryResult");
    assert_eq!(primary["Rows"].as_array().unwrap().len(), 5);
    assert_eq!(primary["Rows"][0][0], "TEXAS");
    assert_eq!(primary["Columns"][1], json!({"ColumnName": "count_", "ColumnType": "long"}));
    // Kusto's StormEvents schema (not the NOAA one)
    let (_, v) = call(&app, "/v1/rest/mgmt", json!({"csl": ".show table StormEvents schema as json"})).await;
    let schema: Value = serde_json::from_str(v["Tables"][0]["Rows"][0][1].as_str().unwrap()).unwrap();
    let names: Vec<&str> =
        schema["OrderedColumns"].as_array().unwrap().iter().map(|c| c["Name"].as_str().unwrap()).collect();
    assert_eq!(names[..4], ["StartTime", "EndTime", "EpisodeId", "EventId"]);
    assert!(names.contains(&"StormSummary"));
    let (s, v) = call(&app, "/v1/rest/query", json!({"csl": "StormEvents | take 1 | project StormSummary"})).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert!(v["Tables"][0]["Rows"][0][0].is_object(), "{v}");
}

#[tokio::test]
async fn mgmt_show_commands() {
    let app = app();
    // MgmtShowTablesReturnsTableList
    let (s, v) = call(&app, "/v1/rest/mgmt", json!({"db": "StormEvents", "csl": ".show tables"})).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let names: Vec<&str> = v["Tables"][0]["Rows"].as_array().unwrap().iter().map(|r| r[0].as_str().unwrap()).collect();
    assert_eq!(names, ["Big", "T"]);
    assert_eq!(v["Tables"][0]["Rows"][0][1], "StormEvents");
    // MgmtShowTableSchemaReturnsColumns
    let (_, v) = call(&app, "/v1/rest/mgmt", json!({"csl": ".show table T schema"})).await;
    let rows = v["Tables"][0]["Rows"].as_array().unwrap();
    assert_eq!(rows[0], json!(["Id", "System.Int64", "long"]));
    // .show table T schema as json / cslschema
    let (_, v) = call(&app, "/v1/rest/mgmt", json!({"csl": ".show table T schema as json"})).await;
    assert_eq!(v["Tables"][0]["Columns"][1]["ColumnName"], "Schema");
    let schema: Value = serde_json::from_str(v["Tables"][0]["Rows"][0][1].as_str().unwrap()).unwrap();
    assert_eq!(schema["Name"], "T");
    assert_eq!(schema["OrderedColumns"][0], json!({"Name": "Id", "Type": "System.Int64", "CslType": "long"}));
    let (_, v) = call(&app, "/v1/rest/mgmt", json!({"csl": ".show table t cslschema"})).await;
    assert!(v["Tables"][0]["Rows"][0][1].as_str().unwrap().starts_with("Id:long, Name:string"));
    // MgmtShowDatabasesAsJson_ReturnsSchema
    let (_, v) = call(&app, "/v1/rest/mgmt", json!({"db": "StormEvents", "csl": ".show databases as json"})).await;
    let db = &v["Databases"]["StormEvents"];
    assert_eq!(db["Name"], "StormEvents");
    assert_eq!(db["Tables"]["T"]["Name"], "T");
    let first = &db["Tables"]["T"]["OrderedColumns"][0];
    assert!(first.get("Name").is_some() && first.get("Type").is_some() && first.get("CslType").is_some());
    // Kusto's form: JSON text in one cell
    let (_, v) = call(&app, "/v1/rest/mgmt", json!({"csl": ".show databases schema as json"})).await;
    let s: Value = serde_json::from_str(v["Tables"][0]["Rows"][0][0].as_str().unwrap()).unwrap();
    assert!(s["Databases"]["NetDefaultDB"]["Tables"]["Big"].is_object());
    // .show version / .show databases
    let (s, v) = call(&app, "/v1/rest/mgmt", json!({"csl": ".show version"})).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["Tables"][0]["Columns"][0]["ColumnName"], "BuildVersion");
    let (_, v) = call(&app, "/v1/rest/mgmt", json!({"db": "Db1", "csl": ".show databases"})).await;
    assert_eq!(v["Tables"][0]["Rows"][0][0], "Db1");
    // a .show handled by the translator
    let (s, v) = call(&app, "/v1/rest/mgmt", json!({"csl": ".show functions"})).await;
    assert_eq!(s, StatusCode::OK, "{v}");
}

#[tokio::test]
async fn query_endpoints_reject_commands() {
    let app = app();
    for path in ["/v1/rest/query", "/v2/rest/query"] {
        for csl in [".show tables", "// comment\n.drop table T", ".create table X (a:int)"] {
            let (s, v) = call(&app, path, json!({"csl": csl})).await;
            assert_eq!(s, StatusCode::BAD_REQUEST, "{path} {csl}");
            assert_eq!(v["error"]["code"], "General_BadRequest");
            assert!(v["error"]["@message"].as_str().unwrap().contains("management commands"));
        }
    }
    let (_, v) = call(&app, "/v1/rest/query", json!({"csl": "T | count"})).await;
    assert_eq!(v["Tables"][0]["Rows"], json!([[3]]));
}

#[tokio::test]
async fn mutating_commands_need_allow_commands() {
    let app = app();
    let (s, v) = call(&app, "/v1/rest/mgmt", json!({"csl": ".drop table T"})).await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{v}");
    assert_eq!(v["error"]["code"], "Forbidden");
    let (_, v) = call(&app, "/v1/rest/query", json!({"csl": "T | count"})).await;
    assert_eq!(v["Tables"][0]["Rows"], json!([[3]]));

    let app = app_with(ServerConfig { allow_commands: true, ..Default::default() }, false);
    let (s, v) = call(&app, "/v1/rest/mgmt", json!({"csl": ".create table People (Name:string, Age:int)"})).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let (s, v) =
        call(&app, "/v1/rest/mgmt", json!({"csl": ".ingest inline into table People <|\nJohn,1\nJane,2"})).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["Tables"][0]["Rows"][0][0], "Completed");
    // the catalog cache is invalidated: the new table is queryable
    let (s, v) = call(&app, "/v1/rest/query", json!({"csl": "People | order by Age asc"})).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["Tables"][0]["Rows"], json!([["John", 1], ["Jane", 2]]));
}

#[tokio::test]
async fn show_table_injection_is_rejected() {
    let app = app();
    for csl in [
        ".show table T'; DROP TABLE T; -- schema",
        ".show table ['T''; DROP TABLE T; --'] schema",
        ".show table [\"x' OR '1'='1\"] schema as json",
        ".show table nosuch schema",
    ] {
        let (s, v) = call(&app, "/v1/rest/mgmt", json!({"csl": csl})).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{csl}: {v}");
        assert!(v["error"]["message"].is_string());
    }
    let (_, v) = call(&app, "/v1/rest/query", json!({"csl": "T | count"})).await;
    assert_eq!(v["Tables"][0]["Rows"], json!([[3]]), "table T must survive");
}

#[tokio::test]
async fn errors_use_the_kusto_envelope() {
    let app = app();
    let (s, v) = call(&app, "/v2/rest/query", json!({"csl": "T | where"})).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(v["error"]["code"], "General_BadRequest");
    assert!(v["error"]["@type"].is_string() && v["error"]["@message"].is_string());
    let (s, v) = call(&app, "/v1/rest/query", json!({"csl": "NoSuchTable"})).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
    let (s, v) = call(&app, "/v1/rest/mgmt", json!({"csl": ".frobnicate"})).await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{v}");
    let (s, v) = call(&app, "/v1/rest/mgmt", json!({"csl": ".show nonsense"})).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{v}");
    assert!(v["error"]["message"].is_string());
    let (s, _, v) = send(&app, Request::post("/v1/rest/query").body(Body::from("not json")).unwrap()).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(v["error"]["code"], "General_BadRequest");
    // engine error (valid translation, runtime failure) → 500
    let (s, v) = call(&app, "/v1/rest/query", json!({"csl": "print toint(parse_json('[1]')[5]) / 0, x = toscalar(range i from 1 to 2 step 1 | project tolong(i)) "})).await;
    assert!(s == StatusCode::OK || s == StatusCode::INTERNAL_SERVER_ERROR, "{s} {v}");
    let (s, _, v) = send(&app, Request::get("/nope").body(Body::empty()).unwrap()).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    assert!(v["error"]["code"].is_string());
}

#[tokio::test]
async fn sql_cannot_touch_files() {
    // the translator never emits file functions, but the engine is locked down regardless
    let db = Database::open(&DbOptions::default()).unwrap();
    let conn = db.connect().unwrap();
    assert!(conn.execute_batch("SELECT * FROM read_csv('/etc/passwd')").is_err());
    assert!(conn.execute_batch("COPY (SELECT 1) TO '/tmp/kql-server-test.csv'").is_err());
    assert!(conn.execute_batch("SET enable_external_access = true").is_err());
    assert!(conn.execute_batch("ATTACH '/tmp/kql-server-test.duckdb'").is_err());
}

#[tokio::test]
async fn auth_required_when_token_set() {
    let app = app_with(ServerConfig { token: Some("s3cret".into()), ..Default::default() }, false);
    let body = json!({"csl": "print 1"});
    let (s, h, v) = send(&app, post("/v2/rest/query", body.clone())).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    assert_eq!(v["error"]["code"], "Unauthorized");
    assert_eq!(h.get(header::WWW_AUTHENTICATE).unwrap(), "Bearer");
    for bad in ["Bearer wrong", "Bearer s3cret2", "Basic s3cret", "s3cret"] {
        let mut r = post("/v1/rest/mgmt", json!({"csl": ".show tables"}));
        r.headers_mut().insert(header::AUTHORIZATION, bad.parse().unwrap());
        assert_eq!(send(&app, r).await.0, StatusCode::UNAUTHORIZED, "{bad}");
    }
    let mut r = post("/v2/rest/query", body);
    r.headers_mut().insert(header::AUTHORIZATION, "Bearer s3cret".parse().unwrap());
    assert_eq!(send(&app, r).await.0, StatusCode::OK);
    // metadata endpoints the SDKs fetch before authenticating stay public
    let (s, _, _) = send(&app, Request::get("/v1/rest/auth/metadata").body(Body::empty()).unwrap()).await;
    assert_eq!(s, StatusCode::OK);
    let (s, _, _) = send(&app, Request::get("/v1/rest/metadata/StormEvents").body(Body::empty()).unwrap()).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn cors_absent_by_default_and_allowlisted_when_configured() {
    let req = || {
        let mut r = post("/v2/rest/query", json!({"csl": "print 1"}));
        r.headers_mut().insert(header::ORIGIN, "https://evil.example".parse().unwrap());
        r
    };
    let (_, h, _) = send(&app(), req()).await;
    assert!(h.get(header::ACCESS_CONTROL_ALLOW_ORIGIN).is_none());

    let app = app_with(
        ServerConfig {
            cors_origins: vec!["https://good.example".into()],
            token: Some("t".into()),
            ..Default::default()
        },
        false,
    );
    let (_, h, _) = send(&app, req()).await;
    assert!(h.get(header::ACCESS_CONTROL_ALLOW_ORIGIN).is_none());
    // preflight from the allowed origin succeeds without a token
    let pre = Request::builder()
        .method("OPTIONS")
        .uri("/v2/rest/query")
        .header(header::ORIGIN, "https://good.example")
        .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
        .header(header::ACCESS_CONTROL_REQUEST_HEADERS, "authorization,content-type")
        .body(Body::empty())
        .unwrap();
    let (s, h, _) = send(&app, pre).await;
    assert!(s.is_success(), "{s}");
    assert_eq!(h.get(header::ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(), "https://good.example");
}

#[tokio::test]
async fn row_limit_truncates_with_partial_failure() {
    let app = app_with(ServerConfig { max_rows: 100, ..Default::default() }, false);
    let (s, v) = call(&app, "/v2/rest/query", json!({"csl": "Big"})).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(frame(&v, "PrimaryResult")["Rows"].as_array().unwrap().len(), 100);
    let done = v.as_array().unwrap().last().unwrap();
    assert_eq!(done["HasErrors"], true);
    assert!(done["OneApiErrors"][0]["error"]["@message"].as_str().unwrap().contains("E_QUERY_RESULT_SET_TOO_LARGE"));
    let qci = frame(&v, "QueryCompletionInformation");
    assert!(qci["Rows"].as_array().unwrap().iter().any(|r| r[6] == "Error"));

    let (_, v) = call(&app, "/v1/rest/query", json!({"csl": "Big"})).await;
    assert_eq!(v["Tables"][0]["Rows"].as_array().unwrap().len(), 100);
    assert_eq!(v["Tables"][1]["TableName"], "QueryStatus");
    assert!(v["Tables"][1]["Rows"][0][4].as_str().unwrap().contains("E_QUERY_RESULT_SET_TOO_LARGE"));

    // exactly at the limit: no error
    let (_, v) = call(&app, "/v2/rest/query", json!({"csl": "Big | take 100"})).await;
    assert_eq!(v.as_array().unwrap().last().unwrap()["HasErrors"], false);
    // the client can lower (not raise) the limit
    let body = json!({"csl": "Big", "properties": {"Options": {"truncationmaxrecords": 7}}});
    let (_, v) = call(&app, "/v1/rest/query", body).await;
    assert_eq!(v["Tables"][0]["Rows"].as_array().unwrap().len(), 7);
}

#[tokio::test]
async fn timeout_interrupts_the_query() {
    let app = app_with(ServerConfig { timeout: Duration::from_secs(1), ..Default::default() }, false);
    let kql = "range a from 1 to 3000000000 step 1 | summarize dcount(strcat(tostring(a), 'x'))";
    let start = std::time::Instant::now();
    let (s, v) = call(&app, "/v1/rest/query", json!({"csl": kql})).await;
    assert!(start.elapsed() < Duration::from_secs(20), "took {:?}", start.elapsed());
    assert_eq!(s, StatusCode::GATEWAY_TIMEOUT, "{v}");
    assert_eq!(v["error"]["code"], "Request_ExecutionTimeout");
}

/// Port of QueryApiTests.MetadataReturnsTables / AuthMetadataShapeMatchesExpected.
#[tokio::test]
async fn metadata_ping_and_auth_metadata() {
    let app = app();
    let (s, _, v) = send(&app, Request::get("/v1/rest/metadata/StormEvents").body(Body::empty()).unwrap()).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v[1]["Columns"][0]["ColumnName"], "TableName");
    let (s, _, v) = send(&app, Request::get("/v1/rest/auth/metadata").body(Body::empty()).unwrap()).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["AzureAD"]["LoginEndpoint"], "https://login.microsoftonline.com");
    assert_eq!(v["AzureAD"]["LoginMfaRequired"], false);
    assert_eq!(v["AzureAD"]["KustoClientAppId"], "db662dc1-0cfe-4e1c-a843-19a68e65be58");
    assert_eq!(v["dSTS"]["ServiceName"], "kusto");
    assert_eq!(v["AzureSettings"]["Classification"], "External");
    let (s, _, v) = send(&app, Request::get("/v1/rest/ping").body(Body::empty()).unwrap()).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["ApplicationHealthState"], "Healthy");
}
