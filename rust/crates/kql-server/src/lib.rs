//! Kusto-compatible HTTP API over DuckDB.
//!
//! Endpoints (see README.md): `POST /v1/rest/query`, `POST /v2/rest/query`, `POST /v1/rest/mgmt`,
//! `GET /v1/rest/metadata/{db}`, `GET /v1/rest/ping`, `GET /v1/rest/auth/metadata`.
//! [`router`] builds the axum application; `main.rs` only parses arguments and serves it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use kql_to_sql::{is_command, translate, translate_command, Column, Dialect};
use serde_json::{json, Value as Json};
use tokio::sync::Semaphore;

pub mod db;
pub mod error;
mod mgmt;
pub mod protocol;
pub mod values;

pub use db::{Database, DbOptions, Limits};
pub use error::ApiError;

/// Server behavior (everything except the bind address and the database).
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// When set, every endpoint except ping/auth-metadata requires `Authorization: Bearer <token>`.
    pub token: Option<String>,
    /// Origins allowed by CORS; empty = no CORS headers at all.
    pub cors_origins: Vec<String>,
    /// Allow management commands that change the database (otherwise only `.show`).
    pub allow_commands: bool,
    pub read_only: bool,
    pub max_rows: usize,
    pub max_result_bytes: usize,
    pub max_body_bytes: usize,
    pub timeout: Duration,
    pub max_concurrent: usize,
    /// Database name reported when the request does not name one.
    pub database_name: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            token: None,
            cors_origins: Vec::new(),
            allow_commands: false,
            read_only: false,
            max_rows: 500_000,
            max_result_bytes: 64 * 1024 * 1024,
            max_body_bytes: 16 * 1024 * 1024,
            timeout: Duration::from_secs(60),
            max_concurrent: 8,
            database_name: "NetDefaultDB".into(),
        }
    }
}

pub struct AppState {
    pub config: ServerConfig,
    pub db: Database,
    permits: Semaphore,
}

pub type SharedState = Arc<AppState>;

impl AppState {
    pub fn new(config: ServerConfig, db: Database) -> SharedState {
        let permits = Semaphore::new(config.max_concurrent.max(1));
        Arc::new(AppState { config, db, permits })
    }
}

/// Builds the application.
pub fn router(state: SharedState) -> Router {
    let protected = Router::new()
        .route("/v1/rest/query", post(v1_query))
        .route("/v2/rest/query", post(v2_query))
        .route("/v1/rest/mgmt", post(v1_mgmt))
        .route("/v1/rest/metadata/{db}", get(metadata))
        .route_layer(middleware::from_fn_with_state(state.clone(), auth));
    // Clients fetch these before authenticating.
    let public = Router::new().route("/v1/rest/ping", get(ping)).route("/v1/rest/auth/metadata", get(auth_metadata));
    let mut app = protected
        .merge(public)
        .fallback(|| async { ApiError::not_found("no such endpoint") })
        .layer(DefaultBodyLimit::max(state.config.max_body_bytes))
        .with_state(state.clone());
    if !state.config.cors_origins.is_empty() {
        let origins: Vec<HeaderValue> =
            state.config.cors_origins.iter().filter_map(|o| HeaderValue::from_str(o).ok()).collect();
        let cors = tower_http::cors::CorsLayer::new()
            .allow_origin(tower_http::cors::AllowOrigin::list(origins))
            .allow_methods([Method::GET, Method::POST])
            .allow_headers(tower_http::cors::AllowHeaders::mirror_request())
            .max_age(Duration::from_secs(600));
        app = app.layer(cors);
    }
    app
}

// ---------------------------------------------------------------------------------------------
// auth

/// Constant-time byte comparison (the length is not secret).
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

async fn auth(State(state): State<SharedState>, req: Request, next: Next) -> Response {
    if let Some(token) = &state.config.token {
        let ok = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer ")))
            .map(|t| ct_eq(t.trim().as_bytes(), token.as_bytes()))
            .unwrap_or(false);
        if !ok {
            let mut r = ApiError::unauthorized().into_response();
            r.headers_mut().insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
            return r;
        }
    }
    next.run(req).await
}

// ---------------------------------------------------------------------------------------------
// request parsing

/// The request body (`Models/QueryRequest` of the C# server): `csl`, `db`, `properties`.
#[derive(Debug, Default)]
pub struct QueryRequest {
    pub csl: String,
    pub db: Option<String>,
    /// `properties` (an object, or a JSON string as some SDKs send it).
    pub properties: Json,
}

fn field<'a>(o: &'a serde_json::Map<String, Json>, name: &str) -> Option<&'a Json> {
    o.get(name).or_else(|| o.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v))
}

impl QueryRequest {
    pub fn parse(body: &[u8]) -> Result<QueryRequest, ApiError> {
        let v: Json =
            serde_json::from_slice(body).map_err(|e| ApiError::bad_request(format!("invalid request body: {e}")))?;
        let o = v.as_object().ok_or_else(|| ApiError::bad_request("invalid request body: expected a JSON object"))?;
        let csl = match field(o, "csl") {
            Some(Json::String(s)) => s.clone(),
            _ => return Err(ApiError::bad_request("invalid request body: 'csl' (string) is required")),
        };
        let db = field(o, "db").and_then(Json::as_str).map(str::to_string);
        let properties = match field(o, "properties") {
            Some(Json::String(s)) if !s.trim().is_empty() => serde_json::from_str(s).unwrap_or(Json::Null),
            Some(p @ Json::Object(_)) => p.clone(),
            _ => Json::Null,
        };
        Ok(QueryRequest { csl, db, properties })
    }

    fn option(&self, name: &str) -> Option<&Json> {
        let opts = self.properties.as_object().and_then(|p| field(p, "Options"))?.as_object()?;
        field(opts, name)
    }

    fn option_bool(&self, name: &str) -> bool {
        match self.option(name) {
            Some(Json::Bool(b)) => *b,
            Some(Json::String(s)) => s.eq_ignore_ascii_case("true"),
            _ => false,
        }
    }

    fn option_u64(&self, name: &str) -> Option<u64> {
        match self.option(name)? {
            Json::Number(n) => n.as_u64(),
            Json::String(s) => s.parse().ok(),
            _ => None,
        }
    }
}

fn json_response(v: &Json) -> Response {
    let mut r = (StatusCode::OK, v.to_string()).into_response();
    r.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json; charset=utf-8"));
    r
}

fn reply(r: Result<Json, ApiError>) -> Response {
    match r {
        Ok(v) => json_response(&v),
        Err(e) => e.into_response(),
    }
}

// ---------------------------------------------------------------------------------------------
// execution

impl AppState {
    fn limits(&self, req: &QueryRequest) -> Limits {
        let mut max_rows = self.config.max_rows;
        if let Some(n) = req.option_u64("truncationmaxrecords") {
            max_rows = max_rows.min(n as usize);
        }
        let mut max_bytes = self.config.max_result_bytes;
        if let Some(n) = req.option_u64("truncationmaxsize") {
            max_bytes = max_bytes.min(n as usize);
        }
        Limits { max_rows, max_bytes, timeout: self.config.timeout }
    }

    /// Runs `work` on a fresh connection in the blocking pool, bounded by the timeout (which
    /// interrupts the running DuckDB query) and the concurrency limit.
    async fn run<T: Send + 'static>(
        self: &Arc<Self>,
        work: impl FnOnce(&duckdb::Connection, Instant) -> Result<T, ApiError> + Send + 'static,
    ) -> Result<T, ApiError> {
        let _permit = self.permits.acquire().await.map_err(|_| ApiError::engine("server is shutting down"))?;
        let conn = self.db.connect()?;
        let interrupt = conn.interrupt_handle();
        let timeout = self.config.timeout;
        let deadline = Instant::now() + timeout;
        let mut task = tokio::task::spawn_blocking(move || work(&conn, deadline));
        match tokio::time::timeout(timeout, &mut task).await {
            Ok(r) => r.map_err(|e| ApiError::engine(format!("query task failed: {e}")))?,
            Err(_) => {
                interrupt.interrupt();
                let _ = task.await;
                Err(ApiError::timeout())
            }
        }
    }

    async fn query(
        self: &Arc<Self>,
        req: &QueryRequest,
    ) -> Result<(db::ResultSet, Option<kql_to_sql::RenderInfo>), ApiError> {
        if is_command(&req.csl) {
            return Err(ApiError::bad_request(
                "management commands are not allowed on the query endpoint; send them to /v1/rest/mgmt",
            ));
        }
        let catalog = self.db.catalog()?;
        let t = translate(&req.csl, &catalog, Dialect::DuckDb).map_err(|e| ApiError::translation(e.message))?;
        let limits = self.limits(req);
        let (sql, cols) = (t.sql, t.columns);
        let rs = self.run(move |conn, deadline| db::query(conn, &sql, &cols, limits, deadline)).await?;
        Ok((rs, t.render))
    }
}

fn client_request_id(headers: &HeaderMap) -> String {
    headers.get("x-ms-client-request-id").and_then(|v| v.to_str().ok()).unwrap_or("").to_string()
}

async fn v1_query(State(state): State<SharedState>, body: Bytes) -> Response {
    let r = async {
        let req = QueryRequest::parse(&body)?;
        let (rs, _) = state.query(&req).await?;
        Ok(protocol::v1_query(rs))
    };
    reply(r.await)
}

async fn v2_query(State(state): State<SharedState>, headers: HeaderMap, body: Bytes) -> Response {
    let r = async {
        let req = QueryRequest::parse(&body)?;
        let (rs, render) = state.query(&req).await?;
        let progressive = req.option_bool("results_progressive_enabled");
        Ok(protocol::v2_query(rs, render.as_ref(), progressive, &client_request_id(&headers)))
    };
    reply(r.await)
}

async fn v1_mgmt(State(state): State<SharedState>, body: Bytes) -> Response {
    reply(mgmt(&state, &body).await)
}

async fn mgmt(state: &SharedState, body: &[u8]) -> Result<Json, ApiError> {
    let req = QueryRequest::parse(body)?;
    if !is_command(&req.csl) {
        return Err(ApiError::bad_request(
            "not a management command (commands start with '.'); send queries to /v1/rest/query",
        ));
    }
    let db_name = req.db.clone().filter(|d| !d.trim().is_empty()).unwrap_or_else(|| state.config.database_name.clone());
    if let Some(r) = mgmt::builtin_show(state, &req.csl, &db_name)? {
        return Ok(r);
    }
    let is_show = req.csl.trim_start().get(..5).is_some_and(|p| p.eq_ignore_ascii_case(".show"));
    if !is_show && !state.config.allow_commands {
        return Err(ApiError::forbidden(
            "this server only answers '.show' commands; start it with --allow-commands to enable commands that change the database",
        ));
    }
    let catalog = state.db.catalog()?;
    let cmd = translate_command(&req.csl, &catalog, Dialect::DuckDb).map_err(|e| ApiError::translation(e.message))?;
    let limits = state.limits(&req);
    let is_ingest = req.csl.trim_start().to_ascii_lowercase().starts_with(".ingest");
    let columns = cmd.columns.clone();
    let statements = cmd.statements;
    let result = state
        .run(move |conn, deadline| {
            if !columns.is_empty() {
                // a `.show` command: one SELECT
                let sql = statements.last().cloned().unwrap_or_default();
                return db::query(conn, &sql, &columns, limits, deadline).map(Some);
            }
            conn.execute_batch("BEGIN TRANSACTION").map_err(|e| ApiError::engine(e.to_string()))?;
            for s in &statements {
                if let Err(e) = conn.execute_batch(s) {
                    let _ = conn.execute_batch("ROLLBACK");
                    return Err(ApiError::engine(e.to_string()));
                }
            }
            conn.execute_batch("COMMIT").map_err(|e| ApiError::engine(e.to_string()))?;
            Ok(None)
        })
        .await;
    if !is_show {
        state.db.invalidate();
    }
    Ok(match result? {
        Some(rs) => json!({"Tables": [protocol::v1_table("Table_0", &rs.columns, rs.rows)]}),
        None if is_ingest => {
            json!({"Tables": [protocol::v1_string_table("IngestionStatus", &["Status"], vec![vec![json!("Completed")]])]})
        }
        None => json!({"Tables": []}),
    })
}

/// `GET /v1/rest/metadata/{db}`: every column of every table, as a v2 frame array.
async fn metadata(State(state): State<SharedState>) -> Response {
    let r = (|| {
        let cat = state.db.catalog()?;
        let mut rows = Vec::new();
        for (t, cols) in &cat.tables {
            for c in cols {
                rows.push(json!([t, c.name, c.ty.name()]));
            }
        }
        let cols: Vec<Column> = ["TableName", "ColumnName", "DataType"]
            .iter()
            .map(|n| Column::new(*n, kql_to_sql::KqlType::String))
            .collect();
        Ok(json!([
            {"FrameType": "DataSetHeader", "Version": "v2.0", "IsProgressive": false},
            {"FrameType": "DataTable", "TableId": 0, "TableKind": "PrimaryResult", "TableName": "Metadata", "Columns": protocol::v2_columns(&cols), "Rows": rows},
            {"FrameType": "DataSetCompletion", "HasErrors": false, "Cancelled": false, "OneApiErrors": null}
        ]))
    })();
    reply(r)
}

async fn ping() -> Response {
    // the client address is not echoed (no information disclosure)
    json_response(&json!({"ApplicationHealthState": "Healthy"}))
}

/// Cloud metadata the Kusto SDKs fetch before connecting (public-cloud shape; the server itself
/// never talks to AAD — see `--token`).
async fn auth_metadata() -> Response {
    json_response(&json!({
        "AzureAD": {
            "LoginEndpoint": "https://login.microsoftonline.com",
            "LoginMfaRequired": false,
            "KustoClientAppId": "db662dc1-0cfe-4e1c-a843-19a68e65be58",
            "KustoClientRedirectUri": "http://localhost",
            "KustoServiceResourceId": "https://kusto.kusto.windows.net",
            "FirstPartyAuthorityUrl": "https://login.microsoftonline.com/f8cdef31-a31e-4b4a-93e4-5f571e91255a"
        },
        "dSTS": {
            "CloudEndpointSuffix": "windows.net",
            "DstsRealm": "realm://dsts.core.windows.net",
            "DstsInstance": "prod-dsts.dsts.core.windows.net",
            "KustoDnsHostName": "kusto.windows.net",
            "ServiceName": "kusto",
            "KustoDstsServiceId": "4d248be5-f7bb-4cb0-95b6-36fb9e4f97a8",
            "DstsJWTAuthorityAddress": "https://prod-passive-dsts.dsts.core.windows.net/dstsv2/7a433bfc-2514-4697-b467-e0933190487f"
        },
        "AzureSettings": {"CloudName": "PublicCloud", "AzureRegion": "West Europe", "Classification": "External"}
    }))
}
