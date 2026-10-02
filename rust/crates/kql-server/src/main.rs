//! `kql-server`: a Kusto-compatible HTTP API over DuckDB.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use kql_server::{router, AppState, Database, DbOptions, ServerConfig};

#[derive(Parser, Debug)]
#[command(name = "kql-server", version, about = "Kusto-compatible HTTP API (v1/v2 REST) over DuckDB")]
struct Args {
    /// Address to listen on. The default only accepts local connections.
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: SocketAddr,

    /// DuckDB database file (default: in-memory).
    #[arg(long)]
    db: Option<PathBuf>,

    /// Open --db read-only.
    #[arg(long)]
    read_only: bool,

    /// Create table StormEvents (Kusto's schema) from this StormEvents.csv.gz at startup.
    #[arg(long, value_name = "CSV")]
    load_storm_events: Option<PathBuf>,

    /// Require `Authorization: Bearer <TOKEN>` on every query/management endpoint.
    #[arg(long, env = "KQL_SERVER_TOKEN", hide_env_values = true)]
    token: Option<String>,

    /// Allow cross-origin browser requests from this origin (repeatable). Default: no CORS.
    #[arg(long = "cors-origin", value_name = "ORIGIN")]
    cors_origins: Vec<String>,

    /// Allow management commands that change the database (.create, .ingest inline, .set-or-append, .drop, ...).
    /// Without it only `.show` commands are answered.
    #[arg(long)]
    allow_commands: bool,

    /// Maximum rows returned per query; more rows yield a partial-failure E_QUERY_RESULT_SET_TOO_LARGE.
    #[arg(long, default_value_t = 500_000)]
    max_rows: usize,

    /// Maximum (approximate) result size in bytes per query.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    max_result_bytes: usize,

    /// Maximum request body size in bytes.
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    max_body_bytes: usize,

    /// Per-request execution timeout in seconds (the running query is interrupted).
    #[arg(long, default_value_t = 60)]
    timeout_secs: u64,

    /// Maximum number of queries executing at the same time.
    #[arg(long, default_value_t = 8)]
    max_concurrent: usize,

    /// DuckDB memory limit, e.g. "4GB".
    #[arg(long)]
    memory_limit: Option<String>,

    /// DuckDB worker threads.
    #[arg(long)]
    threads: Option<u32>,

    /// Database name reported to clients when the request does not name one.
    #[arg(long, default_value = "NetDefaultDB")]
    database_name: String,
}

#[tokio::main]
async fn main() {
    let a = Args::parse();
    let token = a.token.filter(|t| !t.is_empty());
    if token.is_none() && !a.bind.ip().is_loopback() {
        eprintln!(
            "warning: listening on {} without --token: anyone who can reach this address can query the database",
            a.bind
        );
    }
    let db = Database::open(&DbOptions {
        path: a.db,
        read_only: a.read_only,
        storm_events_csv: a.load_storm_events,
        memory_limit: a.memory_limit,
        threads: a.threads,
    })
    .unwrap_or_else(|e| {
        eprintln!("error: cannot open the database: {e}");
        std::process::exit(1);
    });
    let config = ServerConfig {
        token,
        cors_origins: a.cors_origins,
        allow_commands: a.allow_commands,
        read_only: a.read_only,
        max_rows: a.max_rows,
        max_result_bytes: a.max_result_bytes,
        max_body_bytes: a.max_body_bytes,
        timeout: Duration::from_secs(a.timeout_secs.max(1)),
        max_concurrent: a.max_concurrent,
        database_name: a.database_name,
    };
    let app = router(AppState::new(config, db));
    let listener = tokio::net::TcpListener::bind(a.bind).await.unwrap_or_else(|e| {
        eprintln!("error: cannot listen on {}: {e}", a.bind);
        std::process::exit(1);
    });
    eprintln!("kql-server listening on http://{}", a.bind);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .unwrap_or_else(|e| eprintln!("error: {e}"));
}
