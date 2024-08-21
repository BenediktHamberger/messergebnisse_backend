use axum::{http::Method, routing::post, Router};

#[cfg(not(debug_assertions))]
use axum_server::tls_rustls::RustlsConfig;

use http::header::CONTENT_TYPE;

use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};

use std::error::Error;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;

mod betaetigungsscheibe;
mod messergebnisse;
mod query;
mod zugkugelkupplung;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct InpFilter {
    id: String,
    operator: String,
    value: Option<String>,
    lower: Option<String>,
    upper: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryInput {
    filters: Vec<InpFilter>,
}

pub struct ServerState {
    pool: Arc<Mutex<deadpool_tiberius::Pool>>,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = deadpool_tiberius::Manager::new()
        .host("deegg-workv") // default to localhost
        .authentication(tiberius::AuthMethod::sql_server(
            "atlascopco_ro",
            "f6e5d4c3b2a1",
        ))
        .database("pruefergebnisse")
        .trust_cert()
        .max_size(10)
        .wait_timeout(10.0) // in seconds, default to no timeout
        .pre_recycle_sync(|_client, _metrics| Ok(()))
        .create_pool()?;

    let state = Arc::new(Mutex::new(ServerState {
        pool: Arc::new(Mutex::new(pool)),
    }));

    let cors = CorsLayer::new()
        .allow_methods([
            Method::GET,
            Method::HEAD,
            Method::POST,
            Method::OPTIONS,
            Method::PUT,
        ])
        .allow_origin(Any)
        .allow_headers([CONTENT_TYPE]);

    let app = Router::new()
        .route(
            "/query_messergebnisse",
            post(messergebnisse::messergebnisse),
        )
        .route(
            "/query_zugkugelkupplung",
            post(zugkugelkupplung::zugkugelkupplung),
        )
        .route(
            "/query_betaetigungsscheibe",
            post(betaetigungsscheibe::betaetigungsscheibe),
        )
        .layer(cors)
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3001));

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
    Ok(())
}
