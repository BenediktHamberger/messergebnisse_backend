use axum::{extract::State, Json};
use http::StatusCode;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::query::{Filter, SqlType};
use crate::ServerState;

pub async fn messergebnisse(
    State(state): State<Arc<Mutex<ServerState>>>,
    Json(payload): Json<crate::QueryInput>,
) -> Result<Json<Vec<Vec<serde_json::Value>>>, (StatusCode, String)> {
    let state = state.lock().await;
    let pool = state.pool.lock().await;

    let mut conn = pool
        .get()
        .await
        .map_err(|x| (StatusCode::INTERNAL_SERVER_ERROR, x.to_string()))?;

    let filters: Vec<Filter> = payload
        .filters
        .into_iter()
        .map(Filter::from)
        .collect::<Vec<Filter>>();

   

    let mut vec_filters = vec![];

    _ = build_filters(filters, &mut vec_filters);

    let str_filters = match vec_filters.is_empty() {
        true => "".to_owned(),
        false => "WHERE ".to_owned() + &vec_filters.join(" AND "),
    };

    let query = format!("SELECT TOP 500 * FROM [pruefergebnisse].[dbo].[pp_messergebnisse] {} ORDER BY SerienNrTemporaer;", str_filters);
    let mut res: Vec<Vec<Value>> = vec![];

    crate::query::actual_query(query, &mut conn, &mut res).await?;

    Ok(Json(res))
}

use crate::query::GetId;

pub fn build_filters(filters: Vec<Filter>, vec_filters: &mut Vec<String>) -> Result<(), String> {
    for filter in filters.into_iter() {
        let id = filter.get_id();
        let sql_type = match id.as_str() {
            "chtime" => SqlType::SqlDate,
            "BremseNr" | "PKey" => SqlType::SqlNumber,
            _ => SqlType::SqlString,
        };
        crate::query::match_operator(filter, sql_type, vec_filters);

        // match_operator(fil, vals, filter.operator, vec_filters);
    }
    Ok(())
}
