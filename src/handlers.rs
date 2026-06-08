use axum::{Json, extract::State, http::StatusCode};
use sqlx::PgPool;

use crate::models::{CreateEventRequest, EventResponse, validate_request};

pub async fn root_handler() -> (StatusCode, &'static str) {
    (StatusCode::OK, "OK")
}

pub async fn health_handler() -> (StatusCode, &'static str) {
    (StatusCode::OK, "process is alive")
}

pub async fn ready_handler(
    State(pool): State<PgPool>,
) -> Result<(StatusCode, &'static str), (StatusCode, &'static str)> {
    sqlx::query("SELECT 1")
        .execute(&pool)
        .await
        .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "database unreachable"))?;

    Ok((StatusCode::OK, "process can reach Postgres"))
}

pub async fn events_handler(
    Json(payload): Json<CreateEventRequest>,
) -> (StatusCode, Json<EventResponse>) {
    if let Err(err) = validate_request(&payload) {
        return (
            StatusCode::BAD_REQUEST,
            Json(EventResponse {
                event_id: None,
                status: "failed".to_string(),
                message: err.message().to_string(),
                received_at: None,
            }),
        );
    }

    (
        StatusCode::ACCEPTED,
        Json(EventResponse {
            event_id: Some("123".to_string()),
            status: "accepted".to_string(),
            message: format!("{} event received", payload.event_type.as_str()),
            received_at: Some("123".to_string()),
        }),
    )
}
