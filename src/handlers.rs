use axum::{Json, extract::State, http::StatusCode};
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

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
    State(pool): State<PgPool>,
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

    let insert_result = sqlx::query_as::<_, (Uuid, DateTime<Utc>)>(
        r#"
        INSERT INTO events (
            producer_id,
            event_type,
            schema_version,
            message
        )
        VALUES ($1, $2, $3, $4)
        RETURNING id, received_at
        "#,
    )
    .bind(payload.producer_id.trim())
    .bind(payload.event_type.as_str())
    .bind(payload.schema_version as i32)
    .bind(&payload.message)
    .fetch_one(&pool)
    .await;

    let (event_id, received_at) = match insert_result {
        Ok(inserted_event) => inserted_event,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(EventResponse {
                    event_id: None,
                    status: "failed".to_string(),
                    message: "failed to store event".to_string(),
                    received_at: None,
                }),
            );
        }
    };

    (
        StatusCode::ACCEPTED,
        Json(EventResponse {
            event_id: Some(event_id),
            status: "accepted".to_string(),
            message: format!("{} event received", payload.event_type.as_str()),
            received_at: Some(received_at),
        }),
    )
}
