use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::models::{
    CreateEventRequest, ErrorResponse, EventResponse, EventStatusResponse, StoredEventResponse,
    request_fingerprint, validate_request,
};

type ApiError = (StatusCode, Json<ErrorResponse>);
type IdempotencyLookup = (Uuid, Option<String>, String, DateTime<Utc>);

fn error_response(status_code: StatusCode, status: &str, message: &str) -> ApiError {
    (status_code, Json(ErrorResponse::new(status, message)))
}

fn parse_event_id(event_id: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(event_id).map_err(|_| {
        error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "event_id must be a valid UUID",
        )
    })
}

async fn find_event_by_idempotency_key(
    pool: &PgPool,
    producer_id: &str,
    idempotency_key: &str,
) -> Result<Option<IdempotencyLookup>, sqlx::Error> {
    sqlx::query_as::<_, IdempotencyLookup>(
        r#"
        SELECT
            id,
            request_fingerprint,
            status,
            received_at
        FROM events
        WHERE producer_id = $1
            AND idempotency_key = $2
        "#,
    )
    .bind(producer_id)
    .bind(idempotency_key)
    .fetch_optional(pool)
    .await
}

fn idempotency_response(
    existing_event: IdempotencyLookup,
    request_fingerprint: &str,
    event_type: &str,
) -> Result<(StatusCode, Json<EventResponse>), ApiError> {
    let (event_id, existing_fingerprint, status, received_at) = existing_event;

    if existing_fingerprint.as_deref() != Some(request_fingerprint) {
        return Err(error_response(
            StatusCode::CONFLICT,
            "conflict",
            "idempotency_key already used with a different request body",
        ));
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(EventResponse {
            event_id: Some(event_id),
            status,
            message: format!("{event_type} event received"),
            received_at: Some(received_at),
        }),
    ))
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    err.as_database_error()
        .and_then(|db_err| db_err.code())
        .as_deref()
        == Some("23505")
}

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
) -> Result<(StatusCode, Json<EventResponse>), ApiError> {
    if let Err(err) = validate_request(&payload) {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            err.message(),
        ));
    }

    let fingerprint = request_fingerprint(&payload);
    let producer_id = payload.producer_id.trim();
    let idempotency_key = payload.idempotency_key.trim();
    let event_type = payload.event_type.as_str();

    let existing_event = find_event_by_idempotency_key(&pool, producer_id, idempotency_key)
        .await
        .map_err(|_| {
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed",
                "failed to check idempotency key",
            )
        })?;

    if let Some(existing_event) = existing_event {
        return idempotency_response(existing_event, &fingerprint, event_type);
    }

    let insert_result = sqlx::query_as::<_, (Uuid, DateTime<Utc>)>(
        r#"
        INSERT INTO events (
            idempotency_key,
            request_fingerprint,
            producer_id,
            event_type,
            schema_version,
            message
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id, received_at
        "#,
    )
    .bind(idempotency_key)
    .bind(&fingerprint)
    .bind(producer_id)
    .bind(event_type)
    .bind(payload.schema_version as i32)
    .bind(&payload.message)
    .fetch_one(&pool)
    .await;

    let (event_id, received_at) = match insert_result {
        Ok(inserted_event) => inserted_event,
        Err(err) => {
            if is_unique_violation(&err) {
                let existing_event =
                    find_event_by_idempotency_key(&pool, producer_id, idempotency_key)
                        .await
                        .map_err(|_| {
                            error_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "failed",
                                "failed to check idempotency key",
                            )
                        })?;

                if let Some(existing_event) = existing_event {
                    return idempotency_response(existing_event, &fingerprint, event_type);
                }
            }

            return Err(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed",
                "failed to store event",
            ));
        }
    };

    Ok((
        StatusCode::ACCEPTED,
        Json(EventResponse {
            event_id: Some(event_id),
            status: "accepted".to_string(),
            message: format!("{event_type} event received"),
            received_at: Some(received_at),
        }),
    ))
}

pub async fn get_event_handler(
    State(pool): State<PgPool>,
    Path(event_id): Path<String>,
) -> Result<(StatusCode, Json<StoredEventResponse>), ApiError> {
    let event_id = parse_event_id(&event_id)?;

    let lookup_result = sqlx::query_as::<
        _,
        (
            Uuid,
            String,
            Option<String>,
            String,
            String,
            i32,
            String,
            String,
            DateTime<Utc>,
        ),
    >(
        r#"
        SELECT
            id,
            idempotency_key,
            request_fingerprint,
            producer_id,
            event_type,
            schema_version,
            message,
            status,
            received_at
        FROM events
        WHERE id = $1
        "#,
    )
    .bind(event_id)
    .fetch_optional(&pool)
    .await;

    match lookup_result {
        Ok(Some((
            event_id,
            idempotency_key,
            request_fingerprint,
            producer_id,
            event_type,
            schema_version,
            message,
            status,
            received_at,
        ))) => Ok((
            StatusCode::OK,
            Json(StoredEventResponse {
                event_id,
                idempotency_key,
                request_fingerprint,
                producer_id,
                event_type,
                schema_version,
                message,
                status,
                received_at,
            }),
        )),
        Ok(None) => Err(error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            "event not found",
        )),
        Err(_) => Err(error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed",
            "failed to fetch event",
        )),
    }
}

pub async fn get_event_status_handler(
    State(pool): State<PgPool>,
    Path(event_id): Path<String>,
) -> Result<(StatusCode, Json<EventStatusResponse>), ApiError> {
    let event_id = parse_event_id(&event_id)?;

    let lookup_result = sqlx::query_as::<_, (Uuid, String, DateTime<Utc>)>(
        r#"
        SELECT
            id,
            status,
            received_at
        FROM events
        WHERE id = $1
        "#,
    )
    .bind(event_id)
    .fetch_optional(&pool)
    .await;

    match lookup_result {
        Ok(Some((event_id, status, received_at))) => Ok((
            StatusCode::OK,
            Json(EventStatusResponse {
                event_id,
                status,
                received_at,
            }),
        )),
        Ok(None) => Err(error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            "event not found",
        )),
        Err(_) => Err(error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed",
            "failed to fetch event status",
        )),
    }
}
