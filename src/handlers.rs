use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::models::{
    ClaimEventRequest, ClaimEventResponse, CompleteEventRequest, CreateEventRequest, ErrorResponse,
    EventResponse, EventStatus, EventStatusResponse, EventSummaryResponse, ListEventsQuery,
    ListStaleEventsQuery, RequeueEventRequest, StoredEventResponse, UpdateEventStatusRequest,
    is_valid_status_transition, request_fingerprint, validate_request, validate_worker_id,
};

type ApiError = (StatusCode, Json<ErrorResponse>);
type IdempotencyLookup = (Uuid, Option<String>, String, DateTime<Utc>);
const MAX_STALE_LOOKBACK_SECONDS: i64 = 86_400;

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

fn validate_stale_threshold(older_than_seconds: i64) -> Result<(), ApiError> {
    if older_than_seconds <= 0 || older_than_seconds > MAX_STALE_LOOKBACK_SECONDS {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "older_than_seconds must be between 1 and 86400",
        ));
    }

    Ok(())
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

pub async fn list_events_handler(
    State(pool): State<PgPool>,
    Query(query): Query<ListEventsQuery>,
) -> Result<Json<Vec<EventSummaryResponse>>, ApiError> {
    let status = EventStatus::parse(query.status.trim()).ok_or_else(|| {
        error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "status must be one of: accepted, processing, processed, failed",
        )
    })?;

    let events = sqlx::query_as::<
        _,
        (
            Uuid,
            String,
            String,
            Option<String>,
            Option<DateTime<Utc>>,
            DateTime<Utc>,
        ),
    >(
        r#"
        SELECT
            id,
            event_type,
            status,
            locked_by,
            locked_at,
            received_at
        FROM events
        WHERE status = $1
        ORDER BY received_at ASC
        LIMIT 20
        "#,
    )
    .bind(status.as_str())
    .fetch_all(&pool)
    .await
    .map_err(|_| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed",
            "failed to list events",
        )
    })?;

    Ok(Json(
        events
            .into_iter()
            .map(
                |(event_id, event_type, status, locked_by, locked_at, received_at)| {
                    EventSummaryResponse {
                        event_id,
                        event_type,
                        status,
                        locked_by,
                        locked_at,
                        received_at,
                    }
                },
            )
            .collect(),
    ))
}

pub async fn list_stale_events_handler(
    State(pool): State<PgPool>,
    Query(query): Query<ListStaleEventsQuery>,
) -> Result<Json<Vec<EventSummaryResponse>>, ApiError> {
    validate_stale_threshold(query.older_than_seconds)?;

    let events = sqlx::query_as::<
        _,
        (
            Uuid,
            String,
            String,
            Option<String>,
            Option<DateTime<Utc>>,
            DateTime<Utc>,
        ),
    >(
        r#"
        SELECT
            id,
            event_type,
            status,
            locked_by,
            locked_at,
            received_at
        FROM events
        WHERE status = 'processing'
            AND locked_at IS NOT NULL
            AND locked_at < now() - ($1::bigint * INTERVAL '1 second')
        ORDER BY locked_at ASC
        LIMIT 20
        "#,
    )
    .bind(query.older_than_seconds)
    .fetch_all(&pool)
    .await
    .map_err(|_| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed",
            "failed to list stale events",
        )
    })?;

    Ok(Json(
        events
            .into_iter()
            .map(
                |(event_id, event_type, status, locked_by, locked_at, received_at)| {
                    EventSummaryResponse {
                        event_id,
                        event_type,
                        status,
                        locked_by,
                        locked_at,
                        received_at,
                    }
                },
            )
            .collect(),
    ))
}

pub async fn requeue_event_handler(
    State(pool): State<PgPool>,
    Path(event_id): Path<String>,
    Json(payload): Json<RequeueEventRequest>,
) -> Result<(StatusCode, Json<EventStatusResponse>), ApiError> {
    let event_id = parse_event_id(&event_id)?;
    validate_stale_threshold(payload.older_than_seconds)?;

    let event_state_result = sqlx::query_as::<_, (String, Option<DateTime<Utc>>)>(
        r#"
        SELECT status, locked_at
        FROM events
        WHERE id = $1
        "#,
    )
    .bind(event_id)
    .fetch_optional(&pool)
    .await;

    let (current_status, locked_at) = match event_state_result {
        Ok(Some(row)) => row,
        Ok(None) => {
            return Err(error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                "event not found",
            ));
        }
        Err(_) => {
            return Err(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed",
                "failed to fetch event state",
            ));
        }
    };

    if current_status != EventStatus::Processing.as_str() {
        return Err(error_response(
            StatusCode::CONFLICT,
            "conflict",
            "event is not processing",
        ));
    }

    if locked_at.is_none() {
        return Err(error_response(
            StatusCode::CONFLICT,
            "conflict",
            "event is not locked",
        ));
    }

    let update_result = sqlx::query_as::<_, (Uuid, String, DateTime<Utc>)>(
        r#"
        UPDATE events
        SET status = 'accepted',
            locked_by = NULL,
            locked_at = NULL
        WHERE id = $1
            AND status = 'processing'
            AND locked_at IS NOT NULL
            AND locked_at < now() - ($2::bigint * INTERVAL '1 second')
        RETURNING id, status, received_at
        "#,
    )
    .bind(event_id)
    .bind(payload.older_than_seconds)
    .fetch_optional(&pool)
    .await;

    match update_result {
        Ok(Some((event_id, status, received_at))) => Ok((
            StatusCode::OK,
            Json(EventStatusResponse {
                event_id,
                status,
                received_at,
            }),
        )),
        Ok(None) => Err(error_response(
            StatusCode::CONFLICT,
            "conflict",
            "event is not stale enough to requeue",
        )),
        Err(_) => Err(error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed",
            "failed to requeue event",
        )),
    }
}

pub async fn claim_event_handler(
    State(pool): State<PgPool>,
    Json(payload): Json<ClaimEventRequest>,
) -> Result<Response, ApiError> {
    if let Err(err) = validate_worker_id(&payload.worker_id) {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            err.message(),
        ));
    }

    let worker_id = payload.worker_id.trim();

    let claim_result =
        sqlx::query_as::<_, (Uuid, String, String, String, DateTime<Utc>, DateTime<Utc>)>(
            r#"
        UPDATE events
        SET status = 'processing',
            locked_by = $1,
            locked_at = now()
        WHERE id = (
            SELECT id
            FROM events
            WHERE status = 'accepted'
            ORDER BY received_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, event_type, status, locked_by, locked_at, received_at
        "#,
        )
        .bind(worker_id)
        .fetch_optional(&pool)
        .await
        .map_err(|_| {
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed",
                "failed to claim event",
            )
        })?;

    match claim_result {
        Some((event_id, event_type, status, locked_by, locked_at, received_at)) => Ok((
            StatusCode::OK,
            Json(ClaimEventResponse {
                event_id,
                event_type,
                status,
                locked_by,
                locked_at,
                received_at,
            }),
        )
            .into_response()),
        None => Ok(StatusCode::NO_CONTENT.into_response()),
    }
}

pub async fn complete_event_handler(
    State(pool): State<PgPool>,
    Path(event_id): Path<String>,
    Json(payload): Json<CompleteEventRequest>,
) -> Result<(StatusCode, Json<EventStatusResponse>), ApiError> {
    let event_id = parse_event_id(&event_id)?;

    if let Err(err) = validate_worker_id(&payload.worker_id) {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            err.message(),
        ));
    }

    let requested_status = EventStatus::parse(payload.status.trim()).ok_or_else(|| {
        error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "status must be one of: processed, failed",
        )
    })?;

    let requested_status_value = match requested_status {
        EventStatus::Processed | EventStatus::Failed => requested_status.as_str(),
        EventStatus::Accepted | EventStatus::Processing => {
            return Err(error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "status must be one of: processed, failed",
            ));
        }
    };

    let worker_id = payload.worker_id.trim();

    let ownership_result = sqlx::query_as::<_, (String, Option<String>)>(
        r#"
        SELECT status, locked_by
        FROM events
        WHERE id = $1
        "#,
    )
    .bind(event_id)
    .fetch_optional(&pool)
    .await;

    let (current_status, locked_by) = match ownership_result {
        Ok(Some(row)) => row,
        Ok(None) => {
            return Err(error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                "event not found",
            ));
        }
        Err(_) => {
            return Err(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed",
                "failed to fetch event ownership",
            ));
        }
    };

    if current_status != EventStatus::Processing.as_str() {
        return Err(error_response(
            StatusCode::CONFLICT,
            "conflict",
            "event is not processing",
        ));
    }

    let Some(locked_by) = locked_by else {
        return Err(error_response(
            StatusCode::CONFLICT,
            "conflict",
            "event is not owned",
        ));
    };

    if locked_by != worker_id {
        return Err(error_response(
            StatusCode::CONFLICT,
            "conflict",
            "event owned by another worker",
        ));
    }

    let update_result = sqlx::query_as::<_, (Uuid, String, DateTime<Utc>)>(
        r#"
        UPDATE events
        SET status = $1,
            locked_by = NULL,
            locked_at = NULL,
            completed_by = $3,
            completed_at = now()
        WHERE id = $2
            AND status = 'processing'
            AND locked_by = $3
        RETURNING id, status, received_at
        "#,
    )
    .bind(requested_status_value)
    .bind(event_id)
    .bind(worker_id)
    .fetch_optional(&pool)
    .await;

    match update_result {
        Ok(Some((event_id, status, received_at))) => Ok((
            StatusCode::OK,
            Json(EventStatusResponse {
                event_id,
                status,
                received_at,
            }),
        )),
        Ok(None) => Err(error_response(
            StatusCode::CONFLICT,
            "conflict",
            "event ownership changed before completion",
        )),
        Err(_) => Err(error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed",
            "failed to complete event",
        )),
    }
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
            Option<String>,
            Option<DateTime<Utc>>,
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
            received_at,
            completed_by,
            completed_at
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
            completed_by,
            completed_at,
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
                completed_by,
                completed_at,
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

pub async fn update_event_status_handler(
    State(pool): State<PgPool>,
    Path(event_id): Path<String>,
    Json(payload): Json<UpdateEventStatusRequest>,
) -> Result<(StatusCode, Json<EventStatusResponse>), ApiError> {
    let event_id = parse_event_id(&event_id)?;
    let next_status = EventStatus::parse(payload.status.trim()).ok_or_else(|| {
        error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "status must be one of: accepted, processing, processed, failed",
        )
    })?;
    let next_status_value = next_status.as_str();

    let current_status_result = sqlx::query_as::<_, (String,)>(
        r#"
        SELECT status
        FROM events
        WHERE id = $1
        "#,
    )
    .bind(event_id)
    .fetch_optional(&pool)
    .await;

    let current_status = match current_status_result {
        Ok(Some((current_status,))) => current_status,
        Ok(None) => {
            return Err(error_response(
                StatusCode::NOT_FOUND,
                "not_found",
                "event not found",
            ));
        }
        Err(_) => {
            return Err(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed",
                "failed to fetch event status",
            ));
        }
    };

    if current_status == next_status_value {
        return Err(error_response(
            StatusCode::CONFLICT,
            "conflict",
            "event already has the requested status",
        ));
    }

    if !is_valid_status_transition(&current_status, &next_status) {
        return Err(error_response(
            StatusCode::CONFLICT,
            "conflict",
            "invalid status transition",
        ));
    }

    let update_result = sqlx::query_as::<_, (Uuid, String, DateTime<Utc>)>(
        r#"
        UPDATE events
        SET status = $1
        WHERE id = $2
        RETURNING id, status, received_at
        "#,
    )
    .bind(next_status_value)
    .bind(event_id)
    .fetch_one(&pool)
    .await;

    match update_result {
        Ok((event_id, status, received_at)) => Ok((
            StatusCode::OK,
            Json(EventStatusResponse {
                event_id,
                status,
                received_at,
            }),
        )),
        Err(_) => Err(error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed",
            "failed to update event status",
        )),
    }
}
