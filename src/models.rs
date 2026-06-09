use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const MAX_PRODUCER_ID_LEN: usize = 64;
const MAX_IDEMPOTENCY_KEY_LEN: usize = 128;
const MAX_WORKER_ID_LEN: usize = 64;
const MAX_MESSAGE_LEN: usize = 1000;
const SUPPORTED_SCHEMA_VERSION: u32 = 1;

const STATUS_ACCEPTED: &str = "accepted";
const STATUS_PROCESSING: &str = "processing";
const STATUS_PROCESSED: &str = "processed";
const STATUS_FAILED: &str = "failed";

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum EventType {
    #[serde(rename = "user.signup")]
    UserSignup,
    #[serde(rename = "payment.completed")]
    PaymentCompleted,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventStatus {
    Accepted,
    Processing,
    Processed,
    Failed,
}

impl EventStatus {
    pub fn parse(status: &str) -> Option<Self> {
        match status {
            STATUS_ACCEPTED => Some(Self::Accepted),
            STATUS_PROCESSING => Some(Self::Processing),
            STATUS_PROCESSED => Some(Self::Processed),
            STATUS_FAILED => Some(Self::Failed),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Accepted => STATUS_ACCEPTED,
            Self::Processing => STATUS_PROCESSING,
            Self::Processed => STATUS_PROCESSED,
            Self::Failed => STATUS_FAILED,
        }
    }
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::UserSignup => "user.signup",
            Self::PaymentCompleted => "payment.completed",
        }
    }
}

#[derive(Debug)]
pub enum ValidationError {
    InvalidProducerId,
    InvalidIdempotencyKey,
    InvalidWorkerId,
    UnsupportedSchemaVersion,
    EmptyMessage,
    MessageTooLong,
}

impl ValidationError {
    pub fn message(&self) -> &'static str {
        match self {
            Self::InvalidProducerId => "producer_id must be 1-64 non-whitespace characters",
            Self::InvalidIdempotencyKey => {
                "idempotency_key must be 1-128 non-whitespace characters"
            }
            Self::InvalidWorkerId => "worker_id must be 1-64 non-whitespace characters",
            Self::UnsupportedSchemaVersion => "unsupported schema_version",
            Self::EmptyMessage => "message must not be empty",
            Self::MessageTooLong => "message exceeds maximum length",
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct CreateEventRequest {
    pub producer_id: String,
    pub idempotency_key: String,
    pub event_type: EventType,
    pub schema_version: u32,
    pub message: String,
}

#[derive(Deserialize, Debug)]
pub struct UpdateEventStatusRequest {
    pub status: String,
}

#[derive(Deserialize, Debug)]
pub struct ListEventsQuery {
    pub status: String,
}

#[derive(Deserialize, Debug)]
pub struct ListStaleEventsQuery {
    pub older_than_seconds: i64,
}

#[derive(Deserialize, Debug)]
pub struct ClaimEventRequest {
    pub worker_id: String,
}

#[derive(Deserialize, Debug)]
pub struct CompleteEventRequest {
    pub worker_id: String,
    pub status: String,
}

#[derive(Serialize)]
pub struct EventResponse {
    pub event_id: Option<Uuid>,
    pub status: String,
    pub message: String,
    pub received_at: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
pub struct StoredEventResponse {
    pub event_id: Uuid,
    pub idempotency_key: String,
    pub request_fingerprint: Option<String>,
    pub producer_id: String,
    pub event_type: String,
    pub schema_version: i32,
    pub message: String,
    pub status: String,
    pub received_at: DateTime<Utc>,
}

#[derive(Serialize)]
pub struct EventStatusResponse {
    pub event_id: Uuid,
    pub status: String,
    pub received_at: DateTime<Utc>,
}

#[derive(Serialize)]
pub struct EventSummaryResponse {
    pub event_id: Uuid,
    pub event_type: String,
    pub status: String,
    pub locked_by: Option<String>,
    pub locked_at: Option<DateTime<Utc>>,
    pub received_at: DateTime<Utc>,
}

#[derive(Serialize)]
pub struct ClaimEventResponse {
    pub event_id: Uuid,
    pub event_type: String,
    pub status: String,
    pub locked_by: String,
    pub locked_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub status: String,
    pub message: String,
}

impl ErrorResponse {
    pub fn new(status: &str, message: &str) -> Self {
        Self {
            status: status.to_string(),
            message: message.to_string(),
        }
    }
}

pub fn validate_request(req: &CreateEventRequest) -> Result<(), ValidationError> {
    let producer_id = req.producer_id.trim();
    if producer_id.is_empty() || producer_id.len() > MAX_PRODUCER_ID_LEN {
        return Err(ValidationError::InvalidProducerId);
    }

    let idempotency_key = req.idempotency_key.trim();
    if idempotency_key.is_empty() || idempotency_key.len() > MAX_IDEMPOTENCY_KEY_LEN {
        return Err(ValidationError::InvalidIdempotencyKey);
    }

    if req.schema_version != SUPPORTED_SCHEMA_VERSION {
        return Err(ValidationError::UnsupportedSchemaVersion);
    }

    let message = req.message.trim();
    if message.is_empty() {
        return Err(ValidationError::EmptyMessage);
    }
    if message.len() > MAX_MESSAGE_LEN {
        return Err(ValidationError::MessageTooLong);
    }
    Ok(())
}

pub fn validate_worker_id(worker_id: &str) -> Result<(), ValidationError> {
    let worker_id = worker_id.trim();
    if worker_id.is_empty() || worker_id.len() > MAX_WORKER_ID_LEN {
        return Err(ValidationError::InvalidWorkerId);
    }
    Ok(())
}

pub fn is_valid_status_transition(current: &str, next: &EventStatus) -> bool {
    match (current, next) {
        (
            STATUS_ACCEPTED,
            EventStatus::Processing | EventStatus::Processed | EventStatus::Failed,
        ) => true,
        (STATUS_PROCESSING, EventStatus::Processed | EventStatus::Failed) => true,
        _ => false,
    }
}

pub fn request_fingerprint(req: &CreateEventRequest) -> String {
    let mut hasher = Sha256::new();
    let schema_version = req.schema_version.to_string();

    update_fingerprint_field(&mut hasher, "producer_id", req.producer_id.trim());
    update_fingerprint_field(&mut hasher, "event_type", req.event_type.as_str());
    update_fingerprint_field(&mut hasher, "schema_version", &schema_version);
    update_fingerprint_field(&mut hasher, "message", req.message.trim());

    hex::encode(hasher.finalize())
}

fn update_fingerprint_field(hasher: &mut Sha256, name: &str, value: &str) {
    hasher.update(name.as_bytes());
    hasher.update(b"\0");
    hasher.update(value.len().to_string().as_bytes());
    hasher.update(b"\0");
    hasher.update(value.as_bytes());
    hasher.update(b"\0");
}
