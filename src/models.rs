use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const MAX_PRODUCER_ID_LEN: usize = 64;
const MAX_IDEMPOTENCY_KEY_LEN: usize = 128;
const MAX_MESSAGE_LEN: usize = 1000;
const SUPPORTED_SCHEMA_VERSION: u32 = 1;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum EventType {
    #[serde(rename = "user.signup")]
    UserSignup,
    #[serde(rename = "payment.completed")]
    PaymentCompleted,
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
