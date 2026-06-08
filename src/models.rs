use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const MAX_PRODUCER_ID_LEN: usize = 64;
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
    UnsupportedSchemaVersion,
    MessageTooLong,
}

impl ValidationError {
    pub fn message(&self) -> &'static str {
        match self {
            Self::InvalidProducerId => "producer_id must be 1-64 non-whitespace characters",
            Self::UnsupportedSchemaVersion => "unsupported schema_version",
            Self::MessageTooLong => "message exceeds maximum length",
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct CreateEventRequest {
    pub producer_id: String,
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
    pub producer_id: String,
    pub event_type: String,
    pub schema_version: i32,
    pub message: String,
    pub status: String,
    pub received_at: DateTime<Utc>,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub status: String,
    pub message: String,
}

pub fn validate_request(req: &CreateEventRequest) -> Result<(), ValidationError> {
    let producer_id = req.producer_id.trim();
    if producer_id.trim().is_empty() || producer_id.trim().len() > MAX_PRODUCER_ID_LEN {
        return Err(ValidationError::InvalidProducerId);
    }
    if req.schema_version != SUPPORTED_SCHEMA_VERSION {
        return Err(ValidationError::UnsupportedSchemaVersion);
    }
    if req.message.trim().len() > MAX_MESSAGE_LEN {
        return Err(ValidationError::MessageTooLong);
    }
    Ok(())
}
