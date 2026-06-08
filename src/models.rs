use serde::{Deserialize, Serialize};

const MAX_PRODUCER_ID_LEN: usize = 64;
const MAX_MESSAGE_LEN: usize = 1000;
const SUPPORTED_SCHEMA_VERSION: u32 = 1;

#[derive(Deserialize, Debug, PartialEq, Eq)]
pub enum EventType {
    UserSignup,
    PaymentCompleted,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::UserSignup => "user_signup",
            Self::PaymentCompleted => "payment_completed",
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
    pub event_id: Option<String>,
    pub status: String,
    pub message: String,
    pub received_at: Option<String>,
}

pub fn validate_request(req: &CreateEventRequest) -> Result<(), ValidationError> {
    let producer_id = req.producer_id.trim();
    if producer_id.is_empty() || producer_id.len() > MAX_PRODUCER_ID_LEN {
        return Err(ValidationError::InvalidProducerId);
    }
    if req.schema_version != SUPPORTED_SCHEMA_VERSION {
        return Err(ValidationError::UnsupportedSchemaVersion);
    }
    if req.message.len() > MAX_MESSAGE_LEN {
        return Err(ValidationError::MessageTooLong);
    }
    Ok(())
}
