use std::time::Duration;

use rand::RngExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use uuid::Uuid;

const API_BASE_URL: &str = "http://127.0.0.1:3000";
const SUPPORTED_SCHEMA_VERSION: u32 = 1;

const EVENT_TYPES: &[&str] = &["user.signup", "payment.completed"];
const PRODUCER_IDS: &[&str] = &["producer-a", "producer-b", "producer-c"];
const MESSAGES: &[&str] = &[
    "user@example.com",
    "order-12345",
    "subscription-upgrade",
    "trial-started",
    "invoice-paid",
    "account-closed",
];

#[derive(Serialize, Clone)]
struct CreateEventRequest {
    producer_id: String,
    idempotency_key: String,
    event_type: String,
    schema_version: u32,
    message: String,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct EventResponse {
    event_id: Option<Uuid>,
    status: String,
    message: String,
    received_at: Option<String>,
}

fn random_duration() -> Duration {
    let millis = rand::rng().random_range(1000..4000);
    Duration::from_millis(millis)
}

fn pick_random<T: Copy>(slice: &[T]) -> T {
    let idx = rand::rng().random_range(0..slice.len());
    slice[idx]
}

#[tokio::main]
async fn main() {
    let client = Client::new();
    let mut sent_keys: Vec<CreateEventRequest> = Vec::new();

    loop {
        let request: CreateEventRequest;
        let is_replay: bool;

        if !sent_keys.is_empty() && rand::rng().random_range(0..100) < 20 {
            let idx = rand::rng().random_range(0..sent_keys.len());
            let saved = &sent_keys[idx];
            request = CreateEventRequest {
                producer_id: saved.producer_id.clone(),
                idempotency_key: saved.idempotency_key.clone(),
                event_type: saved.event_type.clone(),
                schema_version: saved.schema_version,
                message: saved.message.clone(),
            };
            is_replay = true;
        } else {
            request = CreateEventRequest {
                producer_id: pick_random(PRODUCER_IDS).to_string(),
                idempotency_key: Uuid::new_v4().to_string(),
                event_type: pick_random(EVENT_TYPES).to_string(),
                schema_version: SUPPORTED_SCHEMA_VERSION,
                message: pick_random(MESSAGES).to_string(),
            };
            is_replay = false;
        }

        let response = client
            .post(format!("{API_BASE_URL}/v1/events"))
            .json(&request)
            .send()
            .await;

        match response {
            Ok(resp) => {
                let status = resp.status();
                match status.as_u16() {
                    200 | 202 => {
                        match resp.json::<EventResponse>().await {
                            Ok(event) => {
                                let id = event.event_id.map_or("?".to_string(), |id| id.to_string());
                                if is_replay {
                                    println!("replayed {} (duplicate)", id);
                                } else {
                                    println!("accepted {} {}", id, request.event_type);
                                }
                            }
                            Err(_) => {
                                if is_replay {
                                    println!("replayed (duplicate)");
                                } else {
                                    println!("accepted (parse error)");
                                }
                            }
                        }
                    }
                    409 => {
                        println!("conflict -- same key, different body");
                    }
                    _ => {
                        let body = resp.text().await.unwrap_or_default();
                        println!("unexpected {}: {}", status, body);
                    }
                }
            }
            Err(err) => {
                eprintln!("request failed: {err}");
            }
        }

        if sent_keys.len() > 100 {
            sent_keys.remove(0);
        }
        sent_keys.push(request);

        sleep(random_duration()).await;
    }
}
