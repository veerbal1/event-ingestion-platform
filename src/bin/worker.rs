use std::{env, time::Duration};

use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};

const API_BASE_URL: &str = "http://127.0.0.1:3000";
const DEFAULT_WORKER_ID: &str = "worker-1";

#[derive(Serialize)]
struct ClaimEventRequest<'a> {
    worker_id: &'a str,
}

#[derive(Deserialize)]
struct ClaimEventResponse {
    event_id: String,
    event_type: String,
    status: String,
    locked_by: String,
    locked_at: String,
    received_at: String,
}

#[derive(Serialize)]
struct CompleteEventRequest<'a> {
    worker_id: &'a str,
    status: &'a str,
}

#[derive(Deserialize)]
struct EventStatusResponse {
    event_id: String,
    status: String,
    received_at: String,
}

#[derive(Deserialize)]
struct ErrorResponse {
    status: String,
    message: String,
}

#[tokio::main]
async fn main() {
    let client = Client::new();
    let worker_id = env::var("WORKER_ID")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_WORKER_ID.to_string());

    println!("{worker_id} started");

    loop {
        match claim_event(&client, &worker_id).await {
            Ok(Some(event)) => {
                println!(
                    "{} claimed event {} ({}) with status {} at {} (received_at {})",
                    event.locked_by,
                    event.event_id,
                    event.event_type,
                    event.status,
                    event.locked_at,
                    event.received_at
                );

                println!("{worker_id} processing event {}", event.event_id);
                tokio::time::sleep(Duration::from_secs(1)).await;

                match complete_event(&client, &worker_id, &event.event_id).await {
                    Ok(completed) => {
                        println!(
                            "{} completed event {} as {} (received_at {})",
                            worker_id, completed.event_id, completed.status, completed.received_at
                        );
                    }
                    Err(err) => {
                        println!(
                            "{} failed to complete event {}: {}",
                            worker_id, event.event_id, err
                        );
                    }
                }
            }
            Ok(None) => {
                println!("{worker_id} found no accepted events; sleeping");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(err) => {
                println!("{worker_id} claim failed: {err}; sleeping");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

async fn claim_event(
    client: &Client,
    worker_id: &str,
) -> Result<Option<ClaimEventResponse>, String> {
    let response = client
        .post(format!("{API_BASE_URL}/v1/events/claim"))
        .json(&ClaimEventRequest { worker_id })
        .send()
        .await
        .map_err(|err| err.to_string())?;

    if response.status() == StatusCode::NO_CONTENT {
        return Ok(None);
    }

    if !response.status().is_success() {
        return Err(api_error_message(response).await);
    }

    response
        .json::<ClaimEventResponse>()
        .await
        .map(Some)
        .map_err(|err| err.to_string())
}

async fn complete_event(
    client: &Client,
    worker_id: &str,
    event_id: &str,
) -> Result<EventStatusResponse, String> {
    let response = client
        .post(format!("{API_BASE_URL}/v1/events/{event_id}/complete"))
        .json(&CompleteEventRequest {
            worker_id,
            status: "processed",
        })
        .send()
        .await
        .map_err(|err| err.to_string())?;

    if !response.status().is_success() {
        return Err(api_error_message(response).await);
    }

    response
        .json::<EventStatusResponse>()
        .await
        .map_err(|err| err.to_string())
}

async fn api_error_message(response: reqwest::Response) -> String {
    let status = response.status();

    match response.json::<ErrorResponse>().await {
        Ok(error) => format!("{status}: {} - {}", error.status, error.message),
        Err(err) => format!("{status}: failed to decode error response: {err}"),
    }
}
