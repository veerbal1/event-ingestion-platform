use async_nats::jetstream::{self, stream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = async_nats::connect("nats://localhost:4222").await?;
    println!("connected to NATS");

    let jetstream = jetstream::new(client);

    jetstream
        .get_or_create_stream(stream::Config {
            name: "EVENTS".into(),
            subjects: vec!["events.ingested".into()],
            ..Default::default()
        })
        .await?;
    println!("stream EVENTS ready");

    let payload = serde_json::json!({
        "event_type": "user.signup",
        "message": "spike-test"
    })
    .to_string();

    let ack_future = jetstream
        .publish("events.ingested", payload.into())
        .await?;
    let ack = ack_future.await?;
    println!("published message with seq {}", ack.sequence);

    Ok(())
}
