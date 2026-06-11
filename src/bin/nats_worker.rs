use async_nats::jetstream::{self, consumer, stream};
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = async_nats::connect("nats://localhost:4222").await?;
    println!("connected to NATS");

    let jetstream = jetstream::new(client);

    let stream = jetstream
        .get_or_create_stream(stream::Config {
            name: "EVENTS".into(),
            subjects: vec!["events.ingested".into()],
            ..Default::default()
        })
        .await?;
    println!("stream EVENTS ready");

    let consumer: consumer::PullConsumer = stream
        .get_or_create_consumer(
            "spike-consumer",
            consumer::pull::Config {
                durable_name: Some("spike-consumer".into()),
                ..Default::default()
            },
        )
        .await?;
    println!("consumer spike-consumer ready, listening...");

    let mut messages = consumer.messages().await?;
    while let Some(msg) = messages.next().await {
        match msg {
            Ok(msg) => {
                println!("received: {}", String::from_utf8_lossy(&msg.payload));
                msg.ack().await?;
            }
            Err(err) => eprintln!("message error: {err}"),
        }
    }

    Ok(())
}
