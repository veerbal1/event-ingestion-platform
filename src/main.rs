mod handlers;
mod models;

use axum::{Router, routing::{get, post}};
use handlers::{events_handler, health_handler, root_handler};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(root_handler))
        .route("/health", get(health_handler))
        .route("/v1/events", post(events_handler));

    let addr = "127.0.0.1:3000";
    let listener = TcpListener::bind(addr).await.unwrap();
    println!("Server running on http://{addr}");

    axum::serve(listener, app).await.unwrap();
}
