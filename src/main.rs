use axum::{Router, http::StatusCode, routing::get};
use tokio::net::TcpListener;

async fn root_handler() -> (StatusCode, String) {
    (StatusCode::OK, "OK".to_string())
}

async fn health_handler() -> (StatusCode, String) {
    (StatusCode::OK, "Heath Ok".to_string())
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(root_handler))
        .route("/health", get(health_handler));

    let addr = "127.0.0.1:3000";
    let listener = TcpListener::bind(addr).await.unwrap();
    println!("Server running on http://{}", addr);

    axum::serve(listener, app).await.unwrap()
}
