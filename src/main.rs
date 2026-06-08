mod handlers;
mod models;

use axum::{
    Router,
    routing::{get, post},
};
use handlers::{events_handler, get_event_handler, health_handler, ready_handler, root_handler};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("failed to connect to database");

    sqlx::query("SELECT 1")
        .execute(&pool)
        .await
        .expect("failed to verify database connection");

    let app = Router::new()
        .route("/", get(root_handler))
        .route("/health", get(health_handler))
        .route("/ready", get(ready_handler))
        .route("/v1/events", post(events_handler))
        .route("/v1/events/{event_id}", get(get_event_handler))
        .with_state(pool);

    let addr = "127.0.0.1:3000";
    let listener = TcpListener::bind(addr).await.unwrap();
    println!("Server running on http://{addr}");

    axum::serve(listener, app).await.unwrap();
}
