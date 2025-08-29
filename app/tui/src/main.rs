use mongodb::{Client, options::ClientOptions, bson::doc};
use dotenv::dotenv;
use std::env;
use tokio;

#[tokio::main]
async fn main() -> mongodb::error::Result<()> {
    // Load environment variables from .env file
    dotenv().ok();

    // Get MongoDB URI from environment
    let uri = env::var("MONGODB_URI").expect("MONGODB_URI not set");

    // Parse options and create client
    let client_options = ClientOptions::parse(&uri).await?;
    let client = Client::with_options(client_options)?;

    // Ping the server to confirm connection
    client
        .database("admin")
        .run_command(doc! { "ping": 1 }, None)
        .await?;

    println!("Connected to MongoDB!");

    Ok(())
}
