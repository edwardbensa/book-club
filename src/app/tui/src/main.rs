use mongodb::{Client, options::ClientOptions, bson::{doc, Document}};
use dotenv::dotenv;
use std::env;
use tokio;
use futures::stream::TryStreamExt;

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

    // Access the 'book_club' database and 'books' collection
    let db = client.database("book_club");
    let collection = db.collection::<Document>("genres");

    // Query all documents
    let mut cursor = collection.find(None, None).await?;

    println!("Genres in collection:");
    while let Some(book) = cursor.try_next().await? {
        println!("{:#?}", book);
    }

    Ok(())
}
