use std::env;

use undis::Client;

pub type BoxError = Box<dyn std::error::Error>;
pub type MainResult = Result<(), BoxError>;

pub async fn client() -> Result<Client, BoxError> {
    let url = match env::var("REDIS_URL") {
        Ok(url) => url,
        Err(err) => {
            println!(
                "Failed to lookup `REDIS_URL` env var. Test aborted: {:?}",
                err
            );
            std::process::exit(0);
        }
    };
    let client = Client::builder(1).bind(&url).await?;
    println!("New redis connection: {:?}", client.server_hello());
    Ok(client)
}
