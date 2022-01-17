use std::env;
use std::future::Future;
use std::time::Duration;

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
    let client = Client::new(1, &url).await?;
    Ok(client)
}

pub fn with_client<T, F>(task: T) -> MainResult
where
    T: FnOnce(Client) -> F,
    F: Future<Output = MainResult>,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async { task(client().await?).await })
}

pub fn mostly_eq(low: Duration, high: Duration) -> bool {
    low <= high && high - low < Duration::from_secs(1)
}
