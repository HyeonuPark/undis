use std::error::Error;

use tokio::net::TcpStream;
use undis::connection::Connection;
use undis::resp3::Value;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = std::env::var("REDIS_URL").unwrap_or_else(|_| "localhost:6379".into());
    let name = std::env::var("REDIS_CLIENT_NAME").ok();
    let auth = std::env::var("REDIS_AUTH").ok();
    let auth = auth.as_ref().and_then(|auth| auth.split_once(':'));
    let db: Option<u32> = std::env::var("REDIS_DB").ok().map(|db| db.parse().unwrap());

    let command: Vec<_> = std::env::args().collect();
    let command = &command[1..];

    let conn = TcpStream::connect(&addr).await?;
    let (mut conn, _hello) = Connection::with_args(conn, auth, name.as_deref(), db).await?;

    let resp: Value = conn.raw_command(command).await?;
    println!("RESP: {:?}", resp);

    Ok(())
}
