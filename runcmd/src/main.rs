use std::error::Error;

use tokio::net::TcpStream;
use undis::connection::Connection;
use undis::resp3::Value;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = std::env::var("REDIS_URL").unwrap_or_else(|_| "localhost:6379".into());
    let command: Vec<_> = std::env::args().collect();
    let command = &command[1..];

    let conn = TcpStream::connect(&addr).await?;
    let (mut conn, _hello) = Connection::new(conn).await?;

    let resp: Value = conn.raw_command(command).await?;
    println!("RESP: {:?}", resp);

    Ok(())
}
