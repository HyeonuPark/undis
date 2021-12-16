use std::error::Error;

use tokio::net::TcpStream;
use undis::connection::Connection;

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
struct MyStruct {
    foo: String,
    bar: i32,
    baz: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = std::env::var("REDIS_URL")?;
    let conn = TcpStream::connect(&addr).await?;
    let (mut conn, _hello) = Connection::new(conn).await?;

    let ok: String = conn.raw_command(&("FLUSHALL",)).await?;
    println!("{}", ok);

    Ok(())
}
