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
    let conn = TcpStream::connect("127.0.0.1:6379").await?;
    let (mut conn, hello) = Connection::new(conn).await?;

    println!("hello: {:?}", hello);

    let cnt: usize = conn
        .raw_command(&("HSET", "mykey", "foo", "abcde", "bar", 42, "baz", false))
        .await?;
    let res: MyStruct = conn.raw_command(&("HGETALL", "mykey")).await?;
    let pong: String = conn.raw_command(&("PING", 42)).await?;

    println!("cnt: {}, res: {:#?}, pong: {}", cnt, res, pong);

    Ok(())
}
