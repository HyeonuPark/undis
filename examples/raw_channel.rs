use std::error::Error;

use tokio::net::TcpStream;
use undis::connection::Connection;

#[derive(Debug, serde::Deserialize)]
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

    println!("cnt: {}, res: {:#?}", cnt, res);

    Ok(())
}
