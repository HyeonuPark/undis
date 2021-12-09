use std::error::Error;

use undis::client::Client;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct MyStruct {
    foo: String,
    bar: i32,
    baz: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = std::env::var("REDIS_URL")?;
    let client = Client::builder(10).bind(&addr).await?;
    println!("HELLO: {:?}", client.server_hello());

    let cnt = client
        .hset(
            "mykey",
            &MyStruct {
                foo: "abcde".into(),
                bar: 42,
                baz: false,
            },
        )
        .await?;

    let res: MyStruct = client.hmget("mykey").await?;

    println!("cnt: {}, res: {:#?}", cnt, res);

    Ok(())
}
