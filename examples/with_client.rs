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
    let client = Client::builder(10).bind(([127, 0, 0, 1], 6379).into())?;

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
