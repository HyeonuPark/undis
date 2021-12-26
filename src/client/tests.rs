use super::Error;

macro_rules! test_client {
    () => {
        match std::env::var("REDIS_URL") {
            Ok(url) => crate::Client::builder(1)
                .acquire_timeout(std::time::Duration::from_secs(3))
                .bind(&url)
                .await
                .unwrap(),
            Err(_) => return Ok(()),
        }
    };
}

#[tokio::test]
async fn command_canceled() -> Result<(), Error> {
    let client = test_client!();

    {
        // to emulate canceled command
        let mut conn = client.connection().await?;
        conn.send(("PING", "a")).await?;
    }
    {
        let b: String = client.raw_command(("PING", "b")).await?;
        assert_eq!("b", b);
    }

    Ok(())
}

#[tokio::test]
async fn serde_struct() -> Result<(), Error> {
    let client = test_client!();
    let key = "unittest_client_serde_struct_1";

    #[derive(Debug, serde::Serialize)]
    struct Write {
        mandatory: &'static str,
        default_but_exist: &'static str,
    }
    #[derive(Debug, PartialEq, serde::Deserialize)]
    // #[allow(unused)]
    struct Read {
        mandatory: String,
        #[serde(default)]
        default_but_exist: String,
        #[serde(default = "get_default")]
        default_value: String,
    }
    fn get_default() -> String {
        "wow default".into()
    }

    client
        .hset(
            key,
            Write {
                mandatory: "mandatory",
                default_but_exist: "not so default",
            },
        )
        .await?;
    let res: Read = client.hgetall(key).await?;

    assert_eq!(
        Read {
            mandatory: "mandatory".into(),
            default_but_exist: "not so default".into(),
            default_value: "wow default".into()
        },
        res
    );

    Ok(())
}
