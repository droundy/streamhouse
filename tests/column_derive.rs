mod common;

use function_name::named;
use futures_util::TryStreamExt;
use streamhouse_derive::{Column, Row};

#[named]
#[tokio::test]
async fn stream_rows() {
    let client = common::prepare_database!();

    client
        .execute(
            r"CREATE TABLE IF NOT EXISTS developers (
            name String,
            age UInt8,
       ) Engine=ReplacingMergeTree
           ORDER BY (name);",
        )
        .await
        .unwrap();

    #[derive(Column, Eq, PartialEq, Debug, Clone)]
    struct FirstName(String);
    impl From<&str> for FirstName {
        fn from(value: &str) -> Self {
            FirstName(value.to_string())
        }
    }

    #[derive(Row, Eq, PartialEq, Debug, Clone)]
    struct Developer {
        name: FirstName,
        age: u8,
    }

    let developers = vec![
        Developer {
            name: "David".into(),
            age: 49,
        },
        Developer {
            name: "Roundy".into(),
            age: 49,
        },
    ];
    client
        .insert("developers", developers.clone())
        .await
        .unwrap();

    assert_eq!(
        vec!["David", "Roundy"],
        client
            .query_fetch_all::<String>("select name from developers ORDER BY name")
            .await
            .unwrap()
    );

    assert_eq!(
        vec![49u8, 49],
        client
            .query_fetch_all::<u8>("select age from developers ORDER BY name")
            .await
            .unwrap()
    );

    assert_eq!(
        developers.clone(),
        client
            .query_fetch_all::<Developer>("select name, age from developers ORDER BY name")
            .await
            .unwrap()
    );

    assert_eq!(
        developers.clone(),
        client
            .query::<Developer>("select name, age from developers ORDER BY name")
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    );
}
