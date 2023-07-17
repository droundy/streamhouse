mod common;

use function_name::named;
use futures_util::TryStreamExt;
use streamhouse_derive::Row;

#[named]
#[tokio::test]
async fn stream_rows() {
    let client = common::prepare_database!().build();

    client
        .execute(
            r"CREATE TABLE IF NOT EXISTS developers (
            name String,
            favorite_color String,
            age UInt8,
       ) Engine=ReplacingMergeTree
           ORDER BY (name, favorite_color);",
        )
        .await
        .unwrap();

    #[derive(Row, Eq, PartialEq, Debug, Clone)]
    struct Developer {
        name: String,
        favorite_color: String,
        age: u8,
    }

    let developers = vec![
        Developer {
            name: "David".to_string(),
            favorite_color: "blue".to_string(),
            age: 49,
        },
        Developer {
            name: "Roundy".to_string(),
            favorite_color: "blue".to_string(),
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
            .query_fetch_all::<Developer>(
                "select name, favorite_color, age from developers ORDER BY name"
            )
            .await
            .unwrap()
    );

    assert_eq!(
        developers.clone(),
        client
            .query::<Developer>("select name, favorite_color, age from developers ORDER BY name")
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    );
}

#[named]
#[tokio::test]
async fn insert_stream() {
    let client = common::prepare_database!().build();

    client
        .execute(
            r"CREATE TABLE IF NOT EXISTS developers (
            name String,
            favorite_color String,
            age UInt8,
       ) Engine=ReplacingMergeTree
           ORDER BY (name, favorite_color);",
        )
        .await
        .unwrap();
    client
        .execute(
            r"CREATE TABLE IF NOT EXISTS users (
            name String,
            favorite_color String,
            age UInt8,
       ) Engine=ReplacingMergeTree
           ORDER BY (favorite_color, name);",
        )
        .await
        .unwrap();

    #[derive(Row, Eq, PartialEq, Debug, Clone)]
    struct Developer {
        name: String,
        favorite_color: String,
        age: u8,
    }

    let developers = vec![
        Developer {
            name: "David".to_string(),
            favorite_color: "blue".to_string(),
            age: 49,
        },
        Developer {
            name: "Roundy".to_string(),
            favorite_color: "blue".to_string(),
            age: 49,
        },
    ];
    client.insert("users", developers.clone()).await.unwrap();

    client
        .insert_stream(
            "developers",
            client
                .query::<Developer>("select name, favorite_color, age from users ORDER BY name")
                .await
                .unwrap(),
        )
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
            .query_fetch_all::<Developer>(
                "select name, favorite_color, age from developers ORDER BY name"
            )
            .await
            .unwrap()
    );

    assert_eq!(
        developers.clone(),
        client
            .query::<Developer>("select name, favorite_color, age from developers ORDER BY name")
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    );
}
