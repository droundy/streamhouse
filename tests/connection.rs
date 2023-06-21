mod common;

use function_name::named;
use streamhouse_derive::Row;

#[named]
#[tokio::test]
async fn has_connection() {
    let client = common::prepare_database!();

    let rows = vec!["COLUMNS", "SCHEMATA", "TABLES", "VIEWS"];

    assert_eq!(
        rows,
        client
            .query_fetch_all::<String>(
                "select name from system.tables where database = 'INFORMATION_SCHEMA' ORDER BY name"
            )
            .await
            .unwrap()
    );

    assert_eq!(
        r#"Column types mismatch: [String] vs [UInt8]"#,
        client
            .query_fetch_all::<u8>(
                "select name from system.tables where database = 'INFORMATION_SCHEMA' ORDER BY name"
            )
            .await
            .unwrap_err()
            .to_string()
    );
}

#[named]
#[tokio::test]
async fn create_table() {
    let client = common::prepare_database!();

    assert!(client
        .execute(
            r"CREATE TABLE IF NOT EXI STS test_create_table (
            name String,
            favorite_color String,
            age UInt8,
       ) Engine=ReplacingMergeTree
           ORDER BY (name, favorite_color);",
        )
        .await
        .is_err());

    client
        .execute(
            r"CREATE TABLE IF NOT EXISTS test_create_table (
            name String,
            favorite_color String,
            age UInt8,
       ) Engine=ReplacingMergeTree
           ORDER BY (name, favorite_color);",
        )
        .await
        .unwrap();

    client
        .execute(r"INSERT INTO test_create_table VALUES ('David', 'blue', 49)")
        .await
        .unwrap();

    assert_eq!(
        vec!["David"],
        client
            .query_fetch_all::<String>("select name from test_create_table ORDER BY name")
            .await
            .unwrap()
    );

    client
        .execute(r"INSERT INTO test_create_table VALUES ('Roundy', 'blue', 49)")
        .await
        .unwrap();

    assert_eq!(
        vec!["David", "Roundy"],
        client
            .query_fetch_all::<String>("select name from test_create_table ORDER BY name")
            .await
            .unwrap()
    );
    assert_eq!(
        vec![49u8, 49],
        client
            .query_fetch_all::<u8>("select age from test_create_table ORDER BY name")
            .await
            .unwrap()
    );

    #[derive(Row, Eq, PartialEq, Debug)]
    struct ThisRow {
        name: String,
        favorite_color: String,
        age: u8,
    }

    assert_eq!(
        vec![
            ThisRow {
                name: "David".to_string(),
                favorite_color: "blue".to_string(),
                age: 49
            },
            ThisRow {
                name: "Roundy".to_string(),
                favorite_color: "blue".to_string(),
                age: 49
            },
        ],
        client
            .query_fetch_all::<ThisRow>(
                "select name, favorite_color, age from test_create_table ORDER BY name"
            )
            .await
            .unwrap()
    );
}
#[named]
#[tokio::test]
async fn one_string_column() {
    let client = common::prepare_database!();

    client
        .execute(
            r"CREATE TABLE IF NOT EXISTS test (
            name String,
       ) Engine=MergeTree ORDER BY (name);",
        )
        .await
        .unwrap();

    client
        .execute(r"INSERT INTO test VALUES ('David')")
        .await
        .unwrap();

    assert_eq!(
        vec!["David"],
        client
            .query_fetch_all::<String>("select name from test ORDER BY name")
            .await
            .unwrap()
    );

    client
        .insert("test", ["Roundy".to_string(), "Joel".to_string()])
        .await
        .unwrap();

    assert_eq!(
        vec!["David", "Joel", "Roundy"],
        client
            .query_fetch_all::<String>("select name from test ORDER BY name")
            .await
            .unwrap()
    );
}
