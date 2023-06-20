mod common;

use function_name::named;

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
}
