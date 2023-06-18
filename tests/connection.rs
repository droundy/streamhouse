mod common;

use function_name::named;

#[named]
#[tokio::test]
async fn has_connection() {
    let client = common::prepare_database!();

    let rows = vec![
        "INFORMATION_SCHEMA",
        "default",
        "information_schema",
        "system",
    ];

    assert_eq!(
        rows,
        client
            .query_fetch_all::<String>("select name from system.databases")
            .await
            .unwrap()
    );

    assert_eq!(
        r#"Column types mismatch: ["String"] vs ["UInt8"]"#,
        client
            .query_fetch_all::<u8>("select name from system.databases")
            .await
            .unwrap_err()
            .to_string()
    );
}
