mod common;

use function_name::named;
use streamhouse::types::DateTime;
use streamhouse_derive::Row;

#[named]
#[tokio::test]
async fn stream_rows() {
    let client = common::prepare_database!();

    client
        .execute(
            r"CREATE TABLE IF NOT EXISTS test (
            when DateTime,
       ) Engine=MergeTree
           ORDER BY (when);",
        )
        .await
        .unwrap();

    #[derive(Row, Eq, PartialEq, Debug, Clone)]
    struct ThisRow {
        when: DateTime,
    }
    let rows = vec![ThisRow {
        when: DateTime::now(),
    }];
    client.insert("test", rows.clone()).await.unwrap();

    assert_eq!(
        rows,
        client
            .query_fetch_all::<ThisRow>("select * from test")
            .await
            .unwrap()
    );
}
