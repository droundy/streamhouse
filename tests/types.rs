mod common;

use function_name::named;
use streamhouse_derive::Row;

#[named]
#[tokio::test]
async fn fetch_all() {
    let client = common::prepare_database!();

    client
        .execute(
            r"CREATE TABLE IF NOT EXISTS test (
            f32 Float32,
            f64 Float64,
       ) Engine=MergeTree
           ORDER BY (f32);",
        )
        .await
        .unwrap();

    #[derive(Row, PartialEq, Debug, Clone, Copy)]
    struct AllTypes {
        f32: f32,
        f64: f64,
    }
    let rows = vec![AllTypes {
        f32: 137.0,
        f64: 1.0 / 137.0,
    }];

    client.insert("test", rows.clone()).await.unwrap();

    assert_eq!(
        rows,
        client
            .query_fetch_all::<AllTypes>("select * from test")
            .await
            .unwrap()
    );
}
