mod common;

use function_name::named;
use streamhouse::types::DateTime;
use streamhouse_derive::Row;

#[named]
#[tokio::test]
async fn fetch_all() {
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

    #[derive(Row, Eq, PartialEq, Debug, Clone, Copy)]
    struct ThisRow(DateTime);
    let rows = vec![ThisRow(DateTime::now())];
    let res = client.insert("test", rows.clone()).await;
    println!("res of bad insert is {res:?}");
    assert!(res.is_err());
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("Each column must have a name"));

    #[derive(Row, Eq, PartialEq, Debug, Clone)]
    struct NamedRow {
        when: ThisRow,
    }
    client
        .insert("test", rows.iter().map(|&when| NamedRow { when }))
        .await
        .unwrap();

    assert_eq!(
        rows,
        client
            .query_fetch_all::<ThisRow>("select * from test")
            .await
            .unwrap()
    );
}
