mod common;

use function_name::named;
use streamhouse_derive::Row;

#[named]
#[tokio::test]
async fn insert_long() {
    let client = common::prepare_database!().build();

    client
        .execute(
            r"CREATE TABLE IF NOT EXISTS test (
                age UInt64,
                num_ears UInt8,
                weight UInt64,
            ) Engine=MergeTree ORDER BY (age, num_ears, weight);",
        )
        .await
        .unwrap();

    #[derive(Row, Eq, PartialEq, Debug, Clone, Copy)]
    struct ThisRow {
        age: u64,
        num_ears: u8,
        weight: u64,
    }
    let mut rows = Vec::new();
    const NUM_ROWS: u64 = 10_000;
    for i in 0..NUM_ROWS {
        rows.push(ThisRow {
            age: (i * 137 + 13) % 100,
            weight: (i * 73 + 130) % 137,
            num_ears: i as u8,
        })
    }

    client
        .insert("test", Vec::<ThisRow>::new())
        .await
        .expect("failed to insert nothing");

    client
        .insert("test", rows.iter().copied())
        .await
        .expect("failed to insert");

    let query = "select age, num_ears, weight from test";
    let num_matching = client
        .query_fetch_all::<ThisRow>(query)
        .await
        .expect("query should succeed")
        .iter()
        .filter(|r| r.age == r.weight && r.num_ears < r.age as u8)
        .count();
    println!("query_fetch_all to find {num_matching}");
}
