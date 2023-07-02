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
            u8 UInt8,
            u16 UInt16,
            u32 UInt32,
            u64 UInt64,
            u128 UInt128,
            fixed FixedString(8),
       ) Engine=MergeTree
           ORDER BY (f32);",
        )
        .await
        .unwrap();

    #[derive(Row, PartialEq, Debug, Clone, Copy)]
    struct AllTypes {
        f32: f32,
        f64: f64,
        u8: u8,
        u16: u16,
        u32: u32,
        u64: u64,
        u128: u128,
        fixed: [u8; 8],
    }
    let rows = vec![AllTypes {
        f32: 137.0,
        f64: 1.0 / 137.0,
        u8: 1,
        u16: 2,
        u32: 3,
        u64: 123456,
        u128: 1 << 123 + 2,
        fixed: [b'a'; 8],
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
