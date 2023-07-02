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
            i8 Int8,
            i16 Int16,
            i32 Int32,
            i64 Int64,
            i128 Int128,
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
        i8: i8,
        i16: i16,
        i32: i32,
        i64: i64,
        i128: i128,
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
        i8: -3,
        i16: -127,
        i32: -1,
        i64: 0xffff,
        i128: 0,
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
