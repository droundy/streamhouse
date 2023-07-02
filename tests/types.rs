mod common;

use function_name::named;
use streamhouse::{types::LowCardinality, Row};

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
            string String,
            low_string LowCardinality(String),
            bytes String,
            ipv4 IPv4,
            ipv6 IPv6,
            uuid UUID,
       ) Engine=MergeTree
           ORDER BY (f32);",
        )
        .await
        .unwrap();

    #[derive(Row, PartialEq, Debug, Clone)]
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
        string: String,
        low_string: LowCardinality<String>,
        bytes: Vec<u8>,
        ipv4: std::net::Ipv4Addr,
        ipv6: std::net::Ipv6Addr,
        uuid: streamhouse::types::Uuid,
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
        string: "Hello world".to_string(),
        low_string: "David".to_string().into(),
        bytes: b"Hello world\0".to_vec(),
        ipv4: std::net::Ipv4Addr::new(127, 0, 0, 1),
        ipv6: std::net::Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xc00a, 0x2ff),
        uuid: streamhouse::types::Uuid::from([5; 16]),
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
