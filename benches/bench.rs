use futures_util::stream::{StreamExt, TryStreamExt};

pub struct ClickhouseClients {
    pub streamhouse: Vec<(&'static str, streamhouse::Client)>,
    pub clickhouse: Vec<(&'static str, clickhouse::Client)>,
    pub clickhouse_rs: Vec<(&'static str, clickhouse_rs::Pool)>,
}

mod common {
    #![allow(unused_macros, unused_imports, dead_code, unused_variables)]

    macro_rules! prepare_database {
        () => {
            common::_priv::prepare_database(file!(), function_name!()).await
        };
    }

    pub(crate) use {::function_name::named, prepare_database};
    pub(crate) mod _priv {
        const HTTP_URL: &str = "http://localhost:8124";
        const TCP_URL: &str = "tcp://localhost:9001";
        use streamhouse::Client;

        pub async fn prepare_database(file_path: &str, fn_name: &str) -> crate::ClickhouseClients {
            // let name = make_db_name(file_path, fn_name);
            let mut client = Client::builder().with_url(HTTP_URL);
            let file_path = &file_path[..file_path.len() - 3];
            let file_path = file_path.replace("/", "_");
            let database = format!("{file_path}__{fn_name}");

            println!("Database is {database}");

            let temp = client.clone().build();
            temp.execute(&format!(r"DROP DATABASE IF EXISTS {database}"))
                .await
                .unwrap();
            temp.execute(&format!(r"CREATE DATABASE {database}"))
                .await
                .unwrap();

            let clickhouse_client = clickhouse::Client::default()
                .with_url(HTTP_URL)
                .with_database(&database);

            let opts = clickhouse_rs::Options::new(TCP_URL.parse::<url::Url>().unwrap())
                .database(&database);

            client = client.with_database(&database);
            crate::ClickhouseClients {
                streamhouse: vec![
                    ("streamhouse", client.clone().build()),
                    (
                        "streamhouse-lz4",
                        client
                            .with_compression(streamhouse::Compression::Lz4)
                            .build(),
                    ),
                ],
                clickhouse: vec![
                    ("clickhouse", clickhouse_client.clone()),
                    (
                        "clickhouse-lz4",
                        clickhouse_client.with_compression(clickhouse::Compression::Lz4),
                    ),
                ],
                clickhouse_rs: vec![
                    ("clickhouse_rs", clickhouse_rs::Pool::new(opts.clone())),
                    (
                        "clickhouse_rs-compression",
                        clickhouse_rs::Pool::new(opts.with_compression()),
                    ),
                ],
            }
        }
    }
}

use std::time::Instant;

use function_name::named;
use streamhouse_derive::Row;

#[derive(
    Row, Eq, PartialEq, Debug, Clone, Copy, clickhouse::Row, serde::Deserialize, serde::Serialize,
)]
struct AgeEarsWeightRow {
    age: u64,
    num_ears: u8,
    weight: u64,
}

const NTESTS: usize = 3;

async fn bench_insert(clients: &ClickhouseClients, rows: &[AgeEarsWeightRow]) {
    for (name, client) in clients.streamhouse.iter() {
        for _ in 0..NTESTS {
            client.execute(r"TRUNCATE TABLE test;").await.unwrap();
            let start = std::time::Instant::now();
            client
                .insert::<AgeEarsWeightRow, _>("test", rows)
                .await
                .unwrap();
            println!("{name} insert took {}s", start.elapsed().as_secs_f64());
        }
        for _ in 0..NTESTS {
            client.execute(r"TRUNCATE TABLE test;").await.unwrap();
            let start = std::time::Instant::now();
            client
                .insert_stream::<AgeEarsWeightRow>(
                    "test",
                    futures_util::stream::iter(rows.to_vec().into_iter().map(Ok)),
                )
                .await
                .unwrap();
            println!(
                "{name} insert_stream took {}s",
                start.elapsed().as_secs_f64()
            );
        }
    }
    for (name, client) in clients.clickhouse.iter() {
        for _ in 0..NTESTS {
            client
                .query(r"TRUNCATE TABLE test;")
                .execute()
                .await
                .unwrap();
            let start = std::time::Instant::now();
            let mut inserting = client.insert::<AgeEarsWeightRow>("test").unwrap();
            for r in rows.iter() {
                inserting.write(r).await.unwrap();
            }
            inserting.end().await.unwrap();
            println!("{name} insert took {}s", start.elapsed().as_secs_f64());
        }
    }
    for (name, pool) in clients.clickhouse_rs.iter() {
        let mut handle = pool.get_handle().await.unwrap();
        for _ in 0..NTESTS {
            handle.execute(r"TRUNCATE TABLE test;").await.unwrap();
            let start = std::time::Instant::now();
            let mut block = clickhouse_rs::Block::with_capacity(rows.len());
            use clickhouse_rs::row;
            for r in rows.iter() {
                block
                    .push(row! {
                        age: r.age,
                        weight: r.weight,
                        num_ears: r.num_ears,
                    })
                    .unwrap();
            }
            handle.insert("test", block).await.unwrap();
            println!("{name} insert took {}s", start.elapsed().as_secs_f64());
        }
    }
}

async fn bench_age_ears_weight(clients: &ClickhouseClients) {
    let query = "select age, num_ears, weight from test";

    // First run the query a few times to get everything into cache that will be in cache.
    for _ in 0..NTESTS {
        clients.clickhouse[0]
            .1
            .query(query)
            .fetch_all::<AgeEarsWeightRow>()
            .await
            .unwrap();
    }

    for (name, client) in clients.streamhouse.iter() {
        for _ in 0..NTESTS {
            let start = Instant::now();
            let num_matching = client
                .query_fetch_all::<AgeEarsWeightRow>(query)
                .await
                .unwrap()
                .iter()
                .filter(|r| r.age == r.weight && r.num_ears < r.age as u8)
                .count();
            println!(
                "{name} query_fetch_all took {} to find {num_matching}",
                start.elapsed().as_secs_f64()
            );
        }
        for _ in 0..NTESTS {
            let start = Instant::now();
            let num_matching = client
                .query::<AgeEarsWeightRow>(query)
                .await
                .unwrap()
                .try_filter(|r| {
                    let v = r.age == r.weight && r.num_ears < r.age as u8;
                    async move { v }
                })
                .count()
                .await;
            println!(
                "{name} query took {} to find {num_matching}",
                start.elapsed().as_secs_f64()
            );
        }
        for _ in 0..NTESTS {
            let start = Instant::now();
            let mut rows = client.query::<AgeEarsWeightRow>(query).await.unwrap();
            let mut num_matching = 0;
            if let Some(r) = rows.try_next().await.unwrap() {
                if r.age == r.weight && r.num_ears < r.age as u8 {
                    num_matching += 1;
                }
            }
            println!(
                "{name} query first took {} to find {num_matching}",
                start.elapsed().as_secs_f64()
            );
        }
    }

    for (name, client) in clients.clickhouse.iter() {
        for _ in 0..NTESTS {
            let start = Instant::now();
            let num_matching = client
                .query(query)
                .fetch_all::<AgeEarsWeightRow>()
                .await
                .unwrap()
                .iter()
                .filter(|r| r.age == r.weight && r.num_ears < r.age as u8)
                .count();
            println!(
                "{name} query().fetch_all() took {} to find {num_matching}",
                start.elapsed().as_secs_f64()
            );
        }
        for _ in 0..NTESTS {
            let start = Instant::now();
            let mut rows = client.query(query).fetch::<AgeEarsWeightRow>().unwrap();
            let mut num_matching = 0;
            while let Some(r) = rows.next().await.unwrap() {
                if r.age == r.weight && r.num_ears < r.age as u8 {
                    num_matching += 1;
                }
            }
            println!(
                "{name} query().fetch() took {} to find {num_matching}",
                start.elapsed().as_secs_f64()
            );
        }
        for _ in 0..NTESTS {
            let start = Instant::now();
            let mut rows = client.query(query).fetch::<AgeEarsWeightRow>().unwrap();
            let mut num_matching = 0;
            if let Some(r) = rows.next().await.unwrap() {
                if r.age == r.weight && r.num_ears < r.age as u8 {
                    num_matching += 1;
                }
            }
            println!(
                "{name} query().fetch().first took {} to find {num_matching}",
                start.elapsed().as_secs_f64()
            );
        }
    }

    for (name, pool) in clients.clickhouse_rs.iter() {
        for _ in 0..NTESTS {
            let start = Instant::now();
            let num_matching = pool
                .get_handle()
                .await
                .unwrap()
                .query(query)
                .fetch_all()
                .await
                .unwrap()
                .rows()
                .filter(|r| {
                    let age = r.get::<u64, _>("age").unwrap();
                    let weight = r.get::<u64, _>("weight").unwrap();
                    let num_ears = r.get::<u8, _>("num_ears").unwrap();
                    age == weight && num_ears < age as u8
                })
                .count();
            println!(
                "{name} query().fetch_all() took {} to find {num_matching}",
                start.elapsed().as_secs_f64()
            );
        }
    }
}

#[named]
#[tokio::main]
async fn main() {
    let clients = common::prepare_database!();

    clients.streamhouse[0]
        .1
        .execute(
            r"CREATE TABLE IF NOT EXISTS test (
                age UInt64,
                num_ears UInt8,
                weight UInt64,
            ) Engine=MergeTree ORDER BY (age, num_ears, weight);",
        )
        .await
        .unwrap();

    let mut rows = Vec::new();
    const NUM_ROWS: u64 = 1_000_000;
    for i in 0..NUM_ROWS {
        rows.push(AgeEarsWeightRow {
            age: (i * 137 + 13) % 100,
            weight: (i * 73 + 130) % 137,
            num_ears: i as u8,
        })
    }

    println!("\n\n### Benchmarking with {NUM_ROWS} small values");
    bench_insert(&clients, &rows).await;
    bench_age_ears_weight(&clients).await;

    clients.streamhouse[0]
        .1
        .execute(r"TRUNCATE TABLE test;")
        .await
        .unwrap();

    println!("\n\n### Benchmarking with empty table");
    bench_age_ears_weight(&clients).await;

    rows.clear();
    for _ in 0..NUM_ROWS {
        rows.push(AgeEarsWeightRow {
            age: rand::random(),
            weight: rand::random(),
            num_ears: rand::random(),
        })
    }

    println!("\n\n### Benchmarking with {NUM_ROWS} fully random values");
    bench_insert(&clients, &rows).await;
    bench_age_ears_weight(&clients).await;
}
