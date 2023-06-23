mod common {
    #![allow(unused_macros, unused_imports, dead_code, unused_variables)]

    macro_rules! prepare_database {
        () => {
            common::_priv::prepare_database(file!(), function_name!()).await
        };
    }

    pub(crate) use {::function_name::named, prepare_database};
    pub(crate) mod _priv {
        const HOST: &str = "localhost:8124";
        use streamhouse::Client;

        pub async fn prepare_database(file_path: &str, fn_name: &str) -> Client {
            // let name = make_db_name(file_path, fn_name);
            let mut client = Client::builder().with_url(format!("http://{HOST}"));
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

            client = client.with_database(database);
            client.build()
        }
    }
}

use std::time::Instant;

use function_name::named;
use streamhouse_derive::Row;

#[named]
#[tokio::main]
async fn main() {
    let client = common::prepare_database!();

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
    const NUM_ROWS: u64 = 1_000_000;
    for i in 0..NUM_ROWS {
        rows.push(ThisRow {
            age: (i * 137 + 13) % 100,
            weight: (i * 73 + 130) % 137,
            num_ears: i as u8,
        })
    }

    client.insert("test", rows.iter().copied()).await.unwrap();

    let query = "select age, num_ears, weight from test";
    let start = Instant::now();
    let num_matching = client
        .query_fetch_all::<ThisRow>(query)
        .await
        .unwrap()
        .iter()
        .filter(|r| r.age == r.weight && r.num_ears < r.age as u8)
        .count();
    println!(
        "query_fetch_all took {} to find {num_matching}",
        start.elapsed().as_secs_f64()
    );
}
