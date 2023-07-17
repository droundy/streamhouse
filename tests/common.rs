#![allow(unused_macros, unused_imports, dead_code, unused_variables)]

macro_rules! prepare_database {
    () => {
        common::_priv::prepare_database(file!(), function_name!()).await
    };
}

pub(crate) use {::function_name::named, prepare_database};
pub(crate) mod _priv {
    const HOST: &str = "localhost:8124";
    use streamhouse::{Client, ClientBuilder};

    pub async fn prepare_database(file_path: &str, fn_name: &str) -> ClientBuilder {
        // let name = make_db_name(file_path, fn_name);
        let client = Client::builder().with_url(format!("http://{HOST}"));
        let file_path = &file_path[..file_path.len() - 3];
        let file_path = file_path.replace("tests/", "");
        let database = format!("{file_path}__{fn_name}");

        println!("Database is {database}");

        let temp = client.clone().build();
        temp.execute(&format!(r"DROP DATABASE IF EXISTS {database}"))
            .await
            .unwrap();
        temp.execute(&format!(r"CREATE DATABASE {database}"))
            .await
            .unwrap();

        client.with_database(database)
    }
}
