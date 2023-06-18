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
        let client = Client::builder().with_url(format!("http://{HOST}"));
        client.build()
    }
}
