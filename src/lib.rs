mod error;
pub use error::Error;

mod query;

pub(crate) mod column;
pub use column::{Column, Row};

// use streamhouse_derive::Row;

pub struct Client {
    client: hyper::Client<hyper::client::HttpConnector>,
    url: String,
    user: Option<String>,
    password: Option<String>,
    database: Option<String>,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }
}

#[derive(Default, Clone)]
pub struct ClientBuilder {
    client: hyper::client::Builder,
    url: Option<String>,
    user: Option<String>,
    password: Option<String>,
    database: Option<String>,
}

impl ClientBuilder {
    pub fn with_url(self, url: impl Into<String>) -> Self {
        ClientBuilder {
            url: Some(url.into()),
            ..self
        }
    }
    pub fn with_user(self, user: impl Into<String>) -> Self {
        ClientBuilder {
            user: Some(user.into()),
            ..self
        }
    }
    pub fn with_password(self, password: impl Into<String>) -> Self {
        ClientBuilder {
            password: Some(password.into()),
            ..self
        }
    }
    pub fn with_database(self, database: impl Into<String>) -> Self {
        ClientBuilder {
            database: Some(database.into()),
            ..self
        }
    }
    pub fn build(self) -> Client {
        Client {
            client: self.client.build_http(),
            url: self.url.expect("Need to specify url for Client"),
            user: self.user,
            password: self.password,
            database: self.database,
        }
    }
}
